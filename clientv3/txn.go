// Copyright 2016 The etcd Authors
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package clientv3

import (
	"context"
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"

	"google.golang.org/grpc"
)

// Txn is the interface that wraps mini-transactions.
//
//	 Txn(context.TODO()).If(
//	  Compare(Value(k1), ">", v1),
//	  Compare(Version(k1), "=", 2)
//	 ).Then(
//	  OpPut(k2,v2), OpPut(k3,v3)
//	 ).Else(
//	  OpPut(k4,v4), OpPut(k5,v5)
//	 ).Commit()
//
type Txn interface {
	// If takes a list of comparison. If all comparisons passed in succeed,
	// the operations passed into Then() will be executed. Or the operations
	// passed into Else() will be executed.
	If(cs ...Cmp) Txn

	// Then takes a list of operations. The Ops list will be executed, if the
	// comparisons passed in If() succeed.
	Then(ops ...Op) Txn

	// Else takes a list of operations. The Ops list will be executed, if the
	// comparisons passed in If() fail.
	Else(ops ...Op) Txn

	// Commit tries to commit the transaction.
	Commit() (*TxnResponse, error)
}

type txn struct {
	kv  *kv // kv
	ctx context.Context // 上下文

	mu    sync.Mutex // 锁
	cif   bool // 有if ？
	cthen bool // 有 then ？
	celse bool // 有else ？

	isWrite bool // 写操作

	cmps []*pb.Compare // 比较列表

	sus []*pb.RequestOp // 成功操作
	fas []*pb.RequestOp // 失败操作

	callOpts []grpc.CallOption // rpc 选项
}

func (txn *txn) If(cs ...Cmp) Txn {
    // 加锁
	txn.mu.Lock()
	defer txn.mu.Unlock()

    // 校验是否已经操作过了
	if txn.cif {
		panic("cannot call If twice!")
	}
	if txn.cthen {
		panic("cannot call If after Then!")
	}
	if txn.celse {
		panic("cannot call If after Else!")
	}

    // 设置if 过了
	txn.cif = true

    // 保存比较项
	for i := range cs {
		txn.cmps = append(txn.cmps, (*pb.Compare)(&cs[i]))
	}

	return txn
}

func (txn *txn) Then(ops ...Op) Txn {
    // 加锁
	txn.mu.Lock()
	defer txn.mu.Unlock()

    // 校验是否操作过了
	if txn.cthen {
		panic("cannot call Then twice!")
	}
	if txn.celse {
		panic("cannot call Then after Else!")
	}

    // 设置 then 过了
	txn.cthen = true

    // 保存成功操作
	for _, op := range ops {
	    // 设置写操作位
		txn.isWrite = txn.isWrite || op.isWrite()
		txn.sus = append(txn.sus, op.toRequestOp())
	}

	return txn
}

func (txn *txn) Else(ops ...Op) Txn {
    // 加锁
	txn.mu.Lock()
	defer txn.mu.Unlock()

    // 校验是否操作过了
	if txn.celse {
		panic("cannot call Else twice!")
	}

    // 设置else 过了
	txn.celse = true

    // 保存操作列表
	for _, op := range ops {
	    // 设置写操作位
		txn.isWrite = txn.isWrite || op.isWrite()
		txn.fas = append(txn.fas, op.toRequestOp())
	}

	return txn
}

func (txn *txn) Commit() (*TxnResponse, error) {
    // 加锁
	txn.mu.Lock()
	defer txn.mu.Unlock()

    // 构建提交事务
	r := &pb.TxnRequest{Compare: txn.cmps, Success: txn.sus, Failure: txn.fas}

    // 提交事务
	var resp *pb.TxnResponse
	var err error
	resp, err = txn.kv.remote.Txn(txn.ctx, r, txn.callOpts...)
	if err != nil {
		return nil, toErr(txn.ctx, err)
	}

	return (*TxnResponse)(resp), nil
}
