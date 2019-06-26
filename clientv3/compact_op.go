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
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
)

// CompactOp represents a compact operation.
// 操作
type CompactOp struct {
	revision int64
	physical bool
}

// CompactOption configures compact operation.
// 选项，即函数
type CompactOption func(*CompactOp)

// 选项生效
func (op *CompactOp) applyCompactOpts(opts []CompactOption) {
	for _, opt := range opts {
		opt(op)
	}
}

// OpCompact wraps slice CompactOption to create a CompactOp.
// 创建操作
func OpCompact(rev int64, opts ...CompactOption) CompactOp {
	ret := CompactOp{revision: rev}
	ret.applyCompactOpts(opts)
	return ret
}

// 结构转换
func (op CompactOp) toRequest() *pb.CompactionRequest {
	return &pb.CompactionRequest{Revision: op.revision, Physical: op.physical}
}

// WithCompactPhysical makes Compact wait until all compacted entries are
// removed from the etcd server's storage.
// 默认选项
func WithCompactPhysical() CompactOption {
	return func(op *CompactOp) { op.physical = true }
}
