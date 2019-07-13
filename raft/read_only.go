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

package raft

import pb "go.etcd.io/etcd/raft/raftpb"

// ReadState provides state for read only query.
// It's caller's responsibility to call ReadIndex first before getting
// this state from ready, it's also caller's duty to differentiate if this
// state is what it requests through RequestCtx, eg. given a unique id as
// RequestCtx
// 读状态
type ReadState struct {
	Index      uint64 // 索引
	RequestCtx []byte // 请求上下文
}

// 读索引状态
type readIndexStatus struct {
	req   pb.Message // 请求
	index uint64 // 序号
	acks  map[uint64]struct{} // ack 映射表
}

// 只读
type readOnly struct {
    // 选项
	option           ReadOnlyOption
	// 读索引待定
	pendingReadIndex map[string]*readIndexStatus
	// 读索引队列
	readIndexQueue   []string
}

func newReadOnly(option ReadOnlyOption) *readOnly {
	return &readOnly{
		option:           option,
		pendingReadIndex: make(map[string]*readIndexStatus),
	}
}

// addRequest adds a read only reuqest into readonly struct.
// `index` is the commit index of the raft state machine when it received
// the read only request.
// `m` is the original read only request message from the local or remote node.
// 添加请求
func (ro *readOnly) addRequest(index uint64, m pb.Message) {
	ctx := string(m.Entries[0].Data)
	if _, ok := ro.pendingReadIndex[ctx]; ok {
	    // 在处理了
		return
	}

    // 加入队列
	ro.pendingReadIndex[ctx] = &readIndexStatus{index: index, req: m, acks: make(map[uint64]struct{})}
	ro.readIndexQueue = append(ro.readIndexQueue, ctx)
}

// recvAck notifies the readonly struct that the raft state machine received
// an acknowledgment of the heartbeat that attached with the read only request
// context.
// 添加ack
func (ro *readOnly) recvAck(m pb.Message) int {
	rs, ok := ro.pendingReadIndex[string(m.Context)]
	if !ok {
	    // 不需要处理
		return 0
	}

	rs.acks[m.From] = struct{}{}
	// add one to include an ack from local node
	return len(rs.acks) + 1
}

// advance advances the read only request queue kept by the readonly struct.
// It dequeues the requests until it finds the read only request that has
// the same context as the given `m`.
func (ro *readOnly) advance(m pb.Message) []*readIndexStatus {
	var (
		i     int
		found bool
	)

	ctx := string(m.Context)
	rss := []*readIndexStatus{}

	for _, okctx := range ro.readIndexQueue {
		i++
		rs, ok := ro.pendingReadIndex[okctx]
		if !ok {
		    // 系统bug
			panic("cannot find corresponding read state from pending map")
		}

        // 入缓存
		rss = append(rss, rs)

		if okctx == ctx {
		    // 找到终点了
			found = true
			break
		}
	}

	if found {
	    // 删除
		ro.readIndexQueue = ro.readIndexQueue[i:]
		for _, rs := range rss {
			delete(ro.pendingReadIndex, string(rs.req.Entries[0].Data))
		}
		return rss
	}

	return nil
}

// lastPendingRequestCtx returns the context of the last pending read only
// request in readonly struct.
// 返回最后一次的上下文
func (ro *readOnly) lastPendingRequestCtx() string {
	if len(ro.readIndexQueue) == 0 {
		return ""
	}

	return ro.readIndexQueue[len(ro.readIndexQueue)-1]
}
