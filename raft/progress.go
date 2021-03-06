// Copyright 2015 The etcd Authors
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

import "fmt"

// 状态：探测、复制、快照
const (
	ProgressStateProbe ProgressStateType = iota
	ProgressStateReplicate
	ProgressStateSnapshot
)
type ProgressStateType uint64
var prstmap = [...]string{
	"ProgressStateProbe",
	"ProgressStateReplicate",
	"ProgressStateSnapshot",
}
func (st ProgressStateType) String() string {
    return prstmap[uint64(st)]
}

// Progress represents a follower’s progress in the view of the leader. Leader maintains
// progresses of all followers, and sends entries to the follower based on its progress.
type Progress struct {
	Match, Next uint64

	// State defines how the leader should interact with the follower.
	//
	// When in ProgressStateProbe, leader sends at most one replication message
	// per heartbeat interval. It also probes actual progress of the follower.
	//
	// When in ProgressStateReplicate, leader optimistically increases next
	// to the latest entry sent after sending replication message. This is
	// an optimized state for fast replicating log entries to the follower.
	//
	// When in ProgressStateSnapshot, leader should have sent out snapshot
	// before and stops sending any replication message.
	// 探测状态，每心跳期间发送一条日志去探测追随者的进度。
	// 复制状态，快速发送复制日志
	// 快照状态，停止发送复制日志，并发送快照
	State ProgressStateType

	// Paused is used in ProgressStateProbe.
	// When Paused is true, raft should pause sending replication message to this peer.
	// 用于探测状态，指示停止发送复制日志
	Paused bool

	// PendingSnapshot is used in ProgressStateSnapshot.
	// If there is a pending snapshot, the pendingSnapshot will be set to the
	// index of the snapshot. If pendingSnapshot is set, the replication process of
	// this Progress will be paused. raft will not resend snapshot until the pending one
	// is reported to be failed.
	// 快照状态使用，会触发停止复制日志
	PendingSnapshot uint64

	// RecentActive is true if the progress is recently active. Receiving any messages
	// from the corresponding follower indicates the progress is active.
	// RecentActive can be reset to false after an election timeout.
	RecentActive bool

	// inflights is a sliding window for the inflight messages.
	// Each inflight message contains one or more log entries.
	// The max number of entries per message is defined in raft config as MaxSizePerMsg.
	// Thus inflight effectively limits both the number of inflight messages
	// and the bandwidth each Progress can use.
	// When inflights is full, no more message should be sent.
	// When a leader sends out a message, the index of the last
	// entry should be added to inflights. The index MUST be added
	// into inflights in order.
	// When a leader receives a reply, the previous inflights should
	// be freed by calling inflights.freeTo with the index of the last
	// received entry.
	ins *inflights

	// IsLearner is true if this progress is tracked for a learner.
	IsLearner bool
}

// 重置状态
func (pr *Progress) resetState(state ProgressStateType) {
	pr.Paused = false
	pr.PendingSnapshot = 0
	pr.State = state
	pr.ins.reset()
}

// 变成探测状态
func (pr *Progress) becomeProbe() {
	// If the original state is ProgressStateSnapshot, progress knows that
	// the pending snapshot has been sent to this peer successfully, then
	// probes from pendingSnapshot + 1.
	// 设置为探测状态，如果当前状态为快照，next 为match 或快照的下一个
	if pr.State == ProgressStateSnapshot {
		pendingSnapshot := pr.PendingSnapshot

		pr.resetState(ProgressStateProbe)
		pr.Next = max(pr.Match+1, pendingSnapshot+1)
	} else {
		pr.resetState(ProgressStateProbe)
		pr.Next = pr.Match + 1
	}
}

// 成为复制状态，修改next 索引
func (pr *Progress) becomeReplicate() {
	pr.resetState(ProgressStateReplicate)
	pr.Next = pr.Match + 1
}

// 成为快照状态，备份快照索引
func (pr *Progress) becomeSnapshot(snapshoti uint64) {
	pr.resetState(ProgressStateSnapshot)
	pr.PendingSnapshot = snapshoti
}

// maybeUpdate returns false if the given n index comes from an outdated message.
// Otherwise it updates the progress and returns true.
// 判定是否需要发更新
func (pr *Progress) maybeUpdate(n uint64) bool {
	var updated bool

    // 判定是否更新
	if pr.Match < n {
	    // 命中了新的索引，继续复制同步
		pr.Match = n
		updated = true
		pr.resume()
	}

    // 更新next
	if pr.Next < n+1 {
		pr.Next = n + 1
	}

	return updated
}

// 乐观的更新
func (pr *Progress) optimisticUpdate(n uint64) {
    pr.Next = n + 1
}

// maybeDecrTo returns false if the given to index comes from an out of order message.
// Otherwise it decreases the progress next index to min(rejected, last) and returns true.
// 当收到对方的消息时，反馈如下
func (pr *Progress) maybeDecrTo(rejected, last uint64) bool {
	if pr.State == ProgressStateReplicate {
	    // 复制状态
		// the rejection must be stale if the progress has matched and "rejected"
		// is smaller than "match".
		if rejected <= pr.Match {
		    // 老的拒绝，忽略
			return false
		}

		// directly decrease next to match + 1
		// 新的拒绝，从match + 1 开始重来
		pr.Next = pr.Match + 1
		return true
	}

    // 探测或快照状态
   
	// the rejection must be stale if "rejected" does not match next - 1
	if pr.Next-1 != rejected {
	    // 老的拒绝，忽略
		return false
	}

    // 重置next
	if pr.Next = min(rejected, last+1); pr.Next < 1 {
		pr.Next = 1
	}

    // 继续
	pr.resume()
	return true
}

// 暂停与继续
func (pr *Progress) pause()  { pr.Paused = true }
func (pr *Progress) resume() { pr.Paused = false }

// IsPaused returns whether sending log entries to this node has been
// paused. A node may be paused because it has rejected recent
// MsgApps, is currently waiting for a snapshot, or has reached the
// MaxInflightMsgs limit.
// 是否停止发送日志
func (pr *Progress) IsPaused() bool {
	switch pr.State {
	case ProgressStateProbe:
	    // 默认状态，看是否停止
		return pr.Paused
	case ProgressStateReplicate:
	    // 复制状态，看队列是否满了
		return pr.ins.full()
	case ProgressStateSnapshot:
	    // 快照默认停止
		return true
	default:
		panic("unexpected state")
	}
}

// 快照错误，重置快照索引
func (pr *Progress) snapshotFailure() {
    pr.PendingSnapshot = 0
}

// needSnapshotAbort returns true if snapshot progress's Match
// is equal or higher than the pendingSnapshot.
// 是否需要放弃快照？
// 当match 大于快照时放弃
func (pr *Progress) needSnapshotAbort() bool {
	return
	pr.State == ProgressStateSnapshot &&
	pr.Match >= pr.PendingSnapshot
}

func (pr *Progress) String() string {
	return fmt.Sprintf("next = %d, match = %d, state = %s, waiting = %v, pendingSnapshot = %d", pr.Next, pr.Match, pr.State, pr.IsPaused(), pr.PendingSnapshot)
}

type inflights struct {
	// the starting index in the buffer
	// 开始索引
	start int

	// number of inflights in the buffer
	// 数目
	count int

	// the size of the buffer
	// 容量
	size int

	// buffer contains the index of the last entry
	// inside one message.
	// buffer 列表
	buffer []uint64
}

func newInflights(size int) *inflights {
	return &inflights{
		size: size,
	}
}

// add adds an inflight into inflights
func (in *inflights) add(inflight uint64) {
	if in.full() {
	    // 队列满
		panic("cannot add into a full inflights")
	}

    // 循环队列，找下一个槽位
	next := in.start + in.count
	size := in.size
	if next >= size {
		next -= size
	}

    // 扩大buffer
	if next >= len(in.buffer) {
		in.growBuf()
	}

    // 入队列
	in.buffer[next] = inflight
	in.count++
}

// grow the inflight buffer by doubling up to inflights.size. We grow on demand
// instead of preallocating to inflights.size to handle systems which have
// thousands of Raft groups per process.
// buffer 增长
func (in *inflights) growBuf() {
	newSize := len(in.buffer) * 2
	if newSize == 0 {
	    // 默认一个
		newSize = 1
	} else if newSize > in.size {
	    // 有上限
		newSize = in.size
	}

    // buffer 扩大
	newBuffer := make([]uint64, newSize)
	copy(newBuffer, in.buffer)
	in.buffer = newBuffer
}

// freeTo frees the inflights smaller or equal to the given `to` flight.
func (in *inflights) freeTo(to uint64) {
	if in.count == 0 || to < in.buffer[in.start] {
		// out of the left side of the window
		// to 太小
		return
	}

	idx := in.start
	var i int
	for i = 0; i < in.count; i++ {
		if to < in.buffer[idx] { // found the first large inflight
		    // 找到第一个比to 大的
			break
		}

		// increase index and maybe rotate
		// 索引向右走
		size := in.size
		if idx++; idx >= size {
			idx -= size
		}
	}

	// free i inflights and set new start index
	// 清空索引
	in.count -= i
	in.start = idx
	if in.count == 0 {
		// inflights is empty, reset the start index so that we don't grow the
		// buffer unnecessarily.
		in.start = 0
	}
}

func (in *inflights) freeFirstOne() {
    in.freeTo(in.buffer[in.start])
}

// full returns true if the inflights is full.
// 满了？
func (in *inflights) full() bool {
	return in.count == in.size
}

// resets frees all inflights.
// 重置
func (in *inflights) reset() {
	in.count = 0
	in.start = 0
}
