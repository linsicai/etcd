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

package lease

import (
	"container/heap"
	"context"
	"encoding/binary"
	"errors"
	"math"
	"sort"
	"sync"
	"time"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/lease/leasepb"
	"go.etcd.io/etcd/mvcc/backend"
	"go.uber.org/zap"
)

// NoLease is a special LeaseID representing the absence of a lease.
const NoLease = LeaseID(0)

// MaxLeaseTTL is the maximum lease TTL value
const MaxLeaseTTL = 9000000000

var (
	forever = time.Time{}

	leaseBucketName = []byte("lease")

	// maximum number of leases to revoke per second; configurable for tests
	leaseRevokeRate = 1000

	// maximum number of lease checkpoints recorded to the consensus log per second; configurable for tests
	leaseCheckpointRate = 1000

	// maximum number of lease checkpoints to batch into a single consensus log entry
	maxLeaseCheckpointBatchSize = 1000

	// 错误
	ErrNotPrimary       = errors.New("not a primary lessor")
	ErrLeaseNotFound    = errors.New("lease not found")
	ErrLeaseExists      = errors.New("lease already exists")
	ErrLeaseTTLTooLarge = errors.New("too large lease TTL")
)

// TxnDelete is a TxnWrite that only permits deletes. Defined here
// to avoid circular dependency with mvcc.
type TxnDelete interface {
	DeleteRange(key, end []byte) (n, rev int64)

	End()
}

// RangeDeleter is a TxnDelete constructor.
type RangeDeleter func() TxnDelete

// Checkpointer permits checkpointing of lease remaining TTLs to the consensus log. Defined here to
// avoid circular dependency with mvcc.
type Checkpointer func(ctx context.Context, lc *pb.LeaseCheckpointRequest)

type LeaseID int64

// Lessor owns leases. It can grant, revoke, renew and modify leases for lessee.
type Lessor interface {
	// SetRangeDeleter lets the lessor create TxnDeletes to the store.
	// Lessor deletes the items in the revoked or expired lease by creating
	// new TxnDeletes.
	// 删除事务
	SetRangeDeleter(rd RangeDeleter)
	// checkpoint 事务
	SetCheckpointer(cp Checkpointer)

	// Grant grants a lease that expires at least after TTL seconds.
	// 获取
	Grant(id LeaseID, ttl int64) (*Lease, error)

	// Revoke revokes a lease with given ID. The item attached to the
	// given lease will be removed. If the ID does not exist, an error
	// will be returned.
	// 废除
	Revoke(id LeaseID) error

	// Checkpoint applies the remainingTTL of a lease. The remainingTTL is used in Promote to set
	// the expiry of leases to less than the full TTL when possible.
	// 做checkpoint
	Checkpoint(id LeaseID, remainingTTL int64) error

	// Attach attaches given leaseItem to the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	// 租约添加key
	Attach(id LeaseID, items []LeaseItem) error

	// GetLease returns LeaseID for given item.
	// If no lease found, NoLease value will be returned.
	// 反向查找
	GetLease(item LeaseItem) LeaseID

	// Detach detaches given leaseItem from the lease with given LeaseID.
	// If the lease does not exist, an error will be returned.
	// 租约移除key
	Detach(id LeaseID, items []LeaseItem) error

	// Promote promotes the lessor to be the primary lessor. Primary lessor manages
	// the expiration and renew of leases.
	// Newly promoted lessor renew the TTL of all lease to extend + previous TTL.
	// 升级
	Promote(extend time.Duration)

	// Demote demotes the lessor from being the primary lessor.
	// 降级
	Demote()

	// Renew renews a lease with given ID. It returns the renewed TTL. If the ID does not exist,
	// an error will be returned.
	// 刷新ttl
	Renew(id LeaseID) (int64, error)

	// Lookup gives the lease at a given lease id, if any
	// 查找租约
	Lookup(id LeaseID) *Lease

	// Leases lists all leases.
	// 所有租约列表
	Leases() []*Lease

	// ExpiredLeasesC returns a chan that is used to receive expired leases.
	// 获取超时租约
	ExpiredLeasesC() <-chan []*Lease

	// Recover recovers the lessor state from the given backend and RangeDeleter.
	// 恢复租约
	Recover(b backend.Backend, rd RangeDeleter)

	// Stop stops the lessor for managing leases. The behavior of calling Stop multiple
	// times is undefined.
	// 停机
	Stop()
}

// lessor implements Lessor interface.
// TODO: use clockwork for testability.
type lessor struct {
	// suow
	mu sync.RWMutex

	// demotec is set when the lessor is the primary.
	// demotec will be closed if the lessor is demoted.
	demotec chan struct{}

	// 租约映射表
	leaseMap map[LeaseID]*Lease
	// 超时堆
	leaseHeap LeaseQueue
	// checkpoint 堆
	leaseCheckpointHeap LeaseQueue
	// 反向映射表
	itemMap map[LeaseItem]LeaseID

	// When a lease expires, the lessor will delete the
	// leased range (or key) by the RangeDeleter.
	// 删除者
	rd RangeDeleter

	// When a lease's deadline should be persisted to preserve the remaining TTL across leader
	// elections and restarts, the lessor will checkpoint the lease by the Checkpointer.
	// checkpoint 者
	cp Checkpointer

	// backend to persist leases. We only persist lease ID and expiry for now.
	// The leased items can be recovered by iterating all the keys in kv.
	// 后台线程
	b backend.Backend

	// minLeaseTTL is the minimum lease TTL that can be granted for a lease. Any
	// requests for shorter TTLs are extended to the minimum TTL.
	// 最小ttl
	minLeaseTTL int64

	// 超时通道
	expiredC chan []*Lease

	// stopC is a channel whose closure indicates that the lessor should be stopped.
	// 停机信号
	stopC chan struct{}

	// doneC is a channel whose closure indicates that the lessor is stopped.
	// 结束信号
	doneC chan struct{}

	// 日志
	lg *zap.Logger

	// Wait duration between lease checkpoints.
	// checkpoint 间隔
	checkpointInterval time.Duration
}

// 租约配置
type LessorConfig struct {
	// 最小ttl
	MinLeaseTTL int64

	// checkpoint 间隔
	CheckpointInterval time.Duration
}

func NewLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig) Lessor {
	return newLessor(lg, b, cfg)
}

func newLessor(lg *zap.Logger, b backend.Backend, cfg LessorConfig) *lessor {
	// 取checkpoint 间隔，默认5分钟
	checkpointInterval := cfg.CheckpointInterval
	if checkpointInterval == 0 {
		checkpointInterval = 5 * time.Minute
	}

	// 构造函数
	l := &lessor{
		leaseMap:            make(map[LeaseID]*Lease),
		itemMap:             make(map[LeaseItem]LeaseID),
		leaseHeap:           make(LeaseQueue, 0),
		leaseCheckpointHeap: make(LeaseQueue, 0),
		b:                   b,
		minLeaseTTL:         cfg.MinLeaseTTL,
		checkpointInterval:  checkpointInterval,
		// expiredC is a small buffered chan to avoid unnecessary blocking.
		expiredC: make(chan []*Lease, 16),
		stopC:    make(chan struct{}),
		doneC:    make(chan struct{}),
		lg:       lg,
	}

	// 初始化
	l.initAndRecover()

	// 启动后台流程
	go l.runLoop()

	return l
}

// isPrimary indicates if this lessor is the primary lessor. The primary
// lessor manages lease expiration and renew.
//
// in etcd, raft leader is the primary. Thus there might be two primary
// leaders at the same time (raft allows concurrent leader but with different term)
// for at most a leader election timeout.
// The old primary leader cannot affect the correctness since its proposal has a
// smaller term and will not be committed.
//
// TODO: raft follower do not forward lease management proposals. There might be a
// very small window (within second normally which depends on go scheduling) that
// a raft follow is the primary between the raft leader demotion and lessor demotion.
// Usually this should not be a problem. Lease should not be that sensitive to timing.
// 是否为主
func (le *lessor) isPrimary() bool {
	return le.demotec != nil
}

func (le *lessor) SetRangeDeleter(rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.rd = rd
}

func (le *lessor) SetCheckpointer(cp Checkpointer) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.cp = cp
}

func (le *lessor) Grant(id LeaseID, ttl int64) (*Lease, error) {
	// 参数校验
	if id == NoLease {
		return nil, ErrLeaseNotFound
	}
	if ttl > MaxLeaseTTL {
		return nil, ErrLeaseTTLTooLarge
	}

	// TODO: when lessor is under high load, it should give out lease
	// with longer TTL to reduce renew load.
	// 构造函数
	l := &Lease{
		ID:      id,
		ttl:     ttl,
		itemSet: make(map[LeaseItem]struct{}),
		revokec: make(chan struct{}),
	}

	// 加锁
	le.mu.Lock()
	defer le.mu.Unlock()

	// 校验是否存在
	if _, ok := le.leaseMap[id]; ok {
		return nil, ErrLeaseExists
	}

	// 校正配置
	if l.ttl < le.minLeaseTTL {
		l.ttl = le.minLeaseTTL
	}

	// 为主设置超时时间
	if le.isPrimary() {
		l.refresh(0)
	} else {
		l.forever()
	}

	// 入映射表
	le.leaseMap[id] = l

	// 入超时队列
	item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
	heap.Push(&le.leaseHeap, item)

	// 落盘
	l.persistTo(le.b)

	// 统计
	leaseTotalTTLs.Observe(float64(l.ttl))
	leaseGranted.Inc()

	// 放入checkpoint 队列
	if le.isPrimary() {
		le.scheduleCheckpointIfNeeded(l)
	}

	return l, nil
}

// 销毁
func (le *lessor) Revoke(id LeaseID) error {
	le.mu.Lock()

	l := le.leaseMap[id]
	if l == nil {
		// 无此id
		le.mu.Unlock()
		return ErrLeaseNotFound
	}

	// 处理完后通知
	defer close(l.revokec)

	// unlock before doing external work
	le.mu.Unlock()

	if le.rd == nil {
		// 不需要后续处理
		return nil
	}

	txn := le.rd()

	// sort keys so deletes are in same order among all members,
	// otherwise the backened hashes will be different
	// 获取所有key
	keys := l.Keys()
	sort.StringSlice(keys).Sort()
	for _, key := range keys {
		// 删除key
		txn.DeleteRange([]byte(key), nil)
	}

	le.mu.Lock()
	defer le.mu.Unlock()

	// 删除租约
	delete(le.leaseMap, l.ID)

	// lease deletion needs to be in the same backend transaction with the
	// kv deletion. Or we might end up with not executing the revoke or not
	// deleting the keys if etcdserver fails in between.
	// 写事务
	le.b.BatchTx().UnsafeDelete(leaseBucketName, int64ToBytes(int64(l.ID)))

	txn.End()

	leaseRevoked.Inc()
	return nil
}

func (le *lessor) Checkpoint(id LeaseID, remainingTTL int64) error {
	// 加锁
	le.mu.Lock()
	defer le.mu.Unlock()

	if l, ok := le.leaseMap[id]; ok {
		// when checkpointing, we only update the remainingTTL, Promote is responsible for applying this to lease expiry
		// 更新ttl
		l.remainingTTL = remainingTTL

		if le.isPrimary() {
			// schedule the next checkpoint as needed
			// 通知checkpoint
			le.scheduleCheckpointIfNeeded(l)
		}
	}

	return nil
}

// Renew renews an existing lease. If the given lease does not exist or
// has expired, an error will be returned.
func (le *lessor) Renew(id LeaseID) (int64, error) {
	le.mu.Lock()

	unlock := func() { le.mu.Unlock() }
	defer func() { unlock() }()

	if !le.isPrimary() {
		// forward renew request to primary instead of returning error.
		// 不是主
		return -1, ErrNotPrimary
	}

	demotec := le.demotec

	l := le.leaseMap[id]
	if l == nil {
		// 无此ID
		return -1, ErrLeaseNotFound
	}

	if l.expired() {
		// 超时了
		le.mu.Unlock()
		unlock = func() {}

		select {
		// A expired lease might be pending for revoking or going through
		// quorum to be revoked. To be accurate, renew request must wait for the
		// deletion to complete.
		case <-l.revokec:
			// 销毁了
			return -1, ErrLeaseNotFound
		// The expired lease might fail to be revoked if the primary changes.
		// The caller will retry on ErrNotPrimary.
		case <-demotec:
			// 降级了
			return -1, ErrNotPrimary
		case <-le.stopC:
			// 停机了
			return -1, ErrNotPrimary
		}
	}

	// Clear remaining TTL when we renew if it is set
	// By applying a RAFT entry only when the remainingTTL is already set, we limit the number
	// of RAFT entries written per lease to a max of 2 per checkpoint interval.
	if le.cp != nil && l.remainingTTL > 0 {
		// checkpoint
		le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: []*pb.LeaseCheckpoint{{ID: int64(l.ID), Remaining_TTL: 0}}})
	}

	// 入超时队列
	l.refresh(0)
	item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
	heap.Push(&le.leaseHeap, item)

	// 统计
	leaseRenewed.Inc()

	return l.ttl, nil
}

// 查找
func (le *lessor) Lookup(id LeaseID) *Lease {
	le.mu.RLock()
	defer le.mu.RUnlock()

	return le.leaseMap[id]
}

// 获取所有租约
func (le *lessor) unsafeLeases() []*Lease {
	leases := make([]*Lease, 0, len(le.leaseMap))

	for _, l := range le.leaseMap {
		leases = append(leases, l)
	}

	return leases
}

// 获取所有租约，并排序
func (le *lessor) Leases() []*Lease {
	le.mu.RLock()
	ls := le.unsafeLeases()
	le.mu.RUnlock()

	sort.Sort(leasesByExpiry(ls))

	return ls
}

func (le *lessor) Promote(extend time.Duration) {
	le.mu.Lock()
	defer le.mu.Unlock()

	// 创建降级通道
	le.demotec = make(chan struct{})

	// refresh the expiries of all leases.
	for _, l := range le.leaseMap {
		// 给一定时间
		l.refresh(extend)

		// 入超时队列
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		heap.Push(&le.leaseHeap, item)
	}

	if len(le.leaseMap) < leaseRevokeRate {
		// no possibility of lease pile-up
		// 不需要销毁东西
		return
	}

	// adjust expiries in case of overlap
	// 获取所有租约
	leases := le.unsafeLeases()
	sort.Sort(leasesByExpiry(leases))

	// 按每秒淘汰多少入超时队列
	baseWindow := leases[0].Remaining()
	nextWindow := baseWindow + time.Second
	expires := 0
	// have fewer expires than the total revoke rate so piled up leases
	// don't consume the entire revoke limit
	targetExpiresPerSecond := (3 * leaseRevokeRate) / 4
	for _, l := range leases {
		remaining := l.Remaining()
		if remaining > nextWindow {
			// 更新窗口
			baseWindow = remaining
			nextWindow = baseWindow + time.Second
			expires = 1
			continue
		}

		expires++
		if expires <= targetExpiresPerSecond {
			// 未到目标
			continue
		}

		// 需要销毁
		rateDelay := float64(time.Second) * (float64(expires) / float64(targetExpiresPerSecond))
		// If leases are extended by n seconds, leases n seconds ahead of the
		// base window should be extended by only one second.
		rateDelay -= float64(remaining - baseWindow)

		delay := time.Duration(rateDelay)
		nextWindow = baseWindow + delay

		// 入超时队列
		l.refresh(delay + extend)
		item := &LeaseWithTime{id: l.ID, time: l.expiry.UnixNano()}
		heap.Push(&le.leaseHeap, item)

		// 需要checkpoint
		le.scheduleCheckpointIfNeeded(l)
	}
}

// 排序用
type leasesByExpiry []*Lease

func (le leasesByExpiry) Len() int           { return len(le) }
func (le leasesByExpiry) Less(i, j int) bool { return le[i].Remaining() < le[j].Remaining() }
func (le leasesByExpiry) Swap(i, j int)      { le[i], le[j] = le[j], le[i] }

// 降级
func (le *lessor) Demote() {
	le.mu.Lock()
	defer le.mu.Unlock()

	// set the expiries of all leases to forever
	for _, l := range le.leaseMap {
		// 设置为永不超时
		l.forever()
	}

	// 清除checkpoint 队列
	le.clearScheduledLeasesCheckpoints()

	// 通知降级
	if le.demotec != nil {
		close(le.demotec)

		le.demotec = nil
	}
}

// Attach attaches items to the lease with given ID. When the lease
// expires, the attached items will be automatically removed.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Attach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		// 无租约id
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		// 入租约
		l.itemSet[it] = struct{}{}

		// 方向映射
		le.itemMap[it] = id
	}
	l.mu.Unlock()

	return nil
}

// 查询租约id
func (le *lessor) GetLease(item LeaseItem) LeaseID {
	le.mu.RLock()
	id := le.itemMap[item]
	le.mu.RUnlock()

	return id
}

// Detach detaches items from the lease with given ID.
// If the given lease does not exist, an error will be returned.
func (le *lessor) Detach(id LeaseID, items []LeaseItem) error {
	le.mu.Lock()
	defer le.mu.Unlock()

	l := le.leaseMap[id]
	if l == nil {
		// 无id
		return ErrLeaseNotFound
	}

	l.mu.Lock()
	for _, it := range items {
		// 重租约中移除
		delete(l.itemSet, it)

		// 移除映射表
		delete(le.itemMap, it)
	}
	l.mu.Unlock()

	return nil
}

// 初始化与恢复
func (le *lessor) Recover(b backend.Backend, rd RangeDeleter) {
	le.mu.Lock()
	defer le.mu.Unlock()

	le.b = b
	le.rd = rd
	le.leaseMap = make(map[LeaseID]*Lease)
	le.itemMap = make(map[LeaseItem]LeaseID)
	le.initAndRecover()
}

// 超时通道
func (le *lessor) ExpiredLeasesC() <-chan []*Lease {
	return le.expiredC
}

func (le *lessor) Stop() {
	// 触发停机信号
	close(le.stopC)

	// 等待结束
	<-le.doneC
}

func (le *lessor) runLoop() {
	// 设置结束通道
	defer close(le.doneC)

	// 循环
	for {
		// 移除超时
		le.revokeExpiredLeases()

		// 定期checkpoint
		le.checkpointScheduledLeases()

		select {
		case <-time.After(500 * time.Millisecond):
			// 收到超时事件
		case <-le.stopC:
			// 收到停止信号
			return
		}
	}
}

// revokeExpiredLeases finds all leases past their expiry and sends them to epxired channel for
// to be revoked.
// 销毁超时租约
func (le *lessor) revokeExpiredLeases() {
	var ls []*Lease

	// rate limit
	revokeLimit := leaseRevokeRate / 2

	// 找超时
	le.mu.RLock()
	if le.isPrimary() {
		ls = le.findExpiredLeases(revokeLimit)
	}
	le.mu.RUnlock()

	if len(ls) != 0 {
		select {
		case <-le.stopC:
			// 收到停止信号
			return
		case le.expiredC <- ls:
			// 通知超时
		default:
			// 其他
			// the receiver of expiredC is probably busy handling
			// other stuff
			// let's try this next time after 500ms
		}
	}
}

// checkpointScheduledLeases finds all scheduled lease checkpoints that are due and
// submits them to the checkpointer to persist them to the consensus log.
func (le *lessor) checkpointScheduledLeases() {
	var cps []*pb.LeaseCheckpoint

	// rate limit
	// 速度控制
	for i := 0; i < leaseCheckpointRate/2; i++ {
		// 找事做
		le.mu.Lock()
		if le.isPrimary() {
			cps = le.findDueScheduledCheckpoints(maxLeaseCheckpointBatchSize)
		}
		le.mu.Unlock()

		// 做事情
		if len(cps) != 0 {
			le.cp(context.Background(), &pb.LeaseCheckpointRequest{Checkpoints: cps})
		}

		if len(cps) < maxLeaseCheckpointBatchSize {
			// 没得做了
			return
		}

		// 继续做
	}
}

// 清空checkpoint 队列
func (le *lessor) clearScheduledLeasesCheckpoints() {
	le.leaseCheckpointHeap = make(LeaseQueue, 0)
}

// expireExists returns true if expiry items exist.
// It pops only when expiry item exists.
// "next" is true, to indicate that it may exist in next attempt.
func (le *lessor) expireExists() (l *Lease, ok bool, next bool) {
	if le.leaseHeap.Len() == 0 {
		// 数据为空
		return nil, false, false
	}

	item := le.leaseHeap[0]
	l = le.leaseMap[item.id]
	if l == nil {
		// lease has expired or been revoked
		// no need to revoke (nothing is expiry)
		heap.Pop(&le.leaseHeap) // O(log N)
		// id 不存在了
		return nil, false, true
	}

	if time.Now().UnixNano() < item.time /* expiration time */ {
		// Candidate expirations are caught up, reinsert this item
		// and no need to revoke (nothing is expiry)
		// 未超时
		return l, false, false
	}
	// if the lease is actually expired, add to the removal list. If it is not expired, we can ignore it because another entry will have been inserted into the heap

	// 超时了
	heap.Pop(&le.leaseHeap) // O(log N)
	return l, true, false
}

// findExpiredLeases loops leases in the leaseMap until reaching expired limit
// and returns the expired leases that needed to be revoked.
func (le *lessor) findExpiredLeases(limit int) []*Lease {
	// 初始化，假设为十六
	leases := make([]*Lease, 0, 16)

	for {
		l, ok, next := le.expireExists()
		if !ok && !next {
			// 无超时，无数据
			break
		}

		if !ok {
			// 无数据，继续取
			continue
		}
		if next {
			// 继续取
			continue
		}

		if l.expired() {
			// 超时处理
			leases = append(leases, l)

			// reach expired limit
			if len(leases) == limit {
				break
			}
		}
	}

	return leases
}

func (le *lessor) scheduleCheckpointIfNeeded(lease *Lease) {
	if le.cp == nil {
		// 不需要
		return
	}

	if lease.RemainingTTL() > int64(le.checkpointInterval.Seconds()) {
		// 日志
		if le.lg != nil {
			le.lg.Debug("Scheduling lease checkpoint",
				zap.Int64("leaseID", int64(lease.ID)),
				zap.Duration("intervalSeconds", le.checkpointInterval),
			)
		}

		// checkpoint 超时队列
		heap.Push(&le.leaseCheckpointHeap, &LeaseWithTime{
			id:   lease.ID,
			time: time.Now().Add(le.checkpointInterval).UnixNano(),
		})
	}
}

// 找需要checkpoint的项
func (le *lessor) findDueScheduledCheckpoints(checkpointLimit int) []*pb.LeaseCheckpoint {
	if le.cp == nil {
		// 不需要
		return nil
	}

	now := time.Now()
	cps := []*pb.LeaseCheckpoint{}

	// 循环
	for le.leaseCheckpointHeap.Len() > 0 && len(cps) < checkpointLimit {
		lt := le.leaseCheckpointHeap[0]
		if lt.time /* next checkpoint time */ > now.UnixNano() {
			// 队列头未超时，结束
			return cps
		}

		// 弹出
		heap.Pop(&le.leaseCheckpointHeap)

		var l *Lease
		var ok bool
		if l, ok = le.leaseMap[lt.id]; !ok {
			// 无数据，继续
			continue
		}

		if !now.Before(l.expiry) {
			// 未到时间
			continue
		}

		remainingTTL := int64(math.Ceil(l.expiry.Sub(now).Seconds()))
		if remainingTTL >= l.ttl {
			// 还有ttl
			continue
		}

		// 日志
		if le.lg != nil {
			le.lg.Debug("Checkpointing lease",
				zap.Int64("leaseID", int64(lt.id)),
				zap.Int64("remainingTTL", remainingTTL),
			)
		}

		// 需要处理
		cps = append(cps, &pb.LeaseCheckpoint{ID: int64(lt.id), Remaining_TTL: remainingTTL})
	}

	return cps
}

func (le *lessor) initAndRecover() {
	// 事务
	tx := le.b.BatchTx()

	// 加锁
	tx.Lock()

	// 创建bucket
	tx.UnsafeCreateBucket(leaseBucketName)

	// 加载数据
	_, vs := tx.UnsafeRange(leaseBucketName, int64ToBytes(0), int64ToBytes(math.MaxInt64), 0)
	// TODO: copy vs and do decoding outside tx lock if lock contention becomes an issue.
	for i := range vs {
		// 反序列化
		var lpb leasepb.Lease
		err := lpb.Unmarshal(vs[i])
		if err != nil {
			tx.Unlock()
			panic("failed to unmarshal lease proto item")
		}

		// ID
		ID := LeaseID(lpb.ID)
		if lpb.TTL < le.minLeaseTTL {
			lpb.TTL = le.minLeaseTTL
		}

		// 初始化
		le.leaseMap[ID] = &Lease{
			ID:  ID,
			ttl: lpb.TTL,
			// itemSet will be filled in when recover key-value pairs
			// set expiry to forever, refresh when promoted
			itemSet: make(map[LeaseItem]struct{}),
			expiry:  forever,
			revokec: make(chan struct{}),
		}
	}

	// 建堆
	heap.Init(&le.leaseHeap)
	heap.Init(&le.leaseCheckpointHeap)

	// 解锁
	tx.Unlock()

	// 提交
	le.b.ForceCommit()
}

type Lease struct {
	// id
	ID LeaseID

	// ttl
	ttl int64 // time to live of the lease in seconds

	// 剩余时间
	remainingTTL int64 // remaining time to live in seconds, if zero valued it is considered unset and the full ttl should be used

	// 超时相关
	// expiryMu protects concurrent accesses to expiry
	expiryMu sync.RWMutex
	// expiry is time when lease should expire. no expiration when expiry.IsZero() is true
	expiry time.Time

	// 租约项
	// mu protects concurrent accesses to itemSet
	mu      sync.RWMutex
	itemSet map[LeaseItem]struct{}

	// 结束通道
	revokec chan struct{}
}

// 是否超时
func (l *Lease) expired() bool {
	return l.Remaining() <= 0
}

func (l *Lease) persistTo(b backend.Backend) {
	// 字节
	key := int64ToBytes(int64(l.ID))

	// 序列化值
	lpb := leasepb.Lease{ID: int64(l.ID), TTL: l.ttl, RemainingTTL: l.remainingTTL}
	val, err := lpb.Marshal()
	if err != nil {
		panic("failed to marshal lease proto item")
	}

	// 写db
	b.BatchTx().Lock()
	b.BatchTx().UnsafePut(leaseBucketName, key, val)
	b.BatchTx().Unlock()
}

// TTL returns the TTL of the Lease.
func (l *Lease) TTL() int64 {
	return l.ttl
}

// RemainingTTL returns the last checkpointed remaining TTL of the lease.
// TODO(jpbetz): do not expose this utility method
func (l *Lease) RemainingTTL() int64 {
	if l.remainingTTL > 0 {
		return l.remainingTTL
	}

	return l.ttl
}

// refresh refreshes the expiry of the lease.
// 刷新超时时间
func (l *Lease) refresh(extend time.Duration) {
	newExpiry := time.Now().Add(extend + time.Duration(l.RemainingTTL())*time.Second)

	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()

	l.expiry = newExpiry
}

// forever sets the expiry of lease to be forever.
// 永不超时
func (l *Lease) forever() {
	l.expiryMu.Lock()
	defer l.expiryMu.Unlock()

	l.expiry = forever
}

// Keys returns all the keys attached to the lease.
func (l *Lease) Keys() []string {
	l.mu.RLock()

	keys := make([]string, 0, len(l.itemSet))
	for k := range l.itemSet {
		keys = append(keys, k.Key)
	}

	l.mu.RUnlock()
	return keys
}

// Remaining returns the remaining time of the lease.
func (l *Lease) Remaining() time.Duration {
	l.expiryMu.RLock()
	defer l.expiryMu.RUnlock()

	if l.expiry.IsZero() {
		// 永不超时
		return time.Duration(math.MaxInt64)
	}

	// 返回剩余时间
	return time.Until(l.expiry)
}

// 租约item，key 为字符串
type LeaseItem struct {
	Key string
}

func int64ToBytes(n int64) []byte {
	bytes := make([]byte, 8)
	binary.BigEndian.PutUint64(bytes, uint64(n))
	return bytes
}

// FakeLessor is a fake implementation of Lessor interface.
// Used for testing only.
type FakeLessor struct{}

func (fl *FakeLessor) SetRangeDeleter(dr RangeDeleter) {}

func (fl *FakeLessor) SetCheckpointer(cp Checkpointer) {}

func (fl *FakeLessor) Grant(id LeaseID, ttl int64) (*Lease, error) { return nil, nil }

func (fl *FakeLessor) Revoke(id LeaseID) error { return nil }

func (fl *FakeLessor) Checkpoint(id LeaseID, remainingTTL int64) error { return nil }

func (fl *FakeLessor) Attach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) GetLease(item LeaseItem) LeaseID            { return 0 }
func (fl *FakeLessor) Detach(id LeaseID, items []LeaseItem) error { return nil }

func (fl *FakeLessor) Promote(extend time.Duration) {}

func (fl *FakeLessor) Demote() {}

func (fl *FakeLessor) Renew(id leaseid) (int64, error) { return 10, nil }

func (fl *FakeLessor) Lookup(id LeaseID) *Lease { return nil }

func (fl *FakeLessor) Leases() []*Lease { return nil }

func (fl *FakeLessor) ExpiredLeasesC() <-chan []*Lease { return nil }

func (fl *FakeLessor) Recover(b backend.Backend, rd RangeDeleter) {}

func (fl *FakeLessor) Stop() {}
