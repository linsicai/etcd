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

package mvcc

import (
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"math"
	"sync"
	"sync/atomic"
	"time"

	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/mvcc/mvccpb"
	"go.etcd.io/etcd/pkg/schedule"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

var (
	keyBucketName  = []byte("key")
	metaBucketName = []byte("meta")

	consistentIndexKeyName  = []byte("consistent_index")
	scheduledCompactKeyName = []byte("scheduledCompactRev")
	finishedCompactKeyName  = []byte("finishedCompactRev")

    // 错误
	ErrCompacted = errors.New("mvcc: required revision has been compacted")
	ErrFutureRev = errors.New("mvcc: required revision is a future revision")
	ErrCanceled  = errors.New("mvcc: watcher is canceled")
	ErrClosed    = errors.New("mvcc: closed")

    // 日志
	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "mvcc")
)

const (
	// markedRevBytesLen is the byte length of marked revision.
	// The first `revBytesLen` bytes represents a normal revision. The last
	// one byte is the mark.
	markedRevBytesLen      = revBytesLen + 1
	markBytePosition       = markedRevBytesLen - 1
	markTombstone     byte = 't'
)

var restoreChunkKeys = 10000 // non-const for testing

// ConsistentIndexGetter is an interface that wraps the Get method.
// Consistent index is the offset of an entry in a consistent replicated log.
type ConsistentIndexGetter interface {
	// ConsistentIndex returns the consistent index of current executing entry.
	ConsistentIndex() uint64
}

type store struct {
	ReadView
	WriteView

	// consistentIndex caches the "consistent_index" key's value. Accessed
	// through atomics so must be 64-bit aligned.
	// 一致性索引缓存
	consistentIndex uint64

	// mu read locks for txns and write locks for non-txn store changes.
	mu sync.RWMutex

	ig ConsistentIndexGetter

	b       backend.Backend
	kvindex index

	le lease.Lessor

	// revMuLock protects currentRev and compactMainRev.
	// Locked at end of write txn and released after write txn unlock lock.
	// Locked before locking read txn and released after locking.
	revMu sync.RWMutex
	// currentRev is the revision of the last completed transaction.
	currentRev int64
	// compactMainRev is the main revision of the last compaction.
	compactMainRev int64

	// bytesBuf8 is a byte slice of length 8
	// to avoid a repetitive allocation in saveIndex.
	bytesBuf8 []byte

	fifoSched schedule.Scheduler

	stopc chan struct{}

	lg *zap.Logger
}

// NewStore returns a new store. It is useful to create a store inside
// mvcc pkg. It should only be used for testing externally.
func NewStore(lg *zap.Logger, b backend.Backend, le lease.Lessor, ig ConsistentIndexGetter) *store {
    // 创建对象
	s := &store{
		b:       b,
		ig:      ig,
		kvindex: newTreeIndex(lg),

		le: le,

		currentRev:     1,
		compactMainRev: -1,

		bytesBuf8: make([]byte, 8),
		fifoSched: schedule.NewFIFOScheduler(),

		stopc: make(chan struct{}),

		lg: lg,
	}

    // 创建读写视图
	s.ReadView = &readView{s}
	s.WriteView = &writeView{s}

    // 设置租约函数
	if s.le != nil {
		s.le.SetRangeDeleter(func() lease.TxnDelete { return s.Write() })
	}

    // 创建bucket
	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafeCreateBucket(keyBucketName)
	tx.UnsafeCreateBucket(metaBucketName)
	tx.Unlock()
	s.b.ForceCommit()

    // 加锁恢复数据
	s.mu.Lock()
	defer s.mu.Unlock()
	if err := s.restore(); err != nil {
		// TODO: return the error instead of panic here?
		panic("failed to recover store from backend")
	}

	return s
}

// ？？？
func (s *store) compactBarrier(ctx context.Context, ch chan struct{}) {
	if ctx == nil || ctx.Err() != nil {
	    // 上下文出错了
		s.mu.Lock()
		select {
		case <-s.stopc: // 等待结束
		default:
		    // 重试？
			f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
			s.fifoSched.Schedule(f)
		}
		s.mu.Unlock()
		return
	}

    // 结束
	close(ch)
}

func (s *store) Hash() (hash uint32, revision int64, err error) {
	start := time.Now()

    // 强制提交
	s.b.ForceCommit()

    // 计算耗时
	h, err := s.b.Hash(DefaultIgnores)

    // 耗时统计
	hashSec.Observe(time.Since(start).Seconds())

	return h, s.currentRev, err
}

func (s *store) HashByRev(rev int64) (hash uint32, currentRev int64, compactRev int64, err error) {
	start := time.Now()

	s.mu.RLock()

    // 读取当前版本信息
	s.revMu.RLock()
	compactRev, currentRev = s.compactMainRev, s.currentRev
	s.revMu.RUnlock()

    // 版本校验
	if rev > 0 && rev <= compactRev {
		s.mu.RUnlock()
		return 0, 0, compactRev, ErrCompacted
	} else if rev > 0 && rev > currentRev {
		s.mu.RUnlock()
		return 0, currentRev, 0, ErrFutureRev
	}

	if rev == 0 {
		rev = currentRev
	}
	keep := s.kvindex.Keep(rev)

    // 存储加锁
	tx := s.b.ReadTx()
	tx.Lock()
	defer tx.Unlock()
	s.mu.RUnlock()

	upper := revision{main: rev + 1}
	lower := revision{main: compactRev + 1}
	h := crc32.New(crc32.MakeTable(crc32.Castagnoli))

	h.Write(keyBucketName)
	err = tx.UnsafeForEach(keyBucketName, func(k, v []byte) error {
		kr := bytesToRev(k)
		if !upper.GreaterThan(kr) {
		    // 过右界了
			return nil
		}
		// skip revisions that are scheduled for deletion
		// due to compacting; don't skip if there isn't one.
		if lower.GreaterThan(kr) && len(keep) > 0 {
			if _, ok := keep[kr]; !ok {
			    // 太小且不需要的keep
				return nil
			}
		}

        //！写kv
		h.Write(k)
		h.Write(v)
		return nil
	})
	hash = h.Sum32()

	hashRevSec.Observe(time.Since(start).Seconds())
	return hash, currentRev, compactRev, err
}

func (s *store) updateCompactRev(rev int64) (<-chan struct{}, error) {
    // 加锁
	s.revMu.Lock()

	if rev <= s.compactMainRev {
	    // 老的版本
		ch := make(chan struct{})
		f := func(ctx context.Context) { s.compactBarrier(ctx, ch) }
		s.fifoSched.Schedule(f)
		s.revMu.Unlock()
		return ch, ErrCompacted
	}

	if rev > s.currentRev {
	    // 太超前的版本，报错
		s.revMu.Unlock()
		return nil, ErrFutureRev
	}

    // 记录当前版本
	s.compactMainRev = rev

    // 序列化
	rbytes := newRevBytes()
	revToBytes(revision{main: rev}, rbytes)

    // 写db
	tx := s.b.BatchTx()
	tx.Lock()
	tx.UnsafePut(metaBucketName, scheduledCompactKeyName, rbytes)
	tx.Unlock()
	// ensure that desired compaction is persisted
	s.b.ForceCommit()

	s.revMu.Unlock()

	return nil, nil
}

func (s *store) compact(rev int64) (<-chan struct{}, error) {
	start := time.Now()

    // kv 索引压缩
	keep := s.kvindex.Compact(rev)

	ch := make(chan struct{})
	var j = func(ctx context.Context) {
		if ctx.Err() != nil {
		    // 上下文出错
			s.compactBarrier(ctx, ch)
			return
		}
		if !s.scheduleCompaction(rev, keep) {
		    // 周期性压缩出错
			s.compactBarrier(nil, ch)
			return
		}

        // 结束
		close(ch)
	}

    // 入周期调度队列
	s.fifoSched.Schedule(j)

	indexCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))
	return ch, nil
}

func (s *store) compactLockfree(rev int64) (<-chan struct{}, error) {
	ch, err := s.updateCompactRev(rev)
	if nil != err {
		return ch, err
	}

	return s.compact(rev)
}

func (s *store) Compact(rev int64) (<-chan struct{}, error) {
	s.mu.Lock()

	ch, err := s.updateCompactRev(rev)

	if err != nil {
		s.mu.Unlock()
		return ch, err
	}
	s.mu.Unlock()

	return s.compact(rev)
}

// DefaultIgnores is a map of keys to ignore in hash checking.
var DefaultIgnores map[backend.IgnoreKey]struct{}

func init() {
	DefaultIgnores = map[backend.IgnoreKey]struct{}{
		// consistent index might be changed due to v2 internal sync, which
		// is not controllable by the user.
		{Bucket: string(metaBucketName), Key: string(consistentIndexKeyName)}: {},
	}
}

func (s *store) Commit() {
    // 加锁
	s.mu.Lock()
	defer s.mu.Unlock()

    // db 加锁保存提交
	tx := s.b.BatchTx()
	tx.Lock()
	s.saveIndex(tx)
	tx.Unlock()
	s.b.ForceCommit()
}

func (s *store) Restore(b backend.Backend) error {
    // 加锁
	s.mu.Lock()
	defer s.mu.Unlock()

    // 停止
	close(s.stopc)
	s.fifoSched.Stop()

    // 清空一致性索引
	atomic.StoreUint64(&s.consistentIndex, 0)

    // 清空所有值
	s.b = b
	s.kvindex = newTreeIndex(s.lg)
	s.currentRev = 1
	s.compactMainRev = -1
	s.fifoSched = schedule.NewFIFOScheduler()
	s.stopc = make(chan struct{})

    // 恢复
	return s.restore()
}

func (s *store) restore() error {
	b := s.b
	// 上报db 大小
	reportDbTotalSizeInBytesMu.Lock()
	reportDbTotalSizeInBytes = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesMu.Unlock()
	reportDbTotalSizeInBytesDebuggingMu.Lock()
	reportDbTotalSizeInBytesDebugging = func() float64 { return float64(b.Size()) }
	reportDbTotalSizeInBytesDebuggingMu.Unlock()
	reportDbTotalSizeInUseInBytesMu.Lock()
	reportDbTotalSizeInUseInBytes = func() float64 { return float64(b.SizeInUse()) }
	reportDbTotalSizeInUseInBytesMu.Unlock()

	min, max := newRevBytes(), newRevBytes()
	revToBytes(revision{main: 1}, min)
	revToBytes(revision{main: math.MaxInt64, sub: math.MaxInt64}, max)

	keyToLease := make(map[string]lease.LeaseID)

	// restore index
	tx := s.b.BatchTx()
	tx.Lock()

    // 读取已结束压缩版本号
	_, finishedCompactBytes := tx.UnsafeRange(metaBucketName, finishedCompactKeyName, nil, 0)
	if len(finishedCompactBytes) != 0 {
		s.compactMainRev = bytesToRev(finishedCompactBytes[0]).main

        // 日志
		if s.lg != nil {
			s.lg.Info(
				"restored last compact revision",
				zap.String("meta-bucket-name", string(metaBucketName)),
				zap.String("meta-bucket-name-key", string(finishedCompactKeyName)),
				zap.Int64("restored-compact-revision", s.compactMainRev),
			)
		} else {
			plog.Printf("restore compact to %d", s.compactMainRev)
		}
	}

    // 读取周期性结束版本号
	_, scheduledCompactBytes := tx.UnsafeRange(metaBucketName, scheduledCompactKeyName, nil, 0)
	scheduledCompact := int64(0)
	if len(scheduledCompactBytes) != 0 {
		scheduledCompact = bytesToRev(scheduledCompactBytes[0]).main
	}

	// index keys concurrently as they're loaded in from tx
	keysGauge.Set(0)
	// 创建通道
	rkvc, revc := restoreIntoIndex(s.lg, s.kvindex)
	for {
	    // 批量读取kv
		keys, vals := tx.UnsafeRange(keyBucketName, min, max, int64(restoreChunkKeys))
		if len(keys) == 0 {
			break
		}

		// rkvc blocks if the total pending keys exceeds the restore
		// chunk size to keep keys from consuming too much memory.
		// kv to chunk
		restoreChunk(s.lg, rkvc, keys, vals, keyToLease)
		if len(keys) < restoreChunkKeys {
			// partial set implies final set
			break
		}

		// next set begins after where this one ended
		// 更新最小版本
		newMin := bytesToRev(keys[len(keys)-1][:revBytesLen])
		newMin.sub++
		revToBytes(newMin, min)
	}
	// 结束并等待结果
	close(rkvc)
	s.currentRev = <-revc

	// keys in the range [compacted revision -N, compaction] might all be deleted due to compaction.
	// the correct revision should be set to compaction revision in the case, not the largest revision
	// we have seen.
	// 更新版本号信息
	if s.currentRev < s.compactMainRev {
		s.currentRev = s.compactMainRev
	}
	if scheduledCompact <= s.compactMainRev {
		scheduledCompact = 0
	}

    // 租约
	for key, lid := range keyToLease {
		if s.le == nil {
			panic("no lessor to attach lease")
		}
		err := s.le.Attach(lid, []lease.LeaseItem{{Key: key}})
		if err != nil {
			if s.lg != nil {
				s.lg.Warn(
					"failed to attach a lease",
					zap.String("lease-id", fmt.Sprintf("%016x", lid)),
					zap.Error(err),
				)
			} else {
				plog.Errorf("unexpected Attach error: %v", err)
			}
		}
	}

	tx.Unlock()

	if scheduledCompact != 0 {
	    // 恢复调度压缩？
		s.compactLockfree(scheduledCompact)

		if s.lg != nil {
			s.lg.Info(
				"resume scheduled compaction",
				zap.String("meta-bucket-name", string(metaBucketName)),
				zap.String("meta-bucket-name-key", string(scheduledCompactKeyName)),
				zap.Int64("scheduled-compact-revision", scheduledCompact),
			)
		} else {
			plog.Printf("resume scheduled compaction at %d", scheduledCompact)
		}
	}

	return nil
}

// 
type revKeyValue struct {
	key  []byte
	kv   mvccpb.KeyValue
	kstr string
}

func restoreIntoIndex(lg *zap.Logger, idx index) (chan<- revKeyValue, <-chan int64) {
    // 创建通道，返回去
	rkvc, revc := make(chan revKeyValue, restoreChunkKeys), make(chan int64, 1)
	go func() {
	    // 结束后返回结束版本
		currentRev := int64(1)
		defer func() { revc <- currentRev }()

		// restore the tree index from streaming the unordered index.
		kiCache := make(map[string]*keyIndex, restoreChunkKeys)

        // 遍历输入
		for rkv := range rkvc {
			ki, ok := kiCache[rkv.kstr]

			// purge kiCache if many keys but still missing in the cache
			// 缓存中找不到，清除一点点
			if !ok && len(kiCache) >= restoreChunkKeys {
				i := 10
				for k := range kiCache {
					delete(kiCache, k)
					if i--; i == 0 {
						break
					}
				}
			}
	
			// cache miss, fetch from tree index if there
			if !ok {
			    // 从索引中加载
				ki = &keyIndex{key: rkv.kv.Key}
				if idxKey := idx.KeyIndex(ki); idxKey != nil {
					kiCache[rkv.kstr], ki = idxKey, idxKey
					ok = true
				}
			}
		
		    // key 版本和当前版本
			rev := bytesToRev(rkv.key)
			currentRev = rev.main
			if ok {
			    // 找到后的处理，做更新
				if isTombstone(rkv.key) {
					ki.tombstone(lg, rev.main, rev.sub)
					continue
				}
				ki.put(lg, rev.main, rev.sub)
			} else if !isTombstone(rkv.key) {
			    // 存储找不到，做恢复
				ki.restore(lg, revision{rkv.kv.CreateRevision, 0}, rev, rkv.kv.Version)
				idx.Insert(ki)
				kiCache[rkv.kstr] = ki
			}
		}
	}()

	return rkvc, revc
}

func restoreChunk(lg *zap.Logger,
                  kvc chan<- revKeyValue,
                  keys, vals [][]byte,
                  keyToLease map[string]lease.LeaseID) {
    // 遍历key
	for i, key := range keys {
		rkv := revKeyValue{key: key}

        // 反序列化value
		if err := rkv.kv.Unmarshal(vals[i]); err != nil {
			if lg != nil {
				lg.Fatal("failed to unmarshal mvccpb.KeyValue", zap.Error(err))
			} else {
				plog.Fatalf("cannot unmarshal event: %v", err)
			}
		}

        // key to string
		rkv.kstr = string(rkv.kv.Key)

		if isTombstone(key) {
		    // 墓碑删除租约
			delete(keyToLease, rkv.kstr)
		} else if lid := lease.LeaseID(rkv.kv.Lease); lid != lease.NoLease {
		    // 有租约
			keyToLease[rkv.kstr] = lid
		} else {
		    // 默认删除租约
			delete(keyToLease, rkv.kstr)
		}

        // 丢到通道里面
		kvc <- rkv
	}
}

func (s *store) Close() error {
    // 开启停止通道
	close(s.stopc)

    // 停止调度
	s.fifoSched.Stop()

	return nil
}

func (s *store) saveIndex(tx backend.BatchTx) {
	if s.ig == nil {
		return
	}

    // 序列化一致性索引
	bs := s.bytesBuf8
	ci := s.ig.ConsistentIndex()
	binary.BigEndian.PutUint64(bs, ci)

	// put the index into the underlying backend
	// tx has been locked in TxnBegin, so there is no need to lock it again
	// 写db 和 缓存
	tx.UnsafePut(metaBucketName, consistentIndexKeyName, bs)
	atomic.StoreUint64(&s.consistentIndex, ci)
}

func (s *store) ConsistentIndex() uint64 {
    // 从缓存中获取
	if ci := atomic.LoadUint64(&s.consistentIndex); ci > 0 {
		return ci
	}

    // 加锁
	tx := s.b.BatchTx()
	tx.Lock()
	defer tx.Unlock()

    // 读存储
	_, vs := tx.UnsafeRange(metaBucketName, consistentIndexKeyName, nil, 0)
	if len(vs) == 0 {
		return 0
	}

    // 转u64
	v := binary.BigEndian.Uint64(vs[0])

    // 写缓存
	atomic.StoreUint64(&s.consistentIndex, v)
	return v
}

// appendMarkTombstone appends tombstone mark to normal revision bytes.
func appendMarkTombstone(lg *zap.Logger, b []byte) []byte {
	if len(b) != revBytesLen {
	    // 前置检查
		if lg != nil {
			lg.Panic(
				"cannot append tombstone mark to non-normal revision bytes",
				zap.Int("expected-revision-bytes-size", revBytesLen),
				zap.Int("given-revision-bytes-size", len(b)),
			)
		} else {
			plog.Panicf("cannot append mark to non normal revision bytes")
		}
	}

    // append mark
	return append(b, markTombstone)
}

// isTombstone checks whether the revision bytes is a tombstone.
func isTombstone(b []byte) bool {
	return \
	    len(b) == markedRevBytesLen && \
	    b[markBytePosition] == markTombstone
}
