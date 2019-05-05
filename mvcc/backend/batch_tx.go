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

package backend

import (
	"bytes"
	"math"
	"sync"
	"sync/atomic"
	"time"

	bolt "go.etcd.io/bbolt"
	"go.uber.org/zap"
)

type BatchTx interface {
    // 读接口
	ReadTx

	UnsafeCreateBucket(name []byte)
	UnsafePut(bucketName []byte, key []byte, value []byte)
	UnsafeSeqPut(bucketName []byte, key []byte, value []byte)
	UnsafeDelete(bucketName []byte, key []byte)

	// Commit commits a previous tx and begins a new writable one.
	Commit()

	// CommitAndStop commits the previous tx and does not create a new one.
	CommitAndStop()
}

type batchTx struct {
    // 锁
	sync.Mutex

	tx      *bolt.Tx

    // 后台线程
	backend *backend

    // bucket 数
	pending int
}

func (t *batchTx) UnsafeCreateBucket(name []byte) {
	_, err := t.tx.CreateBucket(name)
	if err != nil && err != bolt.ErrBucketExists {
	    // 错误
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to create a bucket",
				zap.String("bucket-name", string(name)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot create bucket %s (%v)", name, err)
		}
	}

    // ++
	t.pending++
}

// UnsafePut must be called holding the lock on the tx.
func (t *batchTx) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, false)
}

// UnsafeSeqPut must be called holding the lock on the tx.
func (t *batchTx) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.unsafePut(bucketName, key, value, true)
}

func (t *batchTx) unsafePut(bucketName []byte, key []byte, value []byte, seq bool) {
	// 找桶
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
	    // 无桶报错
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}

    // 神奇功效？？？？
	if seq {
		// it is useful to increase fill percent when the workloads are mostly append-only.
		// this can delay the page split and reduce space usage.
		bucket.FillPercent = 0.9
	}

    // 写kv
	if err := bucket.Put(key, value); err != nil {
	    // 写kv 失败
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to write to a bucket",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot put key into bucket (%v)", err)
		}
	}

    // 写成功
	t.pending++
}

// UnsafeRange must be called holding the lock on the tx.
func (t *batchTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
    // 找桶
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}

    // 查询
	return unsafeRange(bucket.Cursor(), key, endKey, limit)
}

func unsafeRange(c *bolt.Cursor, key, endKey []byte, limit int64) (keys [][]byte, vs [][]byte) {
	// 默认查询全部
	if limit <= 0 {
		limit = math.MaxInt64
	}

	var isMatch func(b []byte) bool
	if len(endKey) > 0 {
	    // 比endKey 小
		isMatch = func(b []byte) bool { return bytes.Compare(b, endKey) < 0 }
	} else {
	    // 只查询一个
		isMatch = func(b []byte) bool { return bytes.Equal(b, key) }
		limit = 1
	}

    // 一直往前走
	for ck, cv := c.Seek(key); ck != nil && isMatch(ck); ck, cv = c.Next() {
		// 取kv
		vs = append(vs, cv)
		keys = append(keys, ck)

        // 直至终点
		if limit == int64(len(keys)) {
			break
		}
	}

	return keys, vs
}

// UnsafeDelete must be called holding the lock on the tx.
func (t *batchTx) UnsafeDelete(bucketName []byte, key []byte) {
	// 找桶
	bucket := t.tx.Bucket(bucketName)
	if bucket == nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to find a bucket",
				zap.String("bucket-name", string(bucketName)),
			)
		} else {
			plog.Fatalf("bucket %s does not exist", bucketName)
		}
	}

    // 删除
	err := bucket.Delete(key)
	if err != nil {
		if t.backend.lg != nil {
			t.backend.lg.Fatal(
				"failed to delete a key",
				zap.String("bucket-name", string(bucketName)),
				zap.Error(err),
			)
		} else {
			plog.Fatalf("cannot delete key from bucket (%v)", err)
		}
	}

    // 完成
	t.pending++
}

// UnsafeForEach must be called holding the lock on the tx.
func (t *batchTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	return unsafeForEach(t.tx, bucketName, visitor)
}

func unsafeForEach(tx *bolt.Tx, bucket []byte, visitor func(k, v []byte) error) error {
	if b := tx.Bucket(bucket); b != nil {
		return b.ForEach(visitor)
	}

	return nil
}

// Commit commits a previous tx and begins a new writable one.
func (t *batchTx) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

// CommitAndStop commits the previous tx and does not create a new one.
func (t *batchTx) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTx) Unlock() {
    // 自动开始下一个
	if t.pending >= t.backend.batchLimit {
		t.commit(false)
	}

	t.Mutex.Unlock()
}

func (t *batchTx) safePending() int {
	t.Mutex.Lock()
	defer t.Mutex.Unlock()

	return t.pending
}

func (t *batchTx) commit(stop bool) {
	// commit the last tx
	if t.tx != nil {
		if t.pending == 0 && !stop {
		    // 无数据 且 未停止
			return
		}

        // 计时
		start := time.Now()

        // 提交
		// gofail: var beforeCommit struct{}
		err := t.tx.Commit()
		// gofail: var afterCommit struct{}

        // 打点
		rebalanceSec.Observe(t.tx.Stats().RebalanceTime.Seconds())
		spillSec.Observe(t.tx.Stats().SpillTime.Seconds())
		writeSec.Observe(t.tx.Stats().WriteTime.Seconds())
		commitSec.Observe(time.Since(start).Seconds())
		atomic.AddInt64(&t.backend.commits, 1)

        // 提交完成或错误
		t.pending = 0
		if err != nil {
			if t.backend.lg != nil {
				t.backend.lg.Fatal("failed to commit tx", zap.Error(err))
			} else {
				plog.Fatalf("cannot commit tx (%s)", err)
			}
		}
	}

    // 通知后台线程干活
	if !stop {
		t.tx = t.backend.begin(true)
	}
}

type batchTxBuffered struct {
	batchTx

	buf txWriteBuffer
}

func newBatchTxBuffered(backend *backend) *batchTxBuffered {
	tx := &batchTxBuffered{
		batchTx: batchTx{backend: backend},
		buf: txWriteBuffer{
			txBuffer: txBuffer{make(map[string]*bucketBuffer)},
			seq:      true,
		},
	}

    // 自动干活
	tx.Commit()

	return tx
}

func (t *batchTxBuffered) Unlock() {
	if t.pending != 0 {
	    // 写回到read 里面
		t.backend.readTx.mu.Lock()
		t.buf.writeback(&t.backend.readTx.buf)
		t.backend.readTx.mu.Unlock()

		if t.pending >= t.backend.batchLimit {
			t.commit(false)
		}
	}

	t.batchTx.Unlock()
}

func (t *batchTxBuffered) Commit() {
	t.Lock()
	t.commit(false)
	t.Unlock()
}

func (t *batchTxBuffered) CommitAndStop() {
	t.Lock()
	t.commit(true)
	t.Unlock()
}

func (t *batchTxBuffered) commit(stop bool) {
	// all read txs must be closed to acquire boltdb commit rwlock
	t.backend.readTx.mu.Lock()
	t.unsafeCommit(stop)
	t.backend.readTx.mu.Unlock()
}

func (t *batchTxBuffered) unsafeCommit(stop bool) {
	if t.backend.readTx.tx != nil {
		if err := t.backend.readTx.tx.Rollback(); err != nil {
			if t.backend.lg != nil {
				t.backend.lg.Fatal("failed to rollback tx", zap.Error(err))
			} else {
				plog.Fatalf("cannot rollback tx (%s)", err)
			}
		}
	
		t.backend.readTx.reset()
	}

	t.batchTx.commit(stop)

	if !stop {
		t.backend.readTx.tx = t.backend.begin(false)
	}
}

func (t *batchTxBuffered) UnsafePut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafePut(bucketName, key, value)

	t.buf.put(bucketName, key, value)
}

func (t *batchTxBuffered) UnsafeSeqPut(bucketName []byte, key []byte, value []byte) {
	t.batchTx.UnsafeSeqPut(bucketName, key, value)

	t.buf.putSeq(bucketName, key, value)
}
