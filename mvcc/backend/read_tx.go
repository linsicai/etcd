// Copyright 2017 The etcd Authors
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

	bolt "go.etcd.io/bbolt"
)

// safeRangeBucket is a hack to avoid inadvertently reading duplicate keys;
// overwrites on a bucket should only fetch with limit=1, but safeRangeBucket
// is known to never overwrite any key so range is safe.
var safeRangeBucket = []byte("key")

// 接口
type ReadTx interface {
	// 锁
	Lock()
	Unlock()

	// 批量读取kvs
	UnsafeRange(bucketName []byte, key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte)

	// 遍历kvs
	UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error
}

type readTx struct {
	// mu protects accesses to the txReadBuffer
	// buffer锁
	mu sync.RWMutex

	// buffer
	buf txReadBuffer

	// txmu protects accesses to buckets and tx on Range requests.
	txmu sync.RWMutex
	tx   *bolt.Tx

	// 桶
	buckets map[string]*bolt.Bucket
}

func (rt *readTx) Lock()   { rt.mu.RLock() }
func (rt *readTx) Unlock() { rt.mu.RUnlock() }

func (rt *readTx) UnsafeRange(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
	if endKey == nil {
		// forbid duplicates for single keys
		// 默认单key
		limit = 1
	}
	if limit <= 0 {
		// 全量
		limit = math.MaxInt64
	}

	if limit > 1 && !bytes.Equal(bucketName, safeRangeBucket) {
		// 告警
		panic("do not use unsafeRange on non-keys bucket")
	}

	// 从buffer 里面找
	keys, vals := rt.buf.Range(bucketName, key, endKey, limit)
	if int64(len(keys)) == limit {
		return keys, vals
	}

	// 找bucket，找不到则新建一个
	// find/cache bucket
	bn := string(bucketName)
	rt.txmu.RLock()
	bucket, ok := rt.buckets[bn]
	rt.txmu.RUnlock()
	if !ok {
		rt.txmu.Lock()
		bucket = rt.tx.Bucket(bucketName)
		rt.buckets[bn] = bucket
		rt.txmu.Unlock()
	}

	// ignore missing bucket since may have been created in this batch
	// 无bucket
	if bucket == nil {
		return keys, vals
	}

	// 找到当前cursor
	rt.txmu.Lock()
	c := bucket.Cursor()
	rt.txmu.Unlock()

	// 找bucket
	k2, v2 := unsafeRange(c, key, endKey, limit-int64(len(keys)))

	return append(k2, keys...), append(v2, vals...)
}

func (rt *readTx) UnsafeForEach(bucketName []byte, visitor func(k, v []byte) error) error {
	dups := make(map[string]struct{})
	getDups := func(k, v []byte) error {
		dups[string(k)] = struct{}{}
		return nil
	}
	visitNoDup := func(k, v []byte) error {
		if _, ok := dups[string(k)]; ok {
			return nil
		}

		return visitor(k, v)
	}

	// 从buffer 提取keys
	if err := rt.buf.ForEach(bucketName, getDups); err != nil {
		return err
	}

	// 从tx 提取剩余的keys
	rt.txmu.Lock()
	err := unsafeForEach(rt.tx, bucketName, visitNoDup)
	rt.txmu.Unlock()

	// 出错
	if err != nil {
		return err
	}

	// 遍历buffer
	return rt.buf.ForEach(bucketName, visitor)
}

// 重置
func (rt *readTx) reset() {
	rt.buf.reset()

	rt.buckets = make(map[string]*bolt.Bucket)

	rt.tx = nil
}
