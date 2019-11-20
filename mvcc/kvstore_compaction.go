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
	"encoding/binary"
	"time"

	"go.uber.org/zap"
)

// 周期性压缩
func (s *store) scheduleCompaction(compactMainRev int64, keep map[revision]struct{}) bool {
	// 结束时统计打点
	totalStart := time.Now()
	defer dbCompactionTotalMs.Observe(float64(time.Since(totalStart) / time.Millisecond))
	keyCompactions := 0
	defer func() { dbCompactionKeysCounter.Add(float64(keyCompactions)) }()

	// 计算结束版本
	end := make([]byte, 8)
	binary.BigEndian.PutUint64(end, uint64(compactMainRev+1))

	batchsize := int64(10000)
	last := make([]byte, 8+1+8) // 记录已经处理过最大版本

	// 死循环
	for {
		var rev revision

		// 开始
		start := time.Now()

		// 事务加锁
		tx := s.b.BatchTx()
		tx.Lock()

		// 批量获取老版本key
		keys, _ := tx.UnsafeRange(keyBucketName, last, end, batchsize)

		// 如果不在保留版本中，删除这些key
		for _, key := range keys {
			rev = bytesToRev(key)
			if _, ok := keep[rev]; !ok {
				tx.UnsafeDelete(keyBucketName, key)
				keyCompactions++
			}
		}

		if len(keys) < int(batchsize) {
			// 如果没那么多需要处理

			// 更新当前主版本
			rbytes := make([]byte, 8+1+8)
			revToBytes(revision{main: compactMainRev}, rbytes)
			tx.UnsafePut(metaBucketName, finishedCompactKeyName, rbytes)
			tx.Unlock()

			// 打个日志
			if s.lg != nil {
				s.lg.Info(
					"finished scheduled compaction",
					zap.Int64("compact-revision", compactMainRev),
					zap.Duration("took", time.Since(totalStart)),
				)
			} else {
				plog.Printf("finished scheduled compaction at %d (took %v)", compactMainRev, time.Since(totalStart))
			}

			return true
		}

		// update last
		// 更新版本
		revToBytes(revision{main: rev.main, sub: rev.sub + 1}, last)

		// 解锁，统计一会
		tx.Unlock()
		dbCompactionPauseMs.Observe(float64(time.Since(start) / time.Millisecond))

		// 消停一会，同时看看是否结束了
		select {
		case <-time.After(100 * time.Millisecond):
		case <-s.stopc:
			return false
		}
	}
}
