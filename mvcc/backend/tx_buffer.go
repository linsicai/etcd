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
	"sort"
)

// txBuffer handles functionality shared between txWriteBuffer and txReadBuffer.
type txBuffer struct {
	buckets map[string]*bucketBuffer
}

// 清空
func (txb *txBuffer) reset() {
	for k, v := range txb.buckets {
		if v.used == 0 {
			// demote，降级
			// 删除
			delete(txb.buckets, k)
		}
		v.used = 0
	}
}

// txWriteBuffer buffers writes of pending updates that have not yet committed.
type txWriteBuffer struct {
	txBuffer
	seq bool
}

// 写
func (txw *txWriteBuffer) put(bucket, k, v []byte) {
	txw.seq = false

	txw.putSeq(bucket, k, v)
}

func (txw *txWriteBuffer) putSeq(bucket, k, v []byte) {
    // 找bucket
	b, ok := txw.buckets[string(bucket)]
	if !ok {
		b = newBucketBuffer()
		txw.buckets[string(bucket)] = b
	}

    // 写
	b.add(k, v)
}

func (txw *txWriteBuffer) writeback(txr *txReadBuffer) {
    // 遍历写缓存
	for k, wb := range txw.buckets {
		rb, ok := txr.buckets[k]
		if !ok {
		    // 读里没有，删除写，写进读里
			delete(txw.buckets, k)
			txr.buckets[k] = wb
			continue
		}

		if !txw.seq && wb.used > 1 {
			// assume no duplicate keys
			// 写排序
			sort.Sort(wb)
		}

        // 写合并进读里
		rb.merge(wb)
	}

    // 重置
	txw.reset()
}

// txReadBuffer accesses buffered updates.
// 写缓存
type txReadBuffer struct {
    txBuffer
}

func (txr *txReadBuffer) Range(bucketName, key, endKey []byte, limit int64) ([][]byte, [][]byte) {
    // 找到bucket，然后遍历
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.Range(key, endKey, limit)
	}

	return nil, nil
}

func (txr *txReadBuffer) ForEach(bucketName []byte, visitor func(k, v []byte) error) error {
    // 找到bucket，然后遍历
	if b := txr.buckets[string(bucketName)]; b != nil {
		return b.ForEach(visitor)
	}

	return nil
}

// key 与 value
type kv struct {
	key []byte
	val []byte
}

// bucketBuffer buffers key-value pairs that are pending commit.
type bucketBuffer struct {
    // kv列表
	buf []kv

	// used tracks number of elements in use so buf can be reused without reallocation.
	// buffer 数
	used int
}

func newBucketBuffer() *bucketBuffer {
	return &bucketBuffer {
	    buf: make([]kv, 512),
	    used: 0}
}

func (bb *bucketBuffer) Range(key, endKey []byte, limit int64) (keys [][]byte, vals [][]byte) {
	f := func(i int) bool {
	    return bytes.Compare(bb.buf[i].key, key) >= 0
	}

    // 找第一个比key 大的序号
	idx := sort.Search(bb.used, f)
	if idx < 0 {
		return nil, nil
	}
	
	if len(endKey) == 0 {
	    // 无终点，意思是找一样的
		if bytes.Equal(key, bb.buf[idx].key) {
			keys = append(keys, bb.buf[idx].key)
			vals = append(vals, bb.buf[idx].val)
		}

		return keys, vals
	}

    // 校验终点
	if bytes.Compare(endKey, bb.buf[idx].key) <= 0 {
		return nil, nil
	}

    // 遍历复制，为什么不做二分查找呢？
	for i := idx; i < bb.used && int64(len(keys)) < limit; i++ {
		if bytes.Compare(endKey, bb.buf[i].key) <= 0 {
			break
		}

		keys = append(keys, bb.buf[i].key)
		vals = append(vals, bb.buf[i].val)
	}

	return keys, vals
}

func (bb *bucketBuffer) ForEach(visitor func(k, v []byte) error) error {
    // 遍历调用visitor
	for i := 0; i < bb.used; i++ {
		if err := visitor(bb.buf[i].key, bb.buf[i].val); err != nil {
			return err
		}
	}

	return nil
}

func (bb *bucketBuffer) add(k, v []byte) {
    // append kv
	bb.buf[bb.used].key, bb.buf[bb.used].val = k, v
	bb.used++

	if bb.used == len(bb.buf) {
	    // 扩1.5倍
		buf := make([]kv, (3*len(bb.buf))/2)
		copy(buf, bb.buf)
		bb.buf = buf
	}
}

// merge merges data from bb into bbsrc.
func (bb *bucketBuffer) merge(bbsrc *bucketBuffer) {
    // src to bb
	for i := 0; i < bbsrc.used; i++ {
		bb.add(bbsrc.buf[i].key, bbsrc.buf[i].val)
	}

	if bb.used == bbsrc.used {
	    // bb原来是空的
		return
	}

    // 赠序append
	if bytes.Compare(bb.buf[(bb.used-bbsrc.used)-1].key, bbsrc.buf[0].key) < 0 {
		return
	}

    // 排序
	sort.Stable(bb)

	// remove duplicates, using only newest update
	// 去除重复的key，保留最新值
	widx := 0
	for ridx := 1; ridx < bb.used; ridx++ {
		if !bytes.Equal(bb.buf[ridx].key, bb.buf[widx].key) {
			widx++
		}

		bb.buf[widx] = bb.buf[ridx]
	}

	bb.used = widx + 1
}

// changduq
func (bb *bucketBuffer) Len() int { return bb.used }

// 排序使用
func (bb *bucketBuffer) Less(i, j int) bool {
	return bytes.Compare(bb.buf[i].key, bb.buf[j].key) < 0
}
func (bb *bucketBuffer) Swap(i, j int) {
    bb.buf[i], bb.buf[j] = bb.buf[j], bb.buf[i]
}
