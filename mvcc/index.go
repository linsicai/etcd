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
	"sort"
	"sync"

	"github.com/google/btree"
	"go.uber.org/zap"
)

// 索引接口
type index interface {
	Get(key []byte, atRev int64) (rev, created revision, ver int64, err error)
	Range(key, end []byte, atRev int64) ([][]byte, []revision)
	Revisions(key, end []byte, atRev int64) []revision
	Put(key []byte, rev revision)
	Tombstone(key []byte, rev revision) error
	RangeSince(key, end []byte, rev int64) []revision
	Compact(rev int64) map[revision]struct{}
	Keep(rev int64) map[revision]struct{}
	Equal(b index) bool

	Insert(ki *keyIndex)
	KeyIndex(ki *keyIndex) *keyIndex
}

// 树索引
type treeIndex struct {
	sync.RWMutex // 锁

	tree *btree.BTree // btree

	lg   *zap.Logger // 日志
}

func newTreeIndex(lg *zap.Logger) index {
	return &treeIndex{
		tree: btree.New(32),
		lg:   lg,
	}
}

func (ti *treeIndex) Put(key []byte, rev revision) {
    // key
	keyi := &keyIndex{key: key}

    // 加锁
	ti.Lock()
	defer ti.Unlock()

    // 找key 值
	item := ti.tree.Get(keyi)
	if item == nil {
	    // 更新存储
		keyi.put(ti.lg, rev.main, rev.sub)

        // 更新索引
		ti.tree.ReplaceOrInsert(keyi)
		return
	}

    // 写kv
	okeyi := item.(*keyIndex)
	okeyi.put(ti.lg, rev.main, rev.sub)
}

func (ti *treeIndex) Get(key []byte, atRev int64) (modified, created revision, ver int64, err error) {
    // 拼key
	keyi := &keyIndex{key: key}

    // 加锁
	ti.RLock()
	defer ti.RUnlock()

	if keyi = ti.keyIndex(keyi); keyi == nil {
	    // 找不到
		return revision{}, revision{}, 0, ErrRevisionNotFound
	}

    // 找到了
	return keyi.get(ti.lg, atRev)
}

func (ti *treeIndex) KeyIndex(keyi *keyIndex) *keyIndex {
	ti.RLock()
	defer ti.RUnlock()

    // 转调用
	return ti.keyIndex(keyi)
}

// 从索引中找
func (ti *treeIndex) keyIndex(keyi *keyIndex) *keyIndex {
	if item := ti.tree.Get(keyi); item != nil {
		return item.(*keyIndex)
	}

	return nil
}

func (ti *treeIndex) visit(key, end []byte, f func(ki *keyIndex)) {
    // [begin, end)
	keyi, endi := &keyIndex{key: key}, &keyIndex{key: end}

    // 加锁
	ti.RLock()
	defer ti.RUnlock()

    // 访问树
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
	    // 终点判断
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}

        // 访问
		f(item.(*keyIndex))
		return true
	})
}

func (ti *treeIndex) Revisions(key, end []byte, atRev int64) (revs []revision) {
	if end == nil {
	    // 单点处理
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil
		}
		return []revision{rev}
	}

    // 获取版本号
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
		}
	})
	return revs
}

func (ti *treeIndex) Range(key, end []byte, atRev int64) (keys [][]byte, revs []revision) {
	if end == nil {
	    // 单点处理
		rev, _, _, err := ti.Get(key, atRev)
		if err != nil {
			return nil, nil
		}
		return [][]byte{key}, []revision{rev}
	}

    // 获取版本号和key
	ti.visit(key, end, func(ki *keyIndex) {
		if rev, _, _, err := ki.get(ti.lg, atRev); err == nil {
			revs = append(revs, rev)
			keys = append(keys, ki.key)
		}
	})
	return keys, revs
}

// 里程碑
func (ti *treeIndex) Tombstone(key []byte, rev revision) error {
	keyi := &keyIndex{key: key}

    // 加锁
	ti.Lock()
	defer ti.Unlock()

    // 找一下
	item := ti.tree.Get(keyi)
	if item == nil {
		return ErrRevisionNotFound
	}

    // 里程碑
	ki := item.(*keyIndex)
	return ki.tombstone(ti.lg, rev.main, rev.sub)
}

// RangeSince returns all revisions from key(including) to end(excluding)
// at or after the given rev. The returned slice is sorted in the order
// of revision.
func (ti *treeIndex) RangeSince(key, end []byte, rev int64) []revision {
	keyi := &keyIndex{key: key}

    // 加锁
	ti.RLock()
	defer ti.RUnlock()

	if end == nil {
	    // 单点
		item := ti.tree.Get(keyi)
		if item == nil {
			return nil
		}
		keyi = item.(*keyIndex)
		return keyi.since(ti.lg, rev)
	}

    // 遍历
	endi := &keyIndex{key: end}
	var revs []revision
	ti.tree.AscendGreaterOrEqual(keyi, func(item btree.Item) bool {
		if len(endi.key) > 0 && !item.Less(endi) {
			return false
		}
		curKeyi := item.(*keyIndex)
		revs = append(revs, curKeyi.since(ti.lg, rev)...)
		return true
	})
	sort.Sort(revisions(revs))

	return revs
}

// 压缩
func (ti *treeIndex) Compact(rev int64) map[revision]struct{} {
    // 可用为空
	available := make(map[revision]struct{})
	if ti.lg != nil {
		ti.lg.Info("compact tree index", zap.Int64("revision", rev))
	} else {
		plog.Printf("store.index: compact %d", rev)
	}

    // 复制树
	ti.Lock()
	clone := ti.tree.Clone()
	ti.Unlock()

	clone.Ascend(func(item btree.Item) bool {
		keyi := item.(*keyIndex)

		//Lock is needed here to prevent modification to the keyIndex while
		//compaction is going on or revision added to empty before deletion
		ti.Lock()

        // 压缩key
		keyi.compact(ti.lg, rev, available)
		if keyi.isEmpty() {
		    // 删除索引
			item := ti.tree.Delete(keyi)
			if item == nil {
				if ti.lg != nil {
					ti.lg.Panic("failed to delete during compaction")
				} else {
					plog.Panic("store.index: unexpected delete failure during compaction")
				}
			}
		}

		ti.Unlock()
		return true
	})

	return available
}

// Keep finds all revisions to be kept for a Compaction at the given rev.
func (ti *treeIndex) Keep(rev int64) map[revision]struct{} {
	available := make(map[revision]struct{})

    // 加锁
	ti.RLock()
	defer ti.RUnlock()

    // 遍历树
	ti.tree.Ascend(func(i btree.Item) bool {
		keyi := i.(*keyIndex)
		keyi.keep(rev, available)
		return true
	})

	return available
}

func (ti *treeIndex) Equal(bi index) bool {
    // 类型转换
	b := bi.(*treeIndex)

    // 看长度
	if ti.tree.Len() != b.tree.Len() {
		return false
	}

	equal := true

    // 遍历树判断
	ti.tree.Ascend(func(item btree.Item) bool {
		aki := item.(*keyIndex)
		bki := b.tree.Get(item).(*keyIndex)
		if !aki.equal(bki) {
			equal = false
			return false
		}
		return true
	})

	return equal
}

func (ti *treeIndex) Insert(ki *keyIndex) {
	ti.Lock()
	defer ti.Unlock()

	ti.tree.ReplaceOrInsert(ki)
}
