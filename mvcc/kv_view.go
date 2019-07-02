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

package mvcc

import "go.etcd.io/etcd/lease"

// 读视图
type readView struct {
    kv KV
}

// 第一个版本
func (rv *readView) FirstRev() int64 {
	tr := rv.kv.Read()
	defer tr.End()
	return tr.FirstRev()
}

// 当前
func (rv *readView) Rev() int64 {
	tr := rv.kv.Read()
	defer tr.End()
	return tr.Rev()
}

// 范围
func (rv *readView) Range(key, end []byte, ro RangeOptions) (r *RangeResult, err error) {
	tr := rv.kv.Read()
	defer tr.End()
	return tr.Range(key, end, ro)
}

// 写视图
type writeView struct{ kv KV }

// 删除
func (wv *writeView) DeleteRange(key, end []byte) (n, rev int64) {
	tw := wv.kv.Write()
	defer tw.End()
	return tw.DeleteRange(key, end)
}

// 写
func (wv *writeView) Put(key, value []byte, lease lease.LeaseID) (rev int64) {
	tw := wv.kv.Write()
	defer tw.End()
	return tw.Put(key, value, lease)
}
