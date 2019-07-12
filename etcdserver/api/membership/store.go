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

package membership

import (
	"encoding/json"
	"fmt"
	"path"

	"go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/pkg/types"

	"github.com/coreos/go-semver/semver"
)

const (
	// 属性值
	attributesSuffix     = "attributes"
	raftAttributesSuffix = "raftAttributes"

	// the prefix for stroing membership related information in store provided by store pkg.
	storePrefix = "/0"
)

var (
	membersBucketName        = []byte("members")         // 成员
	membersRemovedBucketName = []byte("members_removed") // 已删除成员
	clusterBucketName        = []byte("cluster")         // 集群

	StoreMembersPrefix        = path.Join(storePrefix, "members")
	storeRemovedMembersPrefix = path.Join(storePrefix, "removed_members")
)

// 写成员
func mustSaveMemberToBackend(be backend.Backend, m *Member) {
	// id to key
	mkey := backendMemberKey(m.ID)

	// member 序列化
	mvalue, err := json.Marshal(m)
	if err != nil {
		plog.Panicf("marshal raftAttributes should never fail: %v", err)
	}

	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafePut(membersBucketName, mkey, mvalue)
	tx.Unlock()
}

// 删除成员
func mustDeleteMemberFromBackend(be backend.Backend, id types.ID) {
	// id to key
	mkey := backendMemberKey(id)

	tx := be.BatchTx()
	tx.Lock()
	tx.UnsafeDelete(membersBucketName, mkey)
	tx.UnsafePut(membersRemovedBucketName, mkey, []byte("removed"))
	tx.Unlock()
}

// 写集群版本
func mustSaveClusterVersionToBackend(be backend.Backend, ver *semver.Version) {
	// const key
	ckey := backendClusterVersionKey()

	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()
	tx.UnsafePut(clusterBucketName, ckey, []byte(ver.String()))
}

// 写成员to v2
func mustSaveMemberToStore(s v2store.Store, m *Member) {
	// 序列化
	b, err := json.Marshal(m.RaftAttributes)
	if err != nil {
		plog.Panicf("marshal raftAttributes should never fail: %v", err)
	}

	p := path.Join(MemberStoreKey(m.ID), raftAttributesSuffix)
	if _, err := s.Create(p, false, string(b), false, v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		plog.Panicf("create raftAttributes should never fail: %v", err)
	}
}

// 删除成员to v2
func mustDeleteMemberFromStore(s v2store.Store, id types.ID) {
	if _, err := s.Delete(MemberStoreKey(id), true, true); err != nil {
		plog.Panicf("delete member should never fail: %v", err)
	}

	if _, err := s.Create(RemovedMemberStoreKey(id), false, "", false, v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		plog.Panicf("create removedMember should never fail: %v", err)
	}
}

// 更新成员
func mustUpdateMemberInStore(s v2store.Store, m *Member) {
	b, err := json.Marshal(m.RaftAttributes)
	if err != nil {
		plog.Panicf("marshal raftAttributes should never fail: %v", err)
	}

	p := path.Join(MemberStoreKey(m.ID), raftAttributesSuffix)
	if _, err := s.Update(p, string(b), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		plog.Panicf("update raftAttributes should never fail: %v", err)
	}
}

// 更新成员属性
func mustUpdateMemberAttrInStore(s v2store.Store, m *Member) {
	b, err := json.Marshal(m.Attributes)
	if err != nil {
		plog.Panicf("marshal raftAttributes should never fail: %v", err)
	}

	p := path.Join(MemberStoreKey(m.ID), attributesSuffix)
	if _, err := s.Set(p, false, string(b), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		plog.Panicf("update raftAttributes should never fail: %v", err)
	}
}

// 保存集群信息
func mustSaveClusterVersionToStore(s v2store.Store, ver *semver.Version) {
	if _, err := s.Set(StoreClusterVersionKey(), false, ver.String(), v2store.TTLOptionSet{ExpireTime: v2store.Permanent}); err != nil {
		plog.Panicf("save cluster version should never fail: %v", err)
	}
}

// nodeToMember builds member from a key value node.
// the child nodes of the given node MUST be sorted by key.
func nodeToMember(n *v2store.NodeExtern) (*Member, error) {
	// new mumber
	m := &Member{ID: MustParseMemberIDFromKey(n.Key)}

	attrs := make(map[string][]byte)

	raftAttrKey := path.Join(n.Key, raftAttributesSuffix)
	attrKey := path.Join(n.Key, attributesSuffix)

	// 读取所有属性
	for _, nn := range n.Nodes {
		if nn.Key != raftAttrKey && nn.Key != attrKey {
			return nil, fmt.Errorf("unknown key %q", nn.Key)
		}
		attrs[nn.Key] = []byte(*nn.Value)
	}

	// 读取raft 属性
	if data := attrs[raftAttrKey]; data != nil {
		if err := json.Unmarshal(data, &m.RaftAttributes); err != nil {
			return nil, fmt.Errorf("unmarshal raftAttributes error: %v", err)
		}
	} else {
		return nil, fmt.Errorf("raftAttributes key doesn't exist")
	}

	// 读取attr
	if data := attrs[attrKey]; data != nil {
		if err := json.Unmarshal(data, &m.Attributes); err != nil {
			return m, fmt.Errorf("unmarshal attributes error: %v", err)
		}
	}

	return m, nil
}

// id to key
func backendMemberKey(id types.ID) []byte {
	return []byte(id.String())
}

// 集群版本号
func backendClusterVersionKey() []byte {
	return []byte("clusterVersion")
}

// 创建bucket
func mustCreateBackendBuckets(be backend.Backend) {
	tx := be.BatchTx()
	tx.Lock()
	defer tx.Unlock()

	tx.UnsafeCreateBucket(membersBucketName)
	tx.UnsafeCreateBucket(membersRemovedBucketName)
	tx.UnsafeCreateBucket(clusterBucketName)
}

// 成员存储key
func MemberStoreKey(id types.ID) string {
	return path.Join(StoreMembersPrefix, id.String())
}

// 集群版本key
func StoreClusterVersionKey() string {
	return path.Join(storePrefix, "version")
}

// 成员属性路径
func MemberAttributesStorePath(id types.ID) string {
	return path.Join(MemberStoreKey(id), attributesSuffix)
}

// 成员路径to 成员ID
func MustParseMemberIDFromKey(key string) types.ID {
	id, err := types.IDFromString(path.Base(key))
	if err != nil {
		plog.Panicf("unexpected parse member id error: %v", err)
	}
	return id
}

// 删除成员key
func RemovedMemberStoreKey(id types.ID) string {
	return path.Join(storeRemovedMembersPrefix, id.String())
}
