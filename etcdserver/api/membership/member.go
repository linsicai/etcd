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

package membership

import (
	"crypto/sha1"
	"encoding/binary"
	"fmt"
	"math/rand"
	"sort"
	"time"

	"github.com/coreos/pkg/capnslog"
	"go.etcd.io/etcd/pkg/types"
)

var (
	// 日志
	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "etcdserver/membership")
)

// RaftAttributes represents the raft related attributes of an etcd member.
// raft 属性
type RaftAttributes struct {
	// PeerURLs is the list of peers in the raft cluster.
	// TODO(philips): ensure these are URLs
	// 对侧URL 列表
	PeerURLs []string `json:"peerURLs"`
}

// Attributes represents all the non-raft related attributes of an etcd member.
// 普通属性
type Attributes struct {
	// 名称
	Name string `json:"name,omitempty"`
	// 客户端URL 列表
	ClientURLs []string `json:"clientURLs,omitempty"`
}

// 成功
type Member struct {
	// ID
	ID types.ID `json:"id"`

	// raft 属性
	RaftAttributes

	// 普通属性
	Attributes
}

// NewMember creates a Member without an ID and generates one based on the
// cluster name, peer URLs, and time. This is used for bootstrapping/adding new member.
func NewMember(name string, peerURLs types.URLs, clusterName string, now *time.Time) *Member {
	// new 成员
	m := &Member{
		RaftAttributes: RaftAttributes{PeerURLs: peerURLs.StringSlice()},
		Attributes:     Attributes{Name: name},
	}

	// 对侧URL 列表
	var b []byte
	sort.Strings(m.PeerURLs)
	for _, p := range m.PeerURLs {
		b = append(b, []byte(p)...)
	}

	// 集群名称
	b = append(b, []byte(clusterName)...)

	// 当前时间
	if now != nil {
		b = append(b, []byte(fmt.Sprintf("%d", now.Unix()))...)
	}

	// 计算ID
	hash := sha1.Sum(b)
	m.ID = types.ID(binary.BigEndian.Uint64(hash[:8]))
	return m
}

// PickPeerURL chooses a random address from a given Member's PeerURLs.
// It will panic if there is no PeerURLs available in Member.
func (m *Member) PickPeerURL() string {
	if len(m.PeerURLs) == 0 {
		// 异常
		panic("member should always have some peer url")
	}

	// 随机选择一个
	return m.PeerURLs[rand.Intn(len(m.PeerURLs))]
}

func (m *Member) Clone() *Member {
	if m == nil {
		// 空成员
		return nil
	}

	// new 成员
	mm := &Member{
		ID: m.ID,
		Attributes: Attributes{
			Name: m.Name,
		},
	}

	// 深度复制
	if m.PeerURLs != nil {
		mm.PeerURLs = make([]string, len(m.PeerURLs))
		copy(mm.PeerURLs, m.PeerURLs)
	}
	if m.ClientURLs != nil {
		mm.ClientURLs = make([]string, len(m.ClientURLs))
		copy(mm.ClientURLs, m.ClientURLs)
	}
	return mm
}

// 是否开始
func (m *Member) IsStarted() bool {
	return len(m.Name) != 0
}

// MembersByID implements sort by ID interface
// 按ID 排序
type MembersByID []*Member

func (ms MembersByID) Len() int           { return len(ms) }
func (ms MembersByID) Less(i, j int) bool { return ms[i].ID < ms[j].ID }
func (ms MembersByID) Swap(i, j int)      { ms[i], ms[j] = ms[j], ms[i] }

// MembersByPeerURLs implements sort by peer urls interface
// 按第一个对侧URL 排序
type MembersByPeerURLs []*Member

func (ms MembersByPeerURLs) Len() int {
	return len(ms)
}
func (ms MembersByPeerURLs) Less(i, j int) bool {
	return ms[i].PeerURLs[0] < ms[j].PeerURLs[0]
}
func (ms MembersByPeerURLs) Swap(i, j int) { ms[i], ms[j] = ms[j], ms[i] }
