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

package etcdserver

import (
	"encoding/json"
	"path"
	"time"

	"go.etcd.io/etcd/etcdserver/api"
	"go.etcd.io/etcd/etcdserver/api/membership"
	"go.etcd.io/etcd/etcdserver/api/v2store"
	"go.etcd.io/etcd/pkg/pbutil"

	"github.com/coreos/go-semver/semver"
	"go.uber.org/zap"
)

// ApplierV2 is the interface for processing V2 raft messages
// 申请者接口
type ApplierV2 interface {
	Delete(r *RequestV2) Response
	Post(r *RequestV2) Response
	Put(r *RequestV2) Response
	QGet(r *RequestV2) Response
	Sync(r *RequestV2) Response
}

func NewApplierV2(lg *zap.Logger, s v2store.Store, c *membership.RaftCluster) ApplierV2 {
	return &applierV2store{lg: lg, store: s, cluster: c}
}

type applierV2store struct {
	lg      *zap.Logger // 日志

	store   v2store.Store // 存储

	cluster *membership.RaftCluster // 集群
}

func (a *applierV2store) Delete(r *RequestV2) Response {
	switch {
	case r.PrevIndex > 0 || r.PrevValue != "":
	    // cas 删除
		return toResponse(a.store.CompareAndDelete(r.Path, r.PrevValue, r.PrevIndex))
	default:
	    // 默认删除指定路径
		return toResponse(a.store.Delete(r.Path, r.Dir, r.Recursive))
	}
}

func (a *applierV2store) Post(r *RequestV2) Response {
    // 创建
	return toResponse(a.store.Create(r.Path, r.Dir, r.Val, true, r.TTLOptions()))
}

func (a *applierV2store) Put(r *RequestV2) Response {
	ttlOptions := r.TTLOptions()

	exists, existsSet := pbutil.GetBool(r.PrevExist)
	switch {
	case existsSet:
	    // 确认有老值
		if exists {
			if r.PrevIndex == 0 && r.PrevValue == "" {
			    // 覆盖写
				return toResponse(a.store.Update(r.Path, r.Val, ttlOptions))
			}
			// 比较写
			return toResponse(a.store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, ttlOptions))
		}
		return toResponse(a.store.Create(r.Path, r.Dir, r.Val, false, ttlOptions))
	case r.PrevIndex > 0 || r.PrevValue != "":
	    // 比较写
		return toResponse(a.store.CompareAndSwap(r.Path, r.PrevValue, r.PrevIndex, r.Val, ttlOptions))
	default:
	    // 默认处理
		if storeMemberAttributeRegexp.MatchString(r.Path) {
		    // 设置成员属性

            // 解析id，反序列化值
			id := membership.MustParseMemberIDFromKey(path.Dir(r.Path))
			var attr membership.Attributes
			if err := json.Unmarshal([]byte(r.Val), &attr); err != nil {
				if a.lg != nil {
					a.lg.Panic("failed to unmarshal", zap.String("value", r.Val), zap.Error(err))
				} else {
					plog.Panicf("unmarshal %s should never fail: %v", r.Val, err)
				}
			}
			// 更新集群成员属性
			if a.cluster != nil {
				a.cluster.UpdateAttributes(id, attr)
			}
			// return an empty response since there is no consumer.
			return Response{}
		}

		if r.Path == membership.StoreClusterVersionKey() {
		    // 设置集群版本
			if a.cluster != nil {
				a.cluster.SetVersion(semver.Must(semver.NewVersion(r.Val)), api.UpdateCapability)
			}
			// return an empty response since there is no consumer.
			return Response{}
		}

		// 设置操作
		return toResponse(a.store.Set(r.Path, r.Dir, r.Val, ttlOptions))
	}
}

func (a *applierV2store) QGet(r *RequestV2) Response {
    // 获取
	return toResponse(a.store.Get(r.Path, r.Recursive, r.Sorted))
}

func (a *applierV2store) Sync(r *RequestV2) Response {
    // 删除过期数据
	a.store.DeleteExpiredKeys(time.Unix(0, r.Time))
	return Response{}
}

// applyV2Request interprets r as a call to v2store.X
// and returns a Response interpreted from v2store.Event
func (s *EtcdServer) applyV2Request(r *RequestV2) Response {
	defer warnOfExpensiveRequest(s.getLogger(), time.Now(), r, nil, nil)

    // 分配任务
	switch r.Method {
	case "POST":
		return s.applyV2.Post(r)
	case "PUT":
		return s.applyV2.Put(r)
	case "DELETE":
		return s.applyV2.Delete(r)
	case "QGET":
		return s.applyV2.QGet(r)
	case "SYNC":
		return s.applyV2.Sync(r)
	default:
		// This should never be reached, but just in case:
		return Response{Err: ErrUnknownMethod}
	}
}

// ttl 选项
func (r *RequestV2) TTLOptions() v2store.TTLOptionSet {
	refresh, _ := pbutil.GetBool(r.Refresh)
	ttlOptions := v2store.TTLOptionSet{Refresh: refresh}
	if r.Expiration != 0 {
		ttlOptions.ExpireTime = time.Unix(0, r.Expiration)
	}
	return ttlOptions
}

func toResponse(ev *v2store.Event, err error) Response {
	return Response{Event: ev, Err: err}
}
