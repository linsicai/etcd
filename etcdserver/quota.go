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
	"sync"

	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"

	humanize "github.com/dustin/go-humanize"
	"go.uber.org/zap"
)

const (
	// DefaultQuotaBytes is the number of bytes the backend Size may
	// consume before exceeding the space quota.
	// 默认配额
	DefaultQuotaBytes = int64(2 * 1024 * 1024 * 1024) // 2GB

	// MaxQuotaBytes is the maximum number of bytes suggested for a backend
	// quota. A larger quota may lead to degraded performance.
	MaxQuotaBytes = int64(8 * 1024 * 1024 * 1024) // 8GB
)

// Quota represents an arbitrary quota against arbitrary requests. Each request
// costs some charge; if there is not enough remaining charge, then there are
// too few resources available within the quota to apply the request.
type Quota interface {
	// Available judges whether the given request fits within the quota.
	// 是否可用
	Available(req interface{}) bool

	// Cost computes the charge against the quota for a given request.
	Cost(req interface{}) int

	// Remaining is the amount of charge left for the quota.
	Remaining() int64
}

// 空配额
type passthroughQuota struct{}
func (*passthroughQuota) Available(interface{}) bool { return true }
func (*passthroughQuota) Cost(interface{}) int       { return 0 }
func (*passthroughQuota) Remaining() int64           { return 1 }

type backendQuota struct {
	s               *EtcdServer // 服务
	maxBackendBytes int64 // 最大后端字节
}

// 评估使用，额外大小
const (
	// leaseOverhead is an estimate for the cost of storing a lease
	leaseOverhead = 64

	// kvOverhead is an estimate for the cost of storing a key's metadata
	kvOverhead = 256
)

var (
	// only log once
	// 一次性
	quotaLogOnce sync.Once

	DefaultQuotaSize = humanize.Bytes(uint64(DefaultQuotaBytes))
	maxQuotaSize     = humanize.Bytes(uint64(MaxQuotaBytes))
)

// NewBackendQuota creates a quota layer with the given storage limit.
func NewBackendQuota(s *EtcdServer, name string) Quota {
	lg := s.getLogger()

    // 设置默认配额大小
	quotaBackendBytes.Set(float64(s.Cfg.QuotaBackendBytes))

	if s.Cfg.QuotaBackendBytes < 0 {
		// disable quotas if negative
		// 关闭quota 管理
		quotaLogOnce.Do(func() {
			if lg != nil {
				lg.Info(
					"disabled backend quota",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
				)
			} else {
				plog.Warningf("disabling backend quota")
			}
		})
		return &passthroughQuota{}
	}

	if s.Cfg.QuotaBackendBytes == 0 {
		// use default size if no quota size given
		// 使用默认配额
		quotaLogOnce.Do(func() {
			if lg != nil {
				lg.Info(
					"enabled backend quota with default value",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", DefaultQuotaBytes),
					zap.String("quota-size", DefaultQuotaSize),
				)
			}
		})
		quotaBackendBytes.Set(float64(DefaultQuotaBytes))
		return &backendQuota{s, DefaultQuotaBytes}
	}

	quotaLogOnce.Do(func() {
	    // 配额信息打印
		if s.Cfg.QuotaBackendBytes > MaxQuotaBytes {
			if lg != nil {
				lg.Warn(
					"quota exceeds the maximum value",
					zap.String("quota-name", name),
					zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
					zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
					zap.Int64("quota-maximum-size-bytes", MaxQuotaBytes),
					zap.String("quota-maximum-size", maxQuotaSize),
				)
			} else {
				plog.Warningf("backend quota %v exceeds maximum recommended quota %v", s.Cfg.QuotaBackendBytes, MaxQuotaBytes)
			}
		}
		if lg != nil {
			lg.Info(
				"enabled backend quota",
				zap.String("quota-name", name),
				zap.Int64("quota-size-bytes", s.Cfg.QuotaBackendBytes),
				zap.String("quota-size", humanize.Bytes(uint64(s.Cfg.QuotaBackendBytes))),
			)
		}
	})
	return &backendQuota{s, s.Cfg.QuotaBackendBytes}
}

// 判断是否可用，已用 + 数据大小 < 最大大小即可
func (b *backendQuota) Available(v interface{}) bool {
	// TODO: maybe optimize backend.Size()
	return b.s.Backend().Size()+int64(b.Cost(v)) < b.maxBackendBytes
}

// 通用计算
func (b *backendQuota) Cost(v interface{}) int {
	switch r := v.(type) {
	case *pb.PutRequest: // put 请求
		return costPut(r)
	case *pb.TxnRequest: // 事务请求
		return costTxn(r)
	case *pb.LeaseGrantRequest: // 租约请求
		return leaseOverhead
	default:
	    // 默认报错
		panic("unexpected cost")
	}
}

// 额外 + k大小 + v大小
func costPut(r *pb.PutRequest) int {
    return kvOverhead + len(r.Key) + len(r.Value)
}

func costTxnReq(u *pb.RequestOp) int {
	r := u.GetRequestPut()
	if r == nil {
	    // 不是put，大小为0
		return 0
	}

	return costPut(r)
}

// 计算失误需要大小
func costTxn(r *pb.TxnRequest) int {
	sizeSuccess := 0
	for _, u := range r.Success {
		sizeSuccess += costTxnReq(u)
	}

	sizeFailure := 0
	for _, u := range r.Failure {
		sizeFailure += costTxnReq(u)
	}

	if sizeFailure > sizeSuccess {
		return sizeFailure
	}
	return sizeSuccess
}

// 计算剩余配额：最大 - 已使用
func (b *backendQuota) Remaining() int64 {
	return b.maxBackendBytes - b.s.Backend().Size()
}
