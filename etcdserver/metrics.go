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

package etcdserver

import (
	goruntime "runtime"
	"time"

	"go.etcd.io/etcd/pkg/runtime"
	"go.etcd.io/etcd/version"

	"github.com/prometheus/client_golang/prometheus"
	"go.uber.org/zap"
)

var (
    // 有leader
	hasLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "has_leader",
		Help:      "Whether or not a leader exists. 1 is existence, 0 is not.",
	})

    // 是leader
	isLeader = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "is_leader",
		Help:      "Whether or not this member is a leader. 1 if is, 0 otherwise.",
	})

    // leader 变更
	leaderChanges = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "leader_changes_seen_total",
		Help:      "The number of leader changes seen.",
	})

    // 心跳发送失败数
	heartbeatSendFailures = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "heartbeat_send_failures_total",
		Help:      "The total number of leader heartbeat send failures (likely overloaded from slow disk).",
	})

    // 慢操作数
	slowApplies = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "slow_apply_total",
		Help:      "The total number of slow apply requests (likely overloaded from slow disk).",
	})

    // 提议提交数
	proposalsCommitted = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "proposals_committed_total",
		Help:      "The total number of consensus proposals committed.",
	})

    // 提议申请数
	proposalsApplied = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "proposals_applied_total",
		Help:      "The total number of consensus proposals applied.",
	})

    // 待处理提议数
	proposalsPending = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "proposals_pending",
		Help:      "The current number of pending proposals to commit.",
	})

    // 提议失败数
	proposalsFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "proposals_failed_total",
		Help:      "The total number of failed proposals seen.",
	})

    // 满读索引数
	slowReadIndex = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "slow_read_indexes_total",
		Help:      "The total number of pending read indexes not in sync with leader's or timed out read index requests.",
	})

    // 读索引失败数
	readIndexFailed = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "read_indexes_failed_total",
		Help:      "The total number of failed read indexes seen.",
	})

    // 租约超时
	leaseExpired = prometheus.NewCounter(prometheus.CounterOpts{
		Namespace: "etcd_debugging",
		Subsystem: "server",
		Name:      "lease_expired_total",
		Help:      "The total number of expired leases.",
	})

    // 配额字节数
	quotaBackendBytes = prometheus.NewGauge(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "quota_backend_bytes",
		Help:      "Current backend storage quota size in bytes.",
	})

    // 当前版本
	currentVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "version",
		Help:      "Which version is running. 1 for 'server_version' label with current version.",
	},
		[]string{"server_version"})
	currentGoVersion = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "go_version",
		Help:      "Which Go version server is running with. 1 for 'server_go_version' label with current version.",
	},
		[]string{"server_go_version"})

    // 服务ID
	serverID = prometheus.NewGaugeVec(prometheus.GaugeOpts{
		Namespace: "etcd",
		Subsystem: "server",
		Name:      "id",
		Help:      "Server or member ID in hexadecimal format. 1 for 'server_id' label with current ID.",
	},
		[]string{"server_id"})
)

func init() {
	prometheus.MustRegister(hasLeader)
	prometheus.MustRegister(isLeader)
	prometheus.MustRegister(leaderChanges)
	prometheus.MustRegister(heartbeatSendFailures)
	prometheus.MustRegister(slowApplies)
	prometheus.MustRegister(proposalsCommitted)
	prometheus.MustRegister(proposalsApplied)
	prometheus.MustRegister(proposalsPending)
	prometheus.MustRegister(proposalsFailed)
	prometheus.MustRegister(slowReadIndex)
	prometheus.MustRegister(readIndexFailed)
	prometheus.MustRegister(leaseExpired)
	prometheus.MustRegister(quotaBackendBytes)
	prometheus.MustRegister(currentVersion)
	prometheus.MustRegister(currentGoVersion)
	prometheus.MustRegister(serverID)

	currentVersion.With(prometheus.Labels{
		"server_version": version.Version,
	}).Set(1)
	currentGoVersion.With(prometheus.Labels{
		"server_go_version": goruntime.Version(),
	}).Set(1)
}

// 监控文件描述符
func monitorFileDescriptor(lg *zap.Logger, done <-chan struct{}) {
	ticker := time.NewTicker(5 * time.Second)
	defer ticker.Stop()

	for {
	    // 查fd 使用情况
		used, err := runtime.FDUsage()
		if err != nil {
			if lg != nil {
				lg.Warn("failed to get file descriptor usage", zap.Error(err))
			} else {
				plog.Errorf("cannot monitor file descriptor usage (%v)", err)
			}
			return
		}

        // 查fd 限制
		limit, err := runtime.FDLimit()
		if err != nil {
			if lg != nil {
				lg.Warn("failed to get file descriptor limit", zap.Error(err))
			} else {
				plog.Errorf("cannot monitor file descriptor usage (%v)", err)
			}
			return
		}

        // 超过80%，告警
		if used >= limit/5*4 {
			if lg != nil {
				lg.Warn("80%% of file descriptors are used", zap.Uint64("used", used), zap.Uint64("limit", limit))
			} else {
				plog.Warningf("80%% of the file descriptor limit is used [used = %d, limit = %d]", used, limit)
			}
		}

        // 等待定时器或结束
		select {
		case <-ticker.C:
		case <-done:
			return
		}
	}
}
