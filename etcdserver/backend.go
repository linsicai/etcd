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

package etcdserver

import (
	"fmt"
	"os"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap"
	"go.etcd.io/etcd/lease"
	"go.etcd.io/etcd/mvcc"
	"go.etcd.io/etcd/mvcc/backend"
	"go.etcd.io/etcd/raft/raftpb"

	"go.uber.org/zap"
)

// 创建后端
func newBackend(cfg ServerConfig) backend.Backend {
    // 取默认后端配置
	bcfg := backend.DefaultBackendConfig()

    // 设置后端配置路径
	bcfg.Path = cfg.backendPath()

    // 更新配置
	if cfg.BackendBatchLimit != 0 {
		bcfg.BatchLimit = cfg.BackendBatchLimit
		if cfg.Logger != nil {
			cfg.Logger.Info("setting backend batch limit", zap.Int("batch limit", cfg.BackendBatchLimit))
		}
	}
	if cfg.BackendBatchInterval != 0 {
		bcfg.BatchInterval = cfg.BackendBatchInterval
		if cfg.Logger != nil {
			cfg.Logger.Info("setting backend batch interval", zap.Duration("batch interval", cfg.BackendBatchInterval))
		}
	}

    // 设置后端日志
	bcfg.Logger = cfg.Logger

    // 更新quota 配置
	if cfg.QuotaBackendBytes > 0 && cfg.QuotaBackendBytes != DefaultQuotaBytes {
		// permit 10% excess over quota for disarm
		bcfg.MmapSize = uint64(cfg.QuotaBackendBytes + cfg.QuotaBackendBytes/10)
	}

    // 构建后端
	return backend.New(bcfg)
}

// openSnapshotBackend renames a snapshot db to the current etcd db and opens it.
// 打开快照后端
func openSnapshotBackend(cfg ServerConfig, ss *snap.Snapshotter, snapshot raftpb.Snapshot) (backend.Backend, error) {
    // 找快照路径
	snapPath, err := ss.DBFilePath(snapshot.Metadata.Index)
	if err != nil {
		return nil, fmt.Errorf("failed to find database snapshot file (%v)", err)
	}

    // 快照转正
	if err := os.Rename(snapPath, cfg.backendPath()); err != nil {
		return nil, fmt.Errorf("failed to rename database snapshot file (%v)", err)
	}

    // 打开后端
	return openBackend(cfg), nil
}

// openBackend returns a backend using the current etcd db.
func openBackend(cfg ServerConfig) backend.Backend {
    // 后端路径
	fn := cfg.backendPath()

	now, beOpened := time.Now(), make(chan backend.Backend)
	go func() {
	    // 异步创建后端
		beOpened <- newBackend(cfg)
	}()

	select {
	case be := <-beOpened:
	    // 打开成功
		if cfg.Logger != nil {
			cfg.Logger.Info("opened backend db", zap.String("path", fn), zap.Duration("took", time.Since(now)))
		}
		return be

	case <-time.After(10 * time.Second):
	    // 每10秒，输出信息
		if cfg.Logger != nil {
			cfg.Logger.Info(
				"db file is flocked by another process, or taking too long",
				zap.String("path", fn),
				zap.Duration("took", time.Since(now)),
			)
		} else {
			plog.Warningf("another etcd process is using %q and holds the file lock, or loading backend file is taking >10 seconds", fn)
			plog.Warningf("waiting for it to exit before starting...")
		}
	}

	return <-beOpened
}

// recoverBackendSnapshot recovers the DB from a snapshot in case etcd crashes
// before updating the backend db after persisting raft snapshot to disk,
// violating the invariant snapshot.Metadata.Index < db.consistentIndex. In this
// case, replace the db with the snapshot db sent by the leader.
func recoverSnapshotBackend(cfg ServerConfig, oldbe backend.Backend, snapshot raftpb.Snapshot) (backend.Backend, error) {
	var cIndex consistentIndex

    // 打开db
	kv := mvcc.New(cfg.Logger, oldbe, &lease.FakeLessor{}, &cIndex)
	defer kv.Close()

    // 校验快照版本
	if snapshot.Metadata.Index <= kv.ConsistentIndex() {
		return oldbe, nil
	}

    // 关闭后端，重新打开
	oldbe.Close()
	return openSnapshotBackend(cfg, snap.New(cfg.Logger, cfg.SnapDir()), snapshot)
}
