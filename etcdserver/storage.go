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
	"io"

	"go.etcd.io/etcd/etcdserver/api/snap"
	pb "go.etcd.io/etcd/etcdserver/etcdserverpb"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/pkg/types"
	"go.etcd.io/etcd/raft/raftpb"
	"go.etcd.io/etcd/wal"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// 存储接口
type Storage interface {
	// Save function saves ents and state to the underlying stable storage.
	// Save MUST block until st and ents are on stable storage.
	// 保存
	Save(st raftpb.HardState, ents []raftpb.Entry) error

	// SaveSnap function saves snapshot to the underlying stable storage.
	// 保存快照
	SaveSnap(snap raftpb.Snapshot) error

	// Close closes the Storage and performs finalization.
	// 关闭
	Close() error
}

type storage struct {
	*wal.WAL // 日志

	*snap.Snapshotter // 快照
}

// 新建
func NewStorage(w *wal.WAL, s *snap.Snapshotter) Storage {
	return &storage{w, s}
}

// SaveSnap saves the snapshot to disk and release the locked
// wal files since they will not be used.
func (st *storage) SaveSnap(snap raftpb.Snapshot) error {
    // 构建 wal日志项
	walsnap := walpb.Snapshot{
		Index: snap.Metadata.Index,
		Term:  snap.Metadata.Term,
	}

    // 写wal 日志
	err := st.WAL.SaveSnapshot(walsnap)
	if err != nil {
		return err
	}

    // 写快照
	err = st.Snapshotter.SaveSnap(snap)
	if err != nil {
		return err
	}

    // wal 解锁
	return st.WAL.ReleaseLockTo(snap.Metadata.Index)
}

func readWAL(lg *zap.Logger, waldir string, snap walpb.Snapshot) (w *wal.WAL, id, cid types.ID, st raftpb.HardState, ents []raftpb.Entry) {
	var (
		err       error
		wmetadata []byte
	)

	repaired := false
	for {
	    // 打开wal
		if w, err = wal.Open(lg, waldir, snap); err != nil {
			if lg != nil {
				lg.Fatal("failed to open WAL", zap.Error(err))
			} else {
				plog.Fatalf("open wal error: %v", err)
			}
		}

        // 读取元信息
		if wmetadata, st, ents, err = w.ReadAll(); err != nil {
		    // 读取失败
			w.Close()

			// we can only repair ErrUnexpectedEOF and we never repair twice.
			if repaired || err != io.ErrUnexpectedEOF {
			    // 修复过了，而且不能修复
				if lg != nil {
					lg.Fatal("failed to read WAL, cannot be repaired", zap.Error(err))
				} else {
					plog.Fatalf("read wal error (%v) and cannot be repaired", err)
				}
			}
	
	        // 修复
			if !wal.Repair(lg, waldir) {
				if lg != nil {
					lg.Fatal("failed to repair WAL", zap.Error(err))
				} else {
					plog.Fatalf("WAL error (%v) cannot be repaired", err)
				}
			} else {
				if lg != nil {
					lg.Info("repaired WAL", zap.Error(err))
				} else {
					plog.Infof("repaired WAL error (%v)", err)
				}
				repaired = true
			}
	
	        // 修复成功后，继续
			continue
		}

        // 结束
		break
	}

    // 反序列化数据
	var metadata pb.Metadata
	pbutil.MustUnmarshal(&metadata, wmetadata)

    // 结构转换
	id = types.ID(metadata.NodeID)
	cid = types.ID(metadata.ClusterID)
	return w, id, cid, st, ents
}
