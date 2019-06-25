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

package snap

import (
	"errors"
	"fmt"
	"hash/crc32"
	"io/ioutil"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"go.etcd.io/etcd/etcdserver/api/snap/snappb"
	pioutil "go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/pkg/pbutil"
	"go.etcd.io/etcd/raft"
	"go.etcd.io/etcd/raft/raftpb"

	"github.com/coreos/pkg/capnslog"
	"go.uber.org/zap"
)

const snapSuffix = ".snap"

var (
	plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "snap")

    // 错误
	ErrNoSnapshot    = errors.New("snap: no available snapshot")
	ErrEmptySnapshot = errors.New("snap: empty snapshot")
	ErrCRCMismatch   = errors.New("snap: crc mismatch")

	crcTable         = crc32.MakeTable(crc32.Castagnoli)

	// A map of valid files that can be present in the snap folder.
	validFiles = map[string]bool{
		"db": true,
	}
)

type Snapshotter struct {
	lg  *zap.Logger // 日志
	dir string // 目录
}

func New(lg *zap.Logger, dir string) *Snapshotter {
	return &Snapshotter{
		lg:  lg,
		dir: dir,
	}
}

// 判空后保存
func (s *Snapshotter) SaveSnap(snapshot raftpb.Snapshot) error {
	if raft.IsEmptySnap(snapshot) {
		return nil
	}

	return s.save(&snapshot)
}

func (s *Snapshotter) save(snapshot *raftpb.Snapshot) error {
	start := time.Now()

    // 拼文件名
	fname := fmt.Sprintf("%016x-%016x%s", snapshot.Metadata.Term, snapshot.Metadata.Index, snapSuffix)

    // 数据序列化，crc，快照序列号
	b := pbutil.MustMarshal(snapshot)
	crc := crc32.Update(0, crcTable, b)
	snap := snappb.Snapshot{Crc: crc, Data: b}
	d, err := snap.Marshal()
	if err != nil {
		return err
	}
	// 监控耗时
	snapMarshallingSec.Observe(time.Since(start).Seconds())

	spath := filepath.Join(s.dir, fname)

    // 写文件，并监控耗时
	fsyncStart := time.Now()
	err = pioutil.WriteAndSyncFile(spath, d, 0666)
	snapFsyncSec.Observe(time.Since(fsyncStart).Seconds())

    // 错误日志
	if err != nil {
		if s.lg != nil {
			s.lg.Warn("failed to write a snap file", zap.String("path", spath), zap.Error(err))
		}
		rerr := os.Remove(spath)
		if rerr != nil {
			if s.lg != nil {
				s.lg.Warn("failed to remove a broken snap file", zap.String("path", spath), zap.Error(err))
			} else {
				plog.Errorf("failed to remove broken snapshot file %s", spath)
			}
		}
		return err
	}

    // 监控耗时
	snapSaveSec.Observe(time.Since(start).Seconds())
	return nil
}

func (s *Snapshotter) Load() (*raftpb.Snapshot, error) {
    // 找快照名称列表
	names, err := s.snapNames()
	if err != nil {
		return nil, err
	}

    // 从快照价值数据
	var snap *raftpb.Snapshot
	for _, name := range names {
		if snap, err = loadSnap(s.lg, s.dir, name); err == nil {
			break
		}
	}

    // 失败或成功
	if err != nil {
		return nil, ErrNoSnapshot
	}
	return snap, nil
}

func loadSnap(lg *zap.Logger, dir, name string) (*raftpb.Snapshot, error) {
	// 拼路径
	fpath := filepath.Join(dir, name)

	// 读取数据
	snap, err := Read(lg, fpath)

	if err != nil {
	    // 读取失败，将文件移到破坏路径
		brokenPath := fpath + ".broken"
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", fpath), zap.Error(err))
		}
		if rerr := os.Rename(fpath, brokenPath); rerr != nil {
			if lg != nil {
				lg.Warn("failed to rename a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath), zap.Error(rerr))
			} else {
				plog.Warningf("cannot rename broken snapshot file %v to %v: %v", fpath, brokenPath, rerr)
			}
		} else {
			if lg != nil {
				lg.Warn("renamed to a broken snap file", zap.String("path", fpath), zap.String("broken-path", brokenPath))
			}
		}
	}

	return snap, err
}

// Read reads the snapshot named by snapname and returns the snapshot.
func Read(lg *zap.Logger, snapname string) (*raftpb.Snapshot, error) {
    // 读取数据
	b, err := ioutil.ReadFile(snapname)
	if err != nil {
		if lg != nil {
			lg.Warn("failed to read a snap file", zap.String("path", snapname), zap.Error(err))
		} else {
			plog.Errorf("cannot read file %v: %v", snapname, err)
		}
		return nil, err
	}

    // 数据为空
	if len(b) == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot file", zap.String("path", snapname))
		} else {
			plog.Errorf("unexpected empty snapshot")
		}
		return nil, ErrEmptySnapshot
	}

    // 反序列化快照
	var serializedSnap snappb.Snapshot
	if err = serializedSnap.Unmarshal(b); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal snappb.Snapshot", zap.String("path", snapname), zap.Error(err))
		} else {
			plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		}
		return nil, err
	}

    // 校验数据 和 crc
	if len(serializedSnap.Data) == 0 || serializedSnap.Crc == 0 {
		if lg != nil {
			lg.Warn("failed to read empty snapshot data", zap.String("path", snapname))
		} else {
			plog.Errorf("unexpected empty snapshot")
		}
		return nil, ErrEmptySnapshot
	}

    // 验证crc
	crc := crc32.Update(0, crcTable, serializedSnap.Data)
	if crc != serializedSnap.Crc {
		if lg != nil {
			lg.Warn("snap file is corrupt",
				zap.String("path", snapname),
				zap.Uint32("prev-crc", serializedSnap.Crc),
				zap.Uint32("new-crc", crc),
			)
		} else {
			plog.Errorf("corrupted snapshot file %v: crc mismatch", snapname)
		}
		return nil, ErrCRCMismatch
	}

    // 反序列化数据
	var snap raftpb.Snapshot
	if err = snap.Unmarshal(serializedSnap.Data); err != nil {
		if lg != nil {
			lg.Warn("failed to unmarshal raftpb.Snapshot", zap.String("path", snapname), zap.Error(err))
		} else {
			plog.Errorf("corrupted snapshot file %v: %v", snapname, err)
		}
		return nil, err
	}

	return &snap, nil
}

// snapNames returns the filename of the snapshots in logical time order (from newest to oldest).
// If there is no available snapshots, an ErrNoSnapshot will be returned.
func (s *Snapshotter) snapNames() ([]string, error) {
    // 打开目录
	dir, err := os.Open(s.dir)
	if err != nil {
		return nil, err
	}
	defer dir.Close()

    // 读取所以文件列表
	names, err := dir.Readdirnames(-1)
	if err != nil {
		return nil, err
	}

    // 校验文件名后缀
	snaps := checkSuffix(s.lg, names)
	if len(snaps) == 0 {
		return nil, ErrNoSnapshot
	}

    // 逆序排序
	sort.Sort(sort.Reverse(sort.StringSlice(snaps)))
	return snaps, nil
}

func checkSuffix(lg *zap.Logger, names []string) []string {
	snaps := []string{}
	for i := range names {
		if strings.HasSuffix(names[i], snapSuffix) {
		    // 有指定后缀
			snaps = append(snaps, names[i])
		} else {
			// If we find a file which is not a snapshot then check if it's
			// a vaild file. If not throw out a warning.
			if _, ok := validFiles[names[i]]; !ok {
			    // 非有效名称，告警
				if lg != nil {
					lg.Warn("found unexpected non-snap file; skipping", zap.String("path", names[i]))
				} else {
					plog.Warningf("skipped unexpected non snapshot file %v", names[i])
				}
			}
		}
	}
	return snaps
}
