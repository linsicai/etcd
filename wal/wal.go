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

package wal

import (
    "bytes"
    "errors"
    "fmt"
    "hash/crc32"
    "io"
    "os"
    "path/filepath"
    "sync"
    "time"

    "go.etcd.io/etcd/pkg/fileutil"
    "go.etcd.io/etcd/pkg/pbutil"
    "go.etcd.io/etcd/raft"
    "go.etcd.io/etcd/raft/raftpb"
    "go.etcd.io/etcd/wal/walpb"

    "github.com/coreos/pkg/capnslog"
    "go.uber.org/zap"
)

const (
    // 元信息
    metadataType int64 = iota + 1

    // 实体类型
    entryType

    // 状态类型
    stateType

    // crc 类型
    crcType

    // 快照类型
    snapshotType

    // warnSyncDuration is the amount of time allotted to an fsync before
    // logging a warning
    warnSyncDuration = time.Second
)

var (
    // SegmentSizeBytes is the preallocated size of each wal segment file.
    // The actual size might be larger than this. In general, the default
    // value should be used, but this is defined as an exported variable
    // so that tests can set a different segment size.
    SegmentSizeBytes int64 = 64 * 1000 * 1000 // 64MB

    plog = capnslog.NewPackageLogger("go.etcd.io/etcd", "wal")

    // 错误
    ErrMetadataConflict = errors.New("wal: conflicting metadata found")
    ErrFileNotFound     = errors.New("wal: file not found")
    ErrCRCMismatch      = errors.New("wal: crc mismatch")
    ErrSnapshotMismatch = errors.New("wal: snapshot mismatch")
    ErrSnapshotNotFound = errors.New("wal: snapshot not found")

    crcTable            = crc32.MakeTable(crc32.Castagnoli)
)

// WAL is a logical representation of the stable storage.
// WAL is either in read mode or append mode but not both.
// A newly created WAL is in append mode, and ready for appending records.
// A just opened WAL is in read mode, and ready for reading records.
// The WAL will be ready for appending after reading out all the previous records.
type WAL struct {
    // 日志
    lg *zap.Logger

    // 目录
    dir string // the living directory of the underlay files

    // dirFile is a fd for the wal directory for syncing on Rename
    // 目录句柄
    dirFile *os.File

    // 元信息
    metadata []byte           // metadata recorded at the head of each WAL

    // 硬件信息？
    state    raftpb.HardState // hardstate recorded at the head of WAL

    // 快照
    start     walpb.Snapshot // snapshot to start reading

    // 解析器
    decoder   *decoder       // decoder to decode records

    // 解析器关闭函数
    readClose func() error   // closer for decode reader

    // 锁
    mu      sync.Mutex

    // 索引
    enti    uint64   // index of the last entry saved to the wal

    // 编码器
    encoder *encoder // encoder to encode records

    // 文件锁
    locks []*fileutil.LockedFile // the locked files the WAL holds (the name is increasing)

    // 文件产生管道
    fp    *filePipeline
}

// Create creates a WAL ready for appending records. The given metadata is
// recorded at the head of each WAL file, and can be retrieved with ReadAll.
func Create(lg *zap.Logger, dirpath string, metadata []byte) (*WAL, error) {
    // 校验路径
    if Exist(dirpath) {
        return nil, os.ErrExist
    }

    // keep temporary wal directory so WAL initialization appears atomic
    // 清除临时文件
    tmpdirpath := filepath.Clean(dirpath) + ".tmp"
    if fileutil.Exist(tmpdirpath) {
        if err := os.RemoveAll(tmpdirpath); err != nil {
            return nil, err
        }
    }

    // 创建临时文件
    if err := fileutil.CreateDirAll(tmpdirpath); err != nil {
        if lg != nil {
            lg.Warn(
                "failed to create a temporary WAL directory",
                zap.String("tmp-dir-path", tmpdirpath),
                zap.String("dir-path", dirpath),
                zap.Error(err),
            )
        }
        return nil, err
    }

    // 创建0-0文件
    p := filepath.Join(tmpdirpath, walName(0, 0))
    f, err := fileutil.LockFile(p, os.O_WRONLY|os.O_CREATE, fileutil.PrivateFileMode)
    if err != nil {
        if lg != nil {
            lg.Warn(
                "failed to flock an initial WAL file",
                zap.String("path", p),
                zap.Error(err),
            )
        }
        return nil, err
    }

    // 定位到文件尾
    if _, err = f.Seek(0, io.SeekEnd); err != nil {
        if lg != nil {
            lg.Warn(
                "failed to seek an initial WAL file",
                zap.String("path", p),
                zap.Error(err),
            )
        }
        return nil, err
    }

    // alloc
    if err = fileutil.Preallocate(f.File, SegmentSizeBytes, true); err != nil {
        if lg != nil {
            lg.Warn(
                "failed to preallocate an initial WAL file",
                zap.String("path", p),
                zap.Int64("segment-bytes", SegmentSizeBytes),
                zap.Error(err),
            )
        }
        return nil, err
    }

    // 得到一个wal
    w := &WAL{
        lg:       lg,
        dir:      dirpath,
        metadata: metadata,
    }
    w.encoder, err = newFileEncoder(f.File, 0)
    if err != nil {
        return nil, err
    }
    w.locks = append(w.locks, f)
    if err = w.saveCrc(0); err != nil {
        return nil, err
    }
    if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: metadata}); err != nil {
        return nil, err
    }

    // 保存快照
    if err = w.SaveSnapshot(walpb.Snapshot{}); err != nil {
        return nil, err
    }

    // tmp 转正
    if w, err = w.renameWAL(tmpdirpath); err != nil {
        if lg != nil {
            lg.Warn(
                "failed to rename the temporary WAL directory",
                zap.String("tmp-dir-path", tmpdirpath),
                zap.String("dir-path", w.dir),
                zap.Error(err),
            )
        }
        return nil, err
    }

    // 打开文件
    // directory was renamed; sync parent dir to persist rename
    pdir, perr := fileutil.OpenDir(filepath.Dir(w.dir))
    if perr != nil {
        if lg != nil {
            lg.Warn(
                "failed to open the parent data directory",
                zap.String("parent-dir-path", filepath.Dir(w.dir)),
                zap.String("dir-path", w.dir),
                zap.Error(perr),
            )
        }
        return nil, perr
    }

    // 刷盘
    if perr = fileutil.Fsync(pdir); perr != nil {
        if lg != nil {
            lg.Warn(
                "failed to fsync the parent data directory file",
                zap.String("parent-dir-path", filepath.Dir(w.dir)),
                zap.String("dir-path", w.dir),
                zap.Error(perr),
            )
        }
        return nil, perr
    }

    // 关闭
    if perr = pdir.Close(); err != nil {
        if lg != nil {
            lg.Warn(
                "failed to close the parent data directory file",
                zap.String("parent-dir-path", filepath.Dir(w.dir)),
                zap.String("dir-path", w.dir),
                zap.Error(perr),
            )
        }
        return nil, perr
    }

    // 成功
    return w, nil
}

func (w *WAL) renameWAL(tmpdirpath string) (*WAL, error) {
    // 删除现有文件
    if err := os.RemoveAll(w.dir); err != nil {
        return nil, err
    }

    // On non-Windows platforms, hold the lock while renaming. Releasing
    // the lock and trying to reacquire it quickly can be flaky because
    // it's possible the process will fork to spawn a process while this is
    // happening. The fds are set up as close-on-exec by the Go runtime,
    // but there is a window between the fork and the exec where another
    // process holds the lock.
    // 改名
    if err := os.Rename(tmpdirpath, w.dir); err != nil {
        if _, ok := err.(*os.LinkError); ok {
            // 另一种方法
            return w.renameWALUnlock(tmpdirpath)
        }

        return nil, err
    }

    // 创建文件
    w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
    df, err := fileutil.OpenDir(w.dir)
    w.dirFile = df
    return w, err
}

func (w *WAL) renameWALUnlock(tmpdirpath string) (*WAL, error) {
    // rename of directory with locked files doesn't work on windows/cifs;
    // close the WAL to release the locks so the directory can be renamed.
    // 日志
    if w.lg != nil {
        w.lg.Info(
            "closing WAL to release flock and retry directory renaming",
            zap.String("from", tmpdirpath),
            zap.String("to", w.dir),
        )
    } else {
        plog.Infof("releasing file lock to rename %q to %q", tmpdirpath, w.dir)
    }
    w.Close()

    // 改名
    if err := os.Rename(tmpdirpath, w.dir); err != nil {
        return nil, err
    }

    // reopen and relock
    // 重新打开
    newWAL, oerr := Open(w.lg, w.dir, walpb.Snapshot{})
    if oerr != nil {
        return nil, oerr
    }

    // 校验一下
    if _, _, _, err := newWAL.ReadAll(); err != nil {
        newWAL.Close()
        return nil, err
    }

    return newWAL, nil
}

// Open opens the WAL at the given snap.
// The snap SHOULD have been previously saved to the WAL, or the following
// ReadAll will fail.
// The returned WAL is ready to read and the first record will be the one after
// the given snap. The WAL cannot be appended to before reading out all of its
// previous records.
func Open(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
    // 
    w, err := openAtIndex(lg, dirpath, snap, true)
    if err != nil {
        return nil, err
    }

    // 打开文件
    if w.dirFile, err = fileutil.OpenDir(w.dir); err != nil {
        return nil, err
    }

    return w, nil
}

// OpenForRead only opens the wal files for read.
// Write on a read only wal panics.
func OpenForRead(lg *zap.Logger, dirpath string, snap walpb.Snapshot) (*WAL, error) {
    return openAtIndex(lg, dirpath, snap, false)
}

func openAtIndex(lg *zap.Logger, dirpath string, snap walpb.Snapshot, write bool) (*WAL, error) {
    // 解析目录文件
    names, err := readWALNames(lg, dirpath)
    if err != nil {
        // 失败了
        return nil, err
    }

    // 查找序号文件
    nameIndex, ok := searchIndex(lg, names, snap.Index)
    if !ok || !isValidSeq(lg, names[nameIndex:]) {
        return nil, ErrFileNotFound
    }

    // open the wal files
    rcs := make([]io.ReadCloser, 0)
    rs := make([]io.Reader, 0)
    ls := make([]*fileutil.LockedFile, 0)
    for _, name := range names[nameIndex:] {
        p := filepath.Join(dirpath, name)
        if write {
            l, err := fileutil.TryLockFile(p, os.O_RDWR, fileutil.PrivateFileMode)
            if err != nil {
                closeAll(rcs...)
                return nil, err
            }
            ls = append(ls, l)
            rcs = append(rcs, l)
        } else {
            rf, err := os.OpenFile(p, os.O_RDONLY, fileutil.PrivateFileMode)
            if err != nil {
                closeAll(rcs...)
                return nil, err
            }
            ls = append(ls, nil)
            rcs = append(rcs, rf)
        }
        rs = append(rs, rcs[len(rcs)-1])
    }

    closer := func() error { return closeAll(rcs...) }

    // create a WAL ready for reading
    w := &WAL{
        lg:        lg,
        dir:       dirpath,
        start:     snap,
        decoder:   newDecoder(rs...),
        readClose: closer,
        locks:     ls,
    }

    if write {
        // write reuses the file descriptors from read; don't close so
        // WAL can append without dropping the file lock
        w.readClose = nil
        if _, _, err := parseWALName(filepath.Base(w.tail().Name())); err != nil {
            closer()
            return nil, err
        }
        w.fp = newFilePipeline(w.lg, w.dir, SegmentSizeBytes)
    }

    return w, nil
}

// ReadAll reads out records of the current WAL.
// If opened in write mode, it must read out all records until EOF. Or an error
// will be returned.
// If opened in read mode, it will try to read all records if possible.
// If it cannot read out the expected snap, it will return ErrSnapshotNotFound.
// If loaded snap doesn't match with the expected one, it will return
// all the records and error ErrSnapshotMismatch.
// TODO: detect not-last-snap error.
// TODO: maybe loose the checking of match.
// After ReadAll, the WAL will be ready for appending new records.
func (w *WAL) ReadAll() (metadata []byte, state raftpb.HardState, ents []raftpb.Entry, err error) {
    w.mu.Lock()
    defer w.mu.Unlock()

    rec := &walpb.Record{}
    decoder := w.decoder

    var match bool
    // 遍历解码
    for err = decoder.decode(rec); err == nil; err = decoder.decode(rec) {
        switch rec.Type {
        case entryType:
            // 实体
            e := mustUnmarshalEntry(rec.Data)
            if e.Index > w.start.Index {
                ents = append(ents[:e.Index-w.start.Index-1], e)
            }
            w.enti = e.Index

        case stateType:
            // 状态
            state = mustUnmarshalState(rec.Data)

        case metadataType:
            // 元信息
            if metadata != nil && !bytes.Equal(metadata, rec.Data) {
                state.Reset()
                return nil, state, nil, ErrMetadataConflict
            }
            metadata = rec.Data

        case crcType:
            // crc
            crc := decoder.crc.Sum32()
            // current crc of decoder must match the crc of the record.
            // do no need to match 0 crc, since the decoder is a new one at this case.
            if crc != 0 && rec.Validate(crc) != nil {
                state.Reset()
                return nil, state, nil, ErrCRCMismatch
            }
            decoder.updateCRC(rec.Crc)

        case snapshotType:
            // 快照
            var snap walpb.Snapshot
            pbutil.MustUnmarshal(&snap, rec.Data)
            if snap.Index == w.start.Index {
                if snap.Term != w.start.Term {
                    state.Reset()
                    return nil, state, nil, ErrSnapshotMismatch
                }
                match = true
            }

        default:
            state.Reset()
            return nil, state, nil, fmt.Errorf("unexpected block type %d", rec.Type)
        }
    }

    switch w.tail() {
    case nil:
        // We do not have to read out all entries in read mode.
        // The last record maybe a partial written one, so
        // ErrunexpectedEOF might be returned.
        if err != io.EOF && err != io.ErrUnexpectedEOF {
            // 出错了
            state.Reset()
            return nil, state, nil, err
        }
    default:
        // We must read all of the entries if WAL is opened in write mode.
        if err != io.EOF {
            // 没有读完
            state.Reset()
            return nil, state, nil, err
        }
        // decodeRecord() will return io.EOF if it detects a zero record,
        // but this zero record may be followed by non-zero records from
        // a torn write. Overwriting some of these non-zero records, but
        // not all, will cause CRC errors on WAL open. Since the records
        // were never fully synced to disk in the first place, it's safe
        // to zero them out to avoid any CRC errors from new writes.
        if _, err = w.tail().Seek(w.decoder.lastOffset(), io.SeekStart); err != nil {
            return nil, state, nil, err
        }
        if err = fileutil.ZeroToEnd(w.tail().File); err != nil {
            return nil, state, nil, err
        }
    }

    err = nil
    if !match {
        // 找不到
        err = ErrSnapshotNotFound
    }

    // close decoder, disable reading
    // 关闭读
    if w.readClose != nil {
        w.readClose()
        w.readClose = nil
    }
    // 空快照
    w.start = walpb.Snapshot{}

    // 元信息
    w.metadata = metadata

    if w.tail() != nil {
        // 更新
        // create encoder (chain crc with the decoder), enable appending
        w.encoder, err = newFileEncoder(w.tail().File, w.decoder.lastCRC())
        if err != nil {
            return
        }
    }
    w.decoder = nil

    return metadata, state, ents, err
}

// cut closes current file written and creates a new one ready to append.
// cut first creates a temp wal file and writes necessary headers into it.
// Then cut atomically rename temp wal file to a wal file.
func (w *WAL) cut() error {
    // close old wal file; truncate to avoid wasting space if an early cut
    // 当前位置
    off, serr := w.tail().Seek(0, io.SeekCurrent)
    if serr != nil {
        return serr
    }

    // 避免空白
    if err := w.tail().Truncate(off); err != nil {
        return err
    }

    // 同步
    if err := w.sync(); err != nil {
        return err
    }

    // 新文件
    fpath := filepath.Join(w.dir, walName(w.seq()+1, w.enti+1))

    // create a temp wal file with name sequence + 1, or truncate the existing one
    // 打开
    newTail, err := w.fp.Open()
    if err != nil {
        return err
    }

    // update writer and save the previous crc
    // 新锁
    w.locks = append(w.locks, newTail)

    // 更新编码器
    prevCrc := w.encoder.crc.Sum32()
    w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
    if err != nil {
        return err
    }

    // 写crc
    if err = w.saveCrc(prevCrc); err != nil {
        return err
    }

    // 写元信息
    if err = w.encoder.encode(&walpb.Record{Type: metadataType, Data: w.metadata}); err != nil {
        return err
    }

    // 写状态
    if err = w.saveState(&w.state); err != nil {
        return err
    }

    // atomically move temp wal file to wal file
    // 同步
    if err = w.sync(); err != nil {
        return err
    }

    // 计算位置
    off, err = w.tail().Seek(0, io.SeekCurrent)
    if err != nil {
        return err
    }

    // 改名同步
    if err = os.Rename(newTail.Name(), fpath); err != nil {
        return err
    }
    if err = fileutil.Fsync(w.dirFile); err != nil {
        return err
    }

    // reopen newTail with its new path so calls to Name() match the wal filename format
    // 关闭锁
    newTail.Close()

    // 重新打开，定位，更新锁
    if newTail, err = fileutil.LockFile(fpath, os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
        return err
    }
    if _, err = newTail.Seek(off, io.SeekStart); err != nil {
        return err
    }
    w.locks[len(w.locks)-1] = newTail

    // 更新编码器
    prevCrc = w.encoder.crc.Sum32()
    w.encoder, err = newFileEncoder(w.tail().File, prevCrc)
    if err != nil {
        return err
    }

    // 写日志
    if w.lg != nil {
        w.lg.Info("created a new WAL segment", zap.String("path", fpath))
    } else {
        plog.Infof("segmented wal file %v is created", fpath)
    }
    return nil
}

func (w *WAL) sync() error {
    // 编码器刷盘
    if w.encoder != nil {
        if err := w.encoder.flush(); err != nil {
            return err
        }
    }

    start := time.Now()
    // 同步
    err := fileutil.Fdatasync(w.tail().File)
    took := time.Since(start)

    // 监控耗时
    if took > warnSyncDuration {
        if w.lg != nil {
            w.lg.Warn(
                "slow fdatasync",
                zap.Duration("took", took),
                zap.Duration("expected-duration", warnSyncDuration),
            )
        } else {
            plog.Warningf("sync duration of %v, expected less than %v", took, warnSyncDuration)
        }
    }
    walFsyncSec.Observe(took.Seconds())

    return err
}

// ReleaseLockTo releases the locks, which has smaller index than the given index
// except the largest one among them.
// For example, if WAL is holding lock 1,2,3,4,5,6, ReleaseLockTo(4) will release
// lock 1,2 but keep 3. ReleaseLockTo(5) will release 1,2,3 but keep 4.
func (w *WAL) ReleaseLockTo(index uint64) error {
    w.mu.Lock()
    defer w.mu.Unlock()

    if len(w.locks) == 0 {
        // 无锁
        return nil
    }

    var smaller int
    found := false
    // 找第一个比index 大的
    for i, l := range w.locks {
        _, lockIndex, err := parseWALName(filepath.Base(l.Name()))
        if err != nil {
            // 错误
            return err
        }

        if lockIndex >= index {
            smaller = i - 1
            found = true
            break
        }
    }

    // if no lock index is greater than the release index, we can
    // release lock up to the last one(excluding).
    if !found {
        // 没找到比index 大的，就是全部了
        smaller = len(w.locks) - 1
    }

    if smaller <= 0 {
        // 保留
        return nil
    }

    // 释放锁
    for i := 0; i < smaller; i++ {
        if w.locks[i] == nil {
            continue
        }
        w.locks[i].Close()
    }
    w.locks = w.locks[smaller:]

    return nil
}

// Close closes the current WAL file and directory.
func (w *WAL) Close() error {
    w.mu.Lock()
    defer w.mu.Unlock()

    // 关闭文件
    if w.fp != nil {
        w.fp.Close()
        w.fp = nil
    }

    // 需要同步
    if w.tail() != nil {
        if err := w.sync(); err != nil {
            return err
        }
    }

    // 关闭锁
    for _, l := range w.locks {
        if l == nil {
            continue
        }

        if err := l.Close(); err != nil {
            if w.lg != nil {
                w.lg.Warn("failed to close WAL", zap.Error(err))
            } else {
                plog.Errorf("failed to unlock during closing wal: %s", err)
            }
        }
    }

    // 关闭目录
    return w.dirFile.Close()
}

func (w *WAL) saveEntry(e *raftpb.Entry) error {
    // TODO: add MustMarshalTo to reduce one allocation.
    // 序列化
    b := pbutil.MustMarshal(e)

    // 构造记录
    rec := &walpb.Record{Type: entryType, Data: b}

    // 编码
    if err := w.encoder.encode(rec); err != nil {
        return err
    }

    w.enti = e.Index
    return nil
}

func (w *WAL) saveState(s *raftpb.HardState) error {
    if raft.IsEmptyHardState(*s) {
        return nil
    }

    w.state = *s

    // 序列化
    b := pbutil.MustMarshal(s)

    // 构造记录
    rec := &walpb.Record{Type: stateType, Data: b}
    
    // 编码
    return w.encoder.encode(rec)
}

func (w *WAL) Save(st raftpb.HardState, ents []raftpb.Entry) error {
    // 加锁
    w.mu.Lock()
    defer w.mu.Unlock()

    // short cut, do not call sync
    if raft.IsEmptyHardState(st) && len(ents) == 0 {
        // 不需要保存
        return nil
    }

    // 是否需要同步
    mustSync := raft.MustSync(st, w.state, len(ents))

    // TODO(xiangli): no more reference operator
    // 遍历保存
    for i := range ents {
        if err := w.saveEntry(&ents[i]); err != nil {
            return err
        }
    }

    // 保存状态
    if err := w.saveState(&st); err != nil {
        return err
    }

    // 文件大小
    curOff, err := w.tail().Seek(0, io.SeekCurrent)
    if err != nil {
        // 出错
        return err
    }

    // 需要同步
    if curOff < SegmentSizeBytes {
        if mustSync {
            return w.sync()
        }
        return nil
    }

    // ？？？
    return w.cut()
}

func (w *WAL) SaveSnapshot(e walpb.Snapshot) error {
    // 序列化
    b := pbutil.MustMarshal(&e)

    // 加锁
    w.mu.Lock()
    defer w.mu.Unlock()

    // 拼装记录
    rec := &walpb.Record{Type: snapshotType, Data: b}
    if err := w.encoder.encode(rec); err != nil {
        return err
    }

    // 实体跟进
    // update enti only when snapshot is ahead of last index
    if w.enti < e.Index {
        w.enti = e.Index
    }

    // 刷盘
    return w.sync()
}

// 保存crc
func (w *WAL) saveCrc(prevCrc uint32) error {
    return w.encoder.encode(&walpb.Record{Type: crcType, Crc: prevCrc})
}

// 最后的锁
func (w *WAL) tail() *fileutil.LockedFile {
    if len(w.locks) > 0 {
        return w.locks[len(w.locks)-1]
    }

    return nil
}

// 序列号
func (w *WAL) seq() uint64 {
    t := w.tail()
    if t == nil {
        // 如果无数据，从0开始
        return 0
    }

    // 解析序号
    seq, _, err := parseWALName(filepath.Base(t.Name()))
    if err != nil {
        // 出错了
        if w.lg != nil {
            w.lg.Fatal("failed to parse WAL name", zap.String("name", t.Name()), zap.Error(err))
        } else {
            plog.Fatalf("bad wal name %s (%v)", t.Name(), err)
        }
    }

    return seq
}

func closeAll(rcs ...io.ReadCloser) error {
    // 关闭所有
    for _, f := range rcs {
        if err := f.Close(); err != nil {
            return err
        }
    }

    return nil
}
