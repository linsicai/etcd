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

package wal

import (
    "fmt"
    "os"
    "path/filepath"

    "go.etcd.io/etcd/pkg/fileutil"

    "go.uber.org/zap"
)

// 一个异步管道，用于申请磁盘空间

// filePipeline pipelines allocating disk space
type filePipeline struct {
    // 日志
    lg *zap.Logger

    // dir to put files
    // 路径
    dir string

    // size of files to make, in bytes
    // 每个文件大小
    size int64

    // count number of files generated
    // 已产生文件数
    count int

    // 文件锁
    filec chan *fileutil.LockedFile

    // 错误通道
    errc  chan error

    // 结束信号量
    donec chan struct{}
}

func newFilePipeline(lg *zap.Logger, dir string, fileSize int64) *filePipeline {
    fp := &filePipeline{
        lg:    lg,
        dir:   dir,
        size:  fileSize,
        filec: make(chan *fileutil.LockedFile),
        errc:  make(chan error, 1),
        donec: make(chan struct{}),
    }

    // 后台运行
    go fp.run()

    return fp
}

// Open returns a fresh file for writing. Rename the file before calling Open again or there will be file collisions.
// 返回可写文件
// 再次调用前
func (fp *filePipeline) Open() (f *fileutil.LockedFile, err error) {
    select {
    case f = <-fp.filec:
    case err = <-fp.errc:
    }

    // 获取文件 或 错误
    return f, err
}

func (fp *filePipeline) Close() error {
    // 通知结束
    close(fp.donec)

    // 等待结束
    return <-fp.errc
}

func (fp *filePipeline) alloc() (f *fileutil.LockedFile, err error) {
    // count % 2 so this file isn't the same as the one last published
    fpath := filepath.Join(fp.dir, fmt.Sprintf("%d.tmp", fp.count%2))

    // 加锁
    if f, err = fileutil.LockFile(fpath, os.O_CREATE|os.O_WRONLY, fileutil.PrivateFileMode); err != nil {
        return nil, err
    }

    // 创建文件
    if err = fileutil.Preallocate(f.File, fp.size, true); err != nil {
        if fp.lg != nil {
            fp.lg.Warn("failed to preallocate space when creating a new WAL", zap.Int64("size", fp.size), zap.Error(err))
        } else {
            plog.Errorf("failed to allocate space when creating new wal file (%v)", err)
        }
        f.Close()
        return nil, err
    }

    // 成功
    fp.count++

    return f, nil
}

func (fp *filePipeline) run() {
    defer close(fp.errc)

    for {
        // 创建文件，若出错则结束
        f, err := fp.alloc()
        if err != nil {
            fp.errc <- err
            return
        }

        select {
        case fp.filec <- f: // 等待获取
        case <-fp.donec:
            // 协程结束
            os.Remove(f.Name())
            f.Close()
            return
        }
    }
}
