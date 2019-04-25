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
    "errors"
    "fmt"
    "strings"

    "go.etcd.io/etcd/pkg/fileutil"

    "go.uber.org/zap"
)

// 错误
var errBadWALName = errors.New("bad wal name")

// Exist returns true if there are any files in a given directory.
// 检测是否wal目录
func Exist(dir string) bool {
    names, err := fileutil.ReadDir(dir, fileutil.WithExt(".wal"))
    if err != nil {
        return false
    }

    return len(names) != 0
}

// searchIndex returns the last array index of names whose raft index section is
// equal to or smaller than the given index.
// The given names MUST be sorted.
func searchIndex(lg *zap.Logger, names []string, index uint64) (int, bool) {
    for i := len(names) - 1; i >= 0; i-- {
        name := names[i]

        // 提取索引
        _, curIndex, err := parseWALName(name)
        if err != nil {
            if lg != nil {
                lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
            } else {
                plog.Panicf("parse correct name should never fail: %v", err)
            }
        }

        // 类似二分查找
        if index >= curIndex {
            return i, true
        }
    }

    return -1, false
}

// names should have been sorted based on sequence number.
// isValidSeq checks whether seq increases continuously.
func isValidSeq(lg *zap.Logger, names []string) bool {
    var lastSeq uint64

    for _, name := range names {
        curSeq, _, err := parseWALName(name)
        if err != nil {
            // 解析wal 名字出错
            if lg != nil {
                lg.Panic("failed to parse WAL file name", zap.String("path", name), zap.Error(err))
            } else {
                plog.Panicf("parse correct name should never fail: %v", err)
            }
        }

        // 没有按顺序来，报错
        if lastSeq != 0 && lastSeq != curSeq-1 {
            return false
        }

        // 往前走
        lastSeq = curSeq
    }

    // 是顺序的
    return true
}

func readWALNames(lg *zap.Logger, dirpath string) ([]string, error) {
    // 读目录
    names, err := fileutil.ReadDir(dirpath)
    if err != nil {
        return nil, err
    }

    // 提取wal 文件列表
    wnames := checkWalNames(lg, names)
    if len(wnames) == 0 {
        return nil, ErrFileNotFound
    }

    return wnames, nil
}

func checkWalNames(lg *zap.Logger, names []string) []string {
    wnames := make([]string, 0)
    for _, name := range names {
        if _, _, err := parseWALName(name); err != nil {
            // 文件格式告警
            // don't complain about left over tmp files
            if !strings.HasSuffix(name, ".tmp") {
                if lg != nil {
                    lg.Warn(
                        "ignored file in WAL directory",
                        zap.String("path", name),
                    )
                } else {
                    plog.Warningf("ignored file %v in wal", name)
                }
            }
            continue
        }

        // 有效名字
        wnames = append(wnames, name)
    }

    return wnames
}

// 解析wal 名字
func parseWALName(str string) (seq, index uint64, err error) {
    // 后缀名不对
    if !strings.HasSuffix(str, ".wal") {
        return 0, 0, errBadWALName
    }

    // 提取数字
    _, err = fmt.Sscanf(str, "%016x-%016x.wal", &seq, &index)

    return seq, index, err
}

// wal 名字，序列号-索引.wal
func walName(seq, index uint64) string {
    return fmt.Sprintf("%016x-%016x.wal", seq, index)
}
