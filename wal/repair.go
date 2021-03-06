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
	"io"
	"os"
	"path/filepath"

	"go.etcd.io/etcd/pkg/fileutil"
	"go.etcd.io/etcd/wal/walpb"

	"go.uber.org/zap"
)

// Repair tries to repair ErrUnexpectedEOF in the
// last wal file by truncating.
func Repair(lg *zap.Logger, dirpath string) bool {
	// 打开最后一个文件
	f, err := openLast(lg, dirpath)
	if err != nil {
		return false
	}
	defer f.Close()

	// 事前日志
	if lg != nil {
		lg.Info("repairing", zap.String("path", f.Name()))
	} else {
		plog.Noticef("repairing %v", f.Name())
	}

	// 申请记录
	rec := &walpb.Record{}

	// 解析器
	decoder := newDecoder(f)
	for {
		// 最后的offset
		lastOffset := decoder.lastOffset()

		err := decoder.decode(rec)
		switch err {
		case nil:
			// update crc of the decoder when necessary
			// 成功
			switch rec.Type {
			case crcType:
				crc := decoder.crc.Sum32()
				// current crc of decoder must match the crc of the record.
				// do no need to match 0 crc, since the decoder is a new one at this case.
				if crc != 0 && rec.Validate(crc) != nil {
					return false
				}

				// 更新crc
				decoder.updateCRC(rec.Crc)
			}
			continue

		case io.EOF:
			// 修复成功
			if lg != nil {
				lg.Info("repaired", zap.String("path", f.Name()), zap.Error(io.EOF))
			}
			return true

		case io.ErrUnexpectedEOF:
			// 创建备份文件
			bf, bferr := os.Create(f.Name() + ".broken")
			if bferr != nil {
				if lg != nil {
					lg.Warn("failed to create backup file", zap.String("path", f.Name()+".broken"), zap.Error(bferr))
				} else {
					plog.Errorf("could not repair %v, failed to create backup file", f.Name())
				}
				return false
			}
			defer bf.Close()

			// 定位至源文件开始位置
			if _, err = f.Seek(0, io.SeekStart); err != nil {
				if lg != nil {
					lg.Warn("failed to read file", zap.String("path", f.Name()), zap.Error(err))
				} else {
					plog.Errorf("could not repair %v, failed to read file", f.Name())
				}
				return false
			}

			// 备份原文件
			if _, err = io.Copy(bf, f); err != nil {
				if lg != nil {
					lg.Warn("failed to copy", zap.String("from", f.Name()+".broken"), zap.String("to", f.Name()), zap.Error(err))
				} else {
					plog.Errorf("could not repair %v, failed to copy file", f.Name())
				}
				return false
			}

			// truncate 文件尾
			if err = f.Truncate(lastOffset); err != nil {
				if lg != nil {
					lg.Warn("failed to truncate", zap.String("path", f.Name()), zap.Error(err))
				} else {
					plog.Errorf("could not repair %v, failed to truncate file", f.Name())
				}
				return false
			}

			// 刷盘
			if err = fileutil.Fsync(f.File); err != nil {
				if lg != nil {
					lg.Warn("failed to fsync", zap.String("path", f.Name()), zap.Error(err))
				} else {
					plog.Errorf("could not repair %v, failed to sync file", f.Name())
				}
				return false
			}

			// 写日志
			if lg != nil {
				lg.Info("repaired", zap.String("path", f.Name()), zap.Error(io.ErrUnexpectedEOF))
			}

			// 修复成功
			return true

		default:
			// 修复失败
			if lg != nil {
				lg.Warn("failed to repair", zap.String("path", f.Name()), zap.Error(err))
			} else {
				plog.Errorf("could not repair error (%v)", err)
			}
			return false
		}
	}
}

// openLast opens the last wal file for read and write.
func openLast(lg *zap.Logger, dirpath string) (*fileutil.LockedFile, error) {
	// 加载文件列表
	names, err := readWALNames(lg, dirpath)
	if err != nil {
		return nil, err
	}

	// 取最后一个
	last := filepath.Join(dirpath, names[len(names)-1])

	// 加锁返回
	return fileutil.LockFile(last, os.O_RDWR, fileutil.PrivateFileMode)
}
