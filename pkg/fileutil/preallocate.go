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

package fileutil

import (
	"io"
	"os"
)

// Preallocate tries to allocate the space for given
// file. This operation is only supported on linux by a
// few filesystems (btrfs, ext4, etc.).
// If the operation is unsupported, no error will be returned.
// Otherwise, the error encountered will be returned.
func Preallocate(f *os.File, sizeInBytes int64, extendFile bool) error {
    // pass 文件大小为零
	if sizeInBytes == 0 {
		// fallocate will return EINVAL if length is 0; skip
		return nil
	}

    // 可扩展文件
	if extendFile {
		return preallocExtend(f, sizeInBytes)
	}

    // 定长文件
	return preallocFixed(f, sizeInBytes)
}

func preallocExtendTrunc(f *os.File, sizeInBytes int64) error {
    // 当前位置
	curOff, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return err
	}

    // 文件尾
	size, err := f.Seek(sizeInBytes, io.SeekEnd)
	if err != nil {
		return err
	}

    // 
	if _, err = f.Seek(curOff, io.SeekStart); err != nil {
		return err
	}

	if sizeInBytes > size {
		return nil
	}

	return f.Truncate(sizeInBytes)
}
