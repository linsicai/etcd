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

// +build !linux,!darwin

package fileutil

import "os"

// 写磁盘时，先写缓冲区，写满后添加到写队列，再写磁盘
// sync 将缓冲区添加到写队列
// fsync 等到写磁盘完成后才返回，包括数据和属性
// fdatasync 类似fsync，但是只同步文件数据部分

// Fsync is a wrapper around file.Sync(). Special handling is needed on darwin platform.
func Fsync(f *os.File) error {
	return f.Sync()
}

// Fdatasync is a wrapper around file.Sync(). Special handling is needed on linux platform.
func Fdatasync(f *os.File) error {
	return f.Sync()
}
