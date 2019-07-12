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

package membership

import (
	"errors"

	"go.etcd.io/etcd/etcdserver/api/v2error"
)

var (
	// 成员ID 错误
	ErrIDRemoved  = errors.New("membership: ID removed")
	ErrIDExists   = errors.New("membership: ID exists")
	ErrIDNotFound = errors.New("membership: ID not found")

	// 对端URL 已存在
	ErrPeerURLexists = errors.New("membership: peerURL exists")
)

// key 没有找到
func isKeyNotFound(err error) bool {
	e, ok := err.(*v2error.Error)
	return ok && e.ErrorCode == v2error.EcodeKeyNotFound
}
