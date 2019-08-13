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

package transport

import (
	"net"
	"time"
)

// 超时链接
type timeoutConn struct {
	net.Conn
	wtimeoutd  time.Duration
	rdtimeoutd time.Duration
}

// 设置写的deadline
func (c timeoutConn) Write(b []byte) (n int, err error) {
	if c.wtimeoutd > 0 {
		if err := c.SetWriteDeadline(time.Now().Add(c.wtimeoutd)); err != nil {
			return 0, err
		}
	}

	return c.Conn.Write(b)
}

// 设置读读deadline
func (c timeoutConn) Read(b []byte) (n int, err error) {
	if c.rdtimeoutd > 0 {
		if err := c.SetReadDeadline(time.Now().Add(c.rdtimeoutd)); err != nil {
			return 0, err
		}
	}
	return c.Conn.Read(b)
}
