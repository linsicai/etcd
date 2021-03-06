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

// etcdctl is a command line application that controls etcd.
package main

import (
    "fmt"
    "os"

    "go.etcd.io/etcd/etcdctl/ctlv2"
    "go.etcd.io/etcd/etcdctl/ctlv3"
)

const (
    apiEnv = "ETCDCTL_API"
)

func main() {
    // 取环境变量，然后再清空
    // unset apiEnv to avoid side-effect for future env and flag parsing.
    apiv := os.Getenv(apiEnv)
    os.Unsetenv(apiEnv)

    if len(apiv) == 0 || apiv == "3" {
        // 默认为v3
        ctlv3.Start()
        return
    }

    if apiv == "2" {
        // 启动V2
        ctlv2.Start()
        return
    }

    // 版本不支持
    fmt.Fprintln(os.Stderr, "unsupported API version", apiv)
    os.Exit(1)
}
