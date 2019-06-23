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

package etcdmain

import (
    "fmt"
    "os"
    "strings"

    "github.com/coreos/go-systemd/daemon"
    systemdutil "github.com/coreos/go-systemd/util"
    "go.uber.org/zap"
)

func Main() {
    // 验证架构
    checkSupportArch()

    if len(os.Args) > 1 {
        // 取参数
        cmd := os.Args[1]

        // cov
        if covArgs := os.Getenv("ETCDCOV_ARGS"); len(covArgs) > 0 {
            args := strings.Split(os.Getenv("ETCDCOV_ARGS"), "\xe7\xcd")[1:]
            rootCmd.SetArgs(args)
            cmd = "grpc-proxy"
        }

        switch cmd {
        case "gateway", "grpc-proxy":
            // 网管 或 代理
            if err := rootCmd.Execute(); err != nil {
                fmt.Fprint(os.Stderr, err)
                os.Exit(1)
            }
            return
        }
    }

    // 启动服务
    startEtcdOrProxyV2()
}

func notifySystemd(lg *zap.Logger) {
    // 看是否是systemd 起来的
    if !systemdutil.IsRunningSystemd() {
        return
    }
    if lg != nil {
        lg.Info("host was booted with systemd, sends READY=1 message to init daemon")
    }

    // 唤醒
    sent, err := daemon.SdNotify(false, "READY=1")
    if err != nil {
        // 出错
        if lg != nil {
            lg.Error("failed to notify systemd for readiness", zap.Error(err))
        } else {
            plog.Errorf("failed to notify systemd for readiness: %v", err)
        }
    }

    // 失败
    if !sent {
        if lg != nil {
            lg.Warn("forgot to set Type=notify in systemd service file?")
        } else {
            plog.Errorf("forgot to set Type=notify in systemd service file?")
        }
    }
}
