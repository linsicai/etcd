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

package wait

import "sync"

// 接口
type WaitTime interface {
    // Wait returns a chan that waits on the given logical deadline.
    // The chan will be triggered when Trigger is called with a
    // deadline that is later than the one it is waiting for.

    // 创建一个deadline 之后才可用的chan
    Wait(deadline uint64) <-chan struct{}


    // Trigger triggers all the waiting chans with an earlier logical deadline.

    // 触发deadline 及之前的通道
    Trigger(deadline uint64)
}

// chan struct 永远是阻塞的，直至它被close
var closec chan struct{}
// 初始化
func init() {
    // 创建通道
    closec = make(chan struct{});

    // 开启通道
    close(closec)
}

// 时间列表
type timeList struct {
    // 锁
    l                   sync.Mutex

    // 上一次触发时间
    lastTriggerDeadline uint64

    // 时间 to 通道映射表
    m                   map[uint64]chan struct{}
}

// 创建时间列表
func NewTimeList() *timeList {
    return &timeList{
        m: make(map[uint64]chan struct{}
    )}
}

// Wait 接口实现
func (tl *timeList) Wait(deadline uint64) <-chan struct{} {
    // 锁
    tl.l.Lock()
    defer tl.l.Unlock()

    // 老的deadline
    if tl.lastTriggerDeadline >= deadline {
        return closec
    }

    // 创建新的chan
    ch := tl.m[deadline]
    if ch == nil {
        ch = make(chan struct{})

        tl.m[deadline] = ch
    }

    return ch
}

// 触发实现接口
func (tl *timeList) Trigger(deadline uint64) {
    // 锁
    tl.l.Lock()
    defer tl.l.Unlock()

    // 记录时间
    tl.lastTriggerDeadline = deadline

    // 触发之前的
    for t, ch := range tl.m {
        if t <= deadline {
            delete(tl.m, t)

            close(ch)
        }
    }
}
