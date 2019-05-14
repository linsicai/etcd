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
    "bytes"
    "hash/crc32"
    "io"
    "io/ioutil"
    "reflect"
    "testing"

    "go.etcd.io/etcd/wal/walpb"
)

var (
    infoData   = []byte("\b\xef\xfd\x02")
    infoRecord = append([]byte("\x0e\x00\x00\x00\x00\x00\x00\x00\b\x01\x10\x99\xb5\xe4\xd0\x03\x1a\x04"), infoData...)
)

func TestReadRecord(t *testing.T) {
    badInfoRecord := make([]byte, len(infoRecord))
    copy(badInfoRecord, infoRecord)
    badInfoRecord[len(badInfoRecord)-1] = 'a'

    tests := []struct {
        data []byte
        wr   *walpb.Record
        we   error
    }{
        // 正常
        {infoRecord, &walpb.Record{Type: 1, Crc: crc32.Checksum(infoData, crcTable), Data: infoData}, nil},

        // 空数据
        {[]byte(""), &walpb.Record{}, io.EOF},

        // 8字节
        {infoRecord[:8], &walpb.Record{}, io.ErrUnexpectedEOF},

        // 前半段少
        {infoRecord[:len(infoRecord)-len(infoData)-8], &walpb.Record{}, io.ErrUnexpectedEOF},

        // 少后半段
        {infoRecord[:len(infoRecord)-len(infoData)], &walpb.Record{}, io.ErrUnexpectedEOF},

        // 后半段少
        {infoRecord[:len(infoRecord)-8], &walpb.Record{}, io.ErrUnexpectedEOF},

        // crc错
        {badInfoRecord, &walpb.Record{}, walpb.ErrCRCMismatch},
    }

    rec := &walpb.Record{}
    for i, tt := range tests {
        // 解码
        buf := bytes.NewBuffer(tt.data)
        decoder := newDecoder(ioutil.NopCloser(buf))
        e := decoder.decode(rec)

        // 预期解码ok
        if !reflect.DeepEqual(rec, tt.wr) {
            t.Errorf("#%d: block = %v, want %v", i, rec, tt.wr)
        }
        // 预期错误码相同
        if !reflect.DeepEqual(e, tt.we) {
            t.Errorf("#%d: err = %v, want %v", i, e, tt.we)
        }

        // 清空
        rec = &walpb.Record{}
    }
}

func TestWriteRecord(t *testing.T) {
    // 测试记录
    b := &walpb.Record{}
    typ := int64(0xABCD)
    d := []byte("Hello world!")
    buf := new(bytes.Buffer)

    // 编码 + flush
    e := newEncoder(buf, 0, 0)
    e.encode(&walpb.Record{Type: typ, Data: d})
    e.flush()

    // 解码
    decoder := newDecoder(ioutil.NopCloser(buf))
    err := decoder.decode(b)
    if err != nil {
        t.Errorf("err = %v, want nil", err)
    }

    // 判断相等
    if b.Type != typ {
        t.Errorf("type = %d, want %d", b.Type, typ)
    }
    if !reflect.DeepEqual(b.Data, d) {
        t.Errorf("data = %v, want %v", b.Data, d)
    }
}
