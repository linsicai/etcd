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
	"encoding/binary"
	"hash"
	"io"
	"os"
	"sync"

	"go.etcd.io/etcd/pkg/crc"
	"go.etcd.io/etcd/pkg/ioutil"
	"go.etcd.io/etcd/wal/walpb"
)

// walPageBytes is the alignment for flushing records to the backing Writer.
// It should be a multiple of the minimum sector size so that WAL can safely
// distinguish between torn writes and ordinary data corruption.
const walPageBytes = 8 * minSectorSize

type encoder struct {
	// 锁
	mu sync.Mutex

	//
	bw *ioutil.PageWriter

	// crc
	crc hash.Hash32

	// 字节流
	buf []byte

	// 辅助用
	uint64buf []byte
}

func newEncoder(w io.Writer, prevCrc uint32, pageOffset int) *encoder {
	return &encoder{
		bw:  ioutil.NewPageWriter(w, walPageBytes, pageOffset),
		crc: crc.New(prevCrc, crcTable),
		// 1MB buffer
		buf:       make([]byte, 1024*1024),
		uint64buf: make([]byte, 8),
	}
}

// newFileEncoder creates a new encoder with current file offset for the page writer.
func newFileEncoder(f *os.File, prevCrc uint32) (*encoder, error) {
	offset, err := f.Seek(0, io.SeekCurrent)
	if err != nil {
		return nil, err
	}

	return newEncoder(f, prevCrc, int(offset)), nil
}

func (e *encoder) encode(rec *walpb.Record) error {
	// 加锁
	e.mu.Lock()
	defer e.mu.Unlock()

	// 计算crc
	e.crc.Write(rec.Data)
	rec.Crc = e.crc.Sum32()

	var (
		data []byte
		err  error
		n    int
	)

	// 做压缩 或 加密
	if rec.Size() > len(e.buf) {
		data, err = rec.Marshal()
		if err != nil {
			return err
		}
	} else {
		n, err = rec.MarshalTo(e.buf)
		if err != nil {
			return err
		}
		data = e.buf[:n]
	}

	// 写数据长度
	lenField, padBytes := encodeFrameSize(len(data))
	if err = writeUint64(e.bw, lenField, e.uint64buf); err != nil {
		return err
	}

	// 字节对齐填充
	if padBytes != 0 {
		data = append(data, make([]byte, padBytes)...)
	}

	// 写数据
	_, err = e.bw.Write(data)

	return err
}

func encodeFrameSize(dataBytes int) (lenField uint64, padBytes int) {
	// 数据长度
	lenField = uint64(dataBytes)

	// force 8 byte alignment so length never gets a torn write
	padBytes = (8 - (dataBytes % 8)) % 8
	if padBytes != 0 {
		// 高8位保存pad字节
		// 低56位保存长度
		lenField |= uint64(0x80|padBytes) << 56
	}

	return lenField, padBytes
}

func (e *encoder) flush() error {
	// 加锁
	e.mu.Lock()
	defer e.mu.Unlock()

	// 刷盘
	return e.bw.Flush()
}

// 写小端长度
func writeUint64(w io.Writer, n uint64, buf []byte) error {
	// http://golang.org/src/encoding/binary/binary.go
	binary.LittleEndian.PutUint64(buf, n)

	_, err := w.Write(buf)

	return err
}
