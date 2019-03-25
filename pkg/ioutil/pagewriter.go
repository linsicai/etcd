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

package ioutil

import (
	"io"
)

// 默认buffer，128k
var defaultBufferBytes = 128 * 1024

// PageWriter implements the io.Writer interface so that writes will
// either be in page chunks or from flushing.
type PageWriter struct {
	w io.Writer

	// pageOffset tracks the page offset of the base of the buffer
	pageOffset int

    // 页面大小
	// pageBytes is the number of bytes per page
	pageBytes int

    // 已缓存大小
	// bufferedBytes counts the number of bytes pending for write in the buffer
	bufferedBytes int

    // 缓存
	// buf holds the write buffer
	buf []byte

    // 缓存高水位
	// bufWatermarkBytes is the number of bytes the buffer can hold before it needs
	// to be flushed. It is less than len(buf) so there is space for slack writes
	// to bring the writer to page alignment.
	bufWatermarkBytes int
}

// NewPageWriter creates a new PageWriter. pageBytes is the number of bytes
// to write per page. pageOffset is the starting offset of io.Writer.
func NewPageWriter(w io.Writer, pageBytes, pageOffset int) *PageWriter {
	return &PageWriter{
		w:                 w,
		pageOffset:        pageOffset,
		pageBytes:         pageBytes,
		buf:               make([]byte, defaultBufferBytes+pageBytes),
		bufWatermarkBytes: defaultBufferBytes,
	}
}

func (pw *PageWriter) Write(p []byte) (n int, err error) {
    // 判断缓存高水位
	if len(p)+pw.bufferedBytes <= pw.bufWatermarkBytes {
		// no overflow，直接写入缓存
		copy(pw.buf[pw.bufferedBytes:], p)

		pw.bufferedBytes += len(p)

		return len(p), nil
	}

    // 计算碎片大小
	// complete the slack page in the buffer if unaligned
	slack := pw.pageBytes - ((pw.pageOffset + pw.bufferedBytes) % pw.pageBytes)
	if slack != pw.pageBytes {
	    // 判断碎片是否可以写入
		partial := slack > len(p)
		if partial {
			// not enough data to complete the slack page
			slack = len(p)
		}

        // 写入碎片
		// special case: writing to slack page in buffer
		copy(pw.buf[pw.bufferedBytes:], p[:slack])
		pw.bufferedBytes += slack
		n = slack
		p = p[slack:]

        // 碎片可写全部，就退出
		if partial {
			// avoid forcing an unaligned flush
			return n, nil
		}
	}

    // 缓存落盘
	// buffer contents are now page-aligned; clear out
	if err = pw.Flush(); err != nil {
		return n, err
	}

    // 写数据落盘
	// directly write all complete pages without copying
	if len(p) > pw.pageBytes {
	    // 计算页数
		pages := len(p) / pw.pageBytes

        // 批量写
		c, werr := pw.w.Write(p[:pages*pw.pageBytes])
		n += c
		if werr != nil {
			return n, werr
		}

        // 去除已经写过的
		p = p[pages*pw.pageBytes:]
	}

    // 写剩下的
	// write remaining tail to buffer
	c, werr := pw.Write(p)
	n += c
	return n, werr
}

func (pw *PageWriter) Flush() error {
	if pw.bufferedBytes == 0 {
		return nil
	}

    // 写缓存
	_, err := pw.w.Write(pw.buf[:pw.bufferedBytes])

    // 修正偏移量
	pw.pageOffset = (pw.pageOffset + pw.bufferedBytes) % pw.pageBytes

	pw.bufferedBytes = 0
	return err
}
