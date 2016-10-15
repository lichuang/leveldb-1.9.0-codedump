// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "db/log_writer.h"

#include <stdint.h>
#include "leveldb/env.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {
namespace log {

Writer::Writer(WritableFile* dest)
    : dest_(dest),
      block_offset_(0) {
  for (int i = 0; i <= kMaxRecordType; i++) {
    char t = static_cast<char>(i);
    type_crc_[i] = crc32c::Value(&t, 1);
  }
}

Writer::~Writer() {
}

// 向日志文件中增添一条记录
Status Writer::AddRecord(const Slice& slice) {
  // ptr和left分别表示所要添加数据的指针和数据大小
  const char* ptr = slice.data();
  size_t left = slice.size();

  // Fragment the record if necessary and emit it.  Note that if slice
  // is empty, we still want to iterate once to emit a single
  // zero-length record
  Status s;
  bool begin = true;
  do {
	// 得到当前block剩余大小
    const int leftover = kBlockSize - block_offset_;
    assert(leftover >= 0);
    // 如果连头部都不够尺寸了，那么就要切换到下一个block
    if (leftover < kHeaderSize) {
      // Switch to a new block
      // 增加一个空的头部信息
      if (leftover > 0) {
        // Fill the trailer (literal below relies on kHeaderSize being 7)
        assert(kHeaderSize == 7);
        // 剩余部分填0
        dest_->Append(Slice("\x00\x00\x00\x00\x00\x00", leftover));
      }
      // 切换到新的block
      block_offset_ = 0;
    }

    // Invariant: we never leave < kHeaderSize bytes in a block.
    assert(kBlockSize - block_offset_ - kHeaderSize >= 0);

    // 当前block可用尺寸
    const size_t avail = kBlockSize - block_offset_ - kHeaderSize;
    // 数据大小和block可用大小，哪个小用哪个
    const size_t fragment_length = (left < avail) ? left : avail;

    RecordType type;
    const bool end = (left == fragment_length);
    if (begin && end) {
      // 一个block能包完所有的记录数据
      type = kFullType;
    } else if (begin) {
      // 一个block不能包完所有的记录数据，但是这部分是第一段数据
      type = kFirstType;
    } else if (end) {
      // 一个block不能包完所有的记录数据，但是这部分是最后一段数据
      type = kLastType;
    } else {
      // 一个block不能包完所有的记录数据，但是这部分是中间的数据
      type = kMiddleType;
    }

    // 正经添加数据
    s = EmitPhysicalRecord(type, ptr, fragment_length);
    ptr += fragment_length;
    left -= fragment_length;
    begin = false;
  } while (s.ok() && left > 0);
  return s;
}

Status Writer::EmitPhysicalRecord(RecordType t, const char* ptr, size_t n) {
  assert(n <= 0xffff);  // Must fit in two bytes
  assert(block_offset_ + kHeaderSize + n <= kBlockSize);

  // Format the header
  // 写block的header
  char buf[kHeaderSize];
  // 写数据大小
  buf[4] = static_cast<char>(n & 0xff);
  buf[5] = static_cast<char>(n >> 8);
  // 写数据类型
  buf[6] = static_cast<char>(t);

  // Compute the crc of the record type and the payload.
  // 写CRC32的校验值
  uint32_t crc = crc32c::Extend(type_crc_[t], ptr, n);
  crc = crc32c::Mask(crc);                 // Adjust for storage
  EncodeFixed32(buf, crc);

  // Write the header and the payload
  // OK，先写header信息
  Status s = dest_->Append(Slice(buf, kHeaderSize));
  if (s.ok()) {
	// 再写正经的数据
    s = dest_->Append(Slice(ptr, n));
    if (s.ok()) {
      // flush到磁盘
      s = dest_->Flush();
    }
  }
  block_offset_ += kHeaderSize + n;
  return s;
}

}  // namespace log
}  // namespace leveldb
