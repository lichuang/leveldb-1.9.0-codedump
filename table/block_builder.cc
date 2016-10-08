// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// BlockBuilder generates blocks where keys are prefix-compressed:
//
// When we store a key, we drop the prefix shared with the previous
// string.  This helps reduce the space requirement significantly.
// Furthermore, once every K keys, we do not apply the prefix
// compression and store the entire key.  We call this a "restart
// point".  The tail end of the block stores the offsets of all of the
// restart points, and can be used to do a binary search when looking
// for a particular key.  Values are stored as-is (without compression)
// immediately following the corresponding key.
//
// An entry for a particular key-value pair has the form:
//     shared_bytes: varint32
//     unshared_bytes: varint32
//     value_length: varint32
//     key_delta: char[unshared_bytes]
//     value: char[value_length]
// shared_bytes == 0 for restart points.
//
// The trailer of the block has the form:
//     restarts: uint32[num_restarts]
//     num_restarts: uint32
// restarts[i] contains the offset within the block of the ith restart point.

// BlockBuilder用于生成前缀压缩的block：
// 当保存一个key时，采用前缀压缩法，可以只保存非共享的前缀字符串。
// 这个做法显著的降低了磁盘空间的消耗。
// 除此之外，每隔K个key，就不采用前缀压缩算法，而是存储这个key字符串。
// 这个位置，被称为“重启点”。每个block的尾部存储了这个block的所有重启点的偏移量，
// 使用重启点，可以采用二分查找来定位一个key位于block的哪个范围中。
// 值紧跟着存放在对应key的位置。
//
// 每个K-V键值对的格式：
//  共享部分的大小：varint32
//  非共享部分的大小：varint32
//  值长度：varint32
//  非共享key字符串数据：char[unshared_bytes]
//  值：char[value_length]
//  共享部分大小为0，代表这是重启点。
//
//  block剩余部分的格式：
//    restart数组：uint32[num_restarts]
//    num_restarts：uint32 重启点数量
//    restart数组存放的是这个block所有重启点在block文件中的偏移量。
#include "table/block_builder.h"

#include <algorithm>
#include <assert.h>
#include "leveldb/comparator.h"
#include "leveldb/table_builder.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options),
      restarts_(),
      counter_(0),
      finished_(false) {
  assert(options->block_restart_interval >= 1);
  restarts_.push_back(0);       // First restart point is at offset 0
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);       // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

// 预估算block文件的大小
size_t BlockBuilder::CurrentSizeEstimate() const {
  return (buffer_.size() +                        // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +   // Restart array
          sizeof(uint32_t));                      // Restart array length
}

// 写入重启点数组和重启点数量
Slice BlockBuilder::Finish() {
  // Append restart array
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);
  assert(counter_ <= options_->block_restart_interval);
  assert(buffer_.empty() // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
	  // 如果少于一个restart里面数据的数量,那么就继续塞进这个restart数据里面
    // See how much sharing to do with previous string
    // 先算出来两者的最小长度
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    // 计算共享部分的数据
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
	// 否则重新开始一个restart
    // Restart compression
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // 下面就是按照格式来写数据了
  // Add "<shared><non_shared><value_size>" to buffer_
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // 保存最后一次更新的key数据
  // Update state
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  // 将这组restart的计数加一
  counter_++;
}

}  // namespace leveldb
