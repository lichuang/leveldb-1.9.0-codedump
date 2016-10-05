// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.
//
// WriteBatch::rep_ :=
//    sequence: fixed64
//    count: fixed32
//    data: record[count]
// record :=
//    kTypeValue varstring varstring         |
//    kTypeDeletion varstring
// varstring :=
//    len: varint32
//    data: uint8[len]

// 上面注释中给出的是req的数据格式

#include "leveldb/write_batch.h"

#include "leveldb/db.h"
#include "db/dbformat.h"
#include "db/memtable.h"
#include "db/write_batch_internal.h"
#include "util/coding.h"

namespace leveldb {

// WriteBatch header has an 8-byte sequence number followed by a 4-byte count.
// header头部数据是8byte的seq，紧跟着4byte的count
static const size_t kHeader = 12;

WriteBatch::WriteBatch() {
  Clear();
}

WriteBatch::~WriteBatch() { }

WriteBatch::Handler::~Handler() { }

void WriteBatch::Clear() {
  rep_.clear();
  rep_.resize(kHeader);
}

// 对waitebatch中的数据进行遍历，其实就是按照格式遍历req中的数据
Status WriteBatch::Iterate(Handler* handler) const {
  Slice input(rep_);
  // 连header大小都不到，出错了
  if (input.size() < kHeader) {
    return Status::Corruption("malformed WriteBatch (too small)");
  }

  // 跳过header
  input.remove_prefix(kHeader);
  Slice key, value;
  int found = 0;
  while (!input.empty()) {
    found++;
    // 拿到数据类型
    char tag = input[0];
    // 拿到之后就跳过数据类型
    input.remove_prefix(1);
    switch (tag) {
      case kTypeValue:
        // 分别拿到key和value
        if (GetLengthPrefixedSlice(&input, &key) &&
            GetLengthPrefixedSlice(&input, &value)) {
          handler->Put(key, value);
        } else {
          return Status::Corruption("bad WriteBatch Put");
        }
        break;
      case kTypeDeletion:
        // 删除数据
        if (GetLengthPrefixedSlice(&input, &key)) {
          handler->Delete(key);
        } else {
          return Status::Corruption("bad WriteBatch Delete");
        }
        break;
      default:
        return Status::Corruption("unknown WriteBatch tag");
    }
  }
  // 数量对不上
  if (found != WriteBatchInternal::Count(this)) {
    return Status::Corruption("WriteBatch has wrong count");
  } else {
    return Status::OK();
  }
}

// 根据前面req_的格式从header部分拿到count
int WriteBatchInternal::Count(const WriteBatch* b) {
  // 距离开始8byte之后是count
  return DecodeFixed32(b->rep_.data() + 8);
}

// 根据前面req_的格式向header部分写入count
void WriteBatchInternal::SetCount(WriteBatch* b, int n) {
  EncodeFixed32(&b->rep_[8], n);
}

// 根据前面req_的格式从header部分拿到seq
SequenceNumber WriteBatchInternal::Sequence(const WriteBatch* b) {
  // SequenceNumber是从起始位置开始的
  return SequenceNumber(DecodeFixed64(b->rep_.data()));
}

// 根据前面req_的格式向header部分写入seq
void WriteBatchInternal::SetSequence(WriteBatch* b, SequenceNumber seq) {
  EncodeFixed64(&b->rep_[0], seq);
}

void WriteBatch::Put(const Slice& key, const Slice& value) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeValue));
  PutLengthPrefixedSlice(&rep_, key);
  PutLengthPrefixedSlice(&rep_, value);
}

void WriteBatch::Delete(const Slice& key) {
  WriteBatchInternal::SetCount(this, WriteBatchInternal::Count(this) + 1);
  rep_.push_back(static_cast<char>(kTypeDeletion));
  PutLengthPrefixedSlice(&rep_, key);
  // 删除时就是新增一个空value的key
}

namespace {
class MemTableInserter : public WriteBatch::Handler {
 public:
  SequenceNumber sequence_;
  MemTable* mem_;

  virtual void Put(const Slice& key, const Slice& value) {
    mem_->Add(sequence_, kTypeValue, key, value);
    sequence_++;
  }
  virtual void Delete(const Slice& key) {
    mem_->Add(sequence_, kTypeDeletion, key, Slice());
    sequence_++;
  }
};
}  // namespace

Status WriteBatchInternal::InsertInto(const WriteBatch* b,
                                      MemTable* memtable) {
  MemTableInserter inserter;
  inserter.sequence_ = WriteBatchInternal::Sequence(b);
  inserter.mem_ = memtable;
  return b->Iterate(&inserter);
}

void WriteBatchInternal::SetContents(WriteBatch* b, const Slice& contents) {
  assert(contents.size() >= kHeader);
  b->rep_.assign(contents.data(), contents.size());
}

void WriteBatchInternal::Append(WriteBatch* dst, const WriteBatch* src) {
  SetCount(dst, Count(dst) + Count(src));
  assert(src->rep_.size() >= kHeader);
  dst->rep_.append(src->rep_.data() + kHeader, src->rep_.size() - kHeader);
}

}  // namespace leveldb
