// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/merger.h"

#include "leveldb/comparator.h"
#include "leveldb/iterator.h"
#include "table/iterator_wrapper.h"

namespace leveldb {

namespace {
class MergingIterator : public Iterator {
 public:
  MergingIterator(const Comparator* comparator, Iterator** children, int n)
      : comparator_(comparator),
        children_(new IteratorWrapper[n]),
        n_(n),
        current_(NULL),
        direction_(kForward) {
    for (int i = 0; i < n; i++) {
      children_[i].Set(children[i]);
    }
  }

  virtual ~MergingIterator() {
    delete[] children_;
  }

  virtual bool Valid() const {
    return (current_ != NULL);
  }

  virtual void SeekToFirst() {
    // 所有的成员都seek到第一个元素
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToFirst();
    }
    // 找到其中最小的child
    FindSmallest();
    // 修改方向为向前查找
    direction_ = kForward;
  }

  virtual void SeekToLast() {
    // 所有的成员都seek到最后一个元素
    for (int i = 0; i < n_; i++) {
      children_[i].SeekToLast();
    }
    // 找到其中最大的child
    FindLargest();
    // 修改方向为向后查找
    direction_ = kReverse;
  }

  virtual void Seek(const Slice& target) {
    // 所有成员都找到target的位置
    for (int i = 0; i < n_; i++) {
      children_[i].Seek(target);
    }
    // 找到最小的
    FindSmallest();
    // 修改方向为向前查找
    direction_ = kForward;
  }

  virtual void Next() {
    assert(Valid());

    // Ensure that all children are positioned after key().
    // If we are moving in the forward direction, it is already
    // true for all of the non-current_ children since current_ is
    // the smallest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    // 以下保证所有child都在key()位置之后。
    // 如果当前是向前的方向，那么这个自然已经得到了保证，因为current就是当前最小的child，
    // 同时满足key() == current_->key()
    // 否则，就需要明确的移动到对应的位置去
    if (direction_ != kForward) { // 只有在方向不是向前的情况下才做这个操作
      // 保证所有的child位置都在current的key之后
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {  // 如果不是current的child
          child->Seek(key()); // 移动到key对应的位置
          if (child->Valid() && // 如果child当前的key就是current->key，那么向前移动一步
              comparator_->Compare(key(), child->key()) == 0) {
            child->Next();
          }
        }
      }
      direction_ = kForward;
    }

    // current向前走一步
    current_->Next();
    // 找到最小的child
    FindSmallest();
  }

  virtual void Prev() {
    assert(Valid());

    // 这段代码不做解释了，见上面的Next操作，类似的
    
    // Ensure that all children are positioned before key().
    // If we are moving in the reverse direction, it is already
    // true for all of the non-current_ children since current_ is
    // the largest child and key() == current_->key().  Otherwise,
    // we explicitly position the non-current_ children.
    if (direction_ != kReverse) {
      for (int i = 0; i < n_; i++) {
        IteratorWrapper* child = &children_[i];
        if (child != current_) {
          // 先以current指针的key来找到一个位置
          child->Seek(key());
          if (child->Valid()) {
            // Child is at first entry >= key().  Step back one to be < key()
        	// 如果找到向前走一步
            child->Prev();
          } else {
            // Child has no entries >= key().  Position at last entry.
        	// 否则绕回到last位置
            child->SeekToLast();
          }
        }
      }
      direction_ = kReverse;
    }

    current_->Prev();
    FindLargest();
  }

  // key返回current key
  virtual Slice key() const {
    assert(Valid());
    return current_->key();
  }

  // key返回current value 
  virtual Slice value() const {
    assert(Valid());
    return current_->value();
  }

  virtual Status status() const {
    Status status;
    for (int i = 0; i < n_; i++) {
      status = children_[i].status();
      if (!status.ok()) {
        break;
      }
    }
    return status;
  }

 private:
  void FindSmallest();
  void FindLargest();

  // We might want to use a heap in case there are lots of children.
  // For now we use a simple array since we expect a very small number
  // of children in leveldb.
  const Comparator* comparator_;
  IteratorWrapper* children_;
  int n_;
  // 保存当前的指针
  IteratorWrapper* current_;

  // Which direction is the iterator moving?
  enum Direction {
    kForward,
    kReverse
  };
  // 保存查找的方向
  Direction direction_;
};

// 找到所有中最小的,同时把current置为这个找到的最小的child
void MergingIterator::FindSmallest() {
  IteratorWrapper* smallest = NULL;
  for (int i = 0; i < n_; i++) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (smallest == NULL) {
        smallest = child;
      } else if (comparator_->Compare(child->key(), smallest->key()) < 0) {
        smallest = child;
      }
    }
  }
  current_ = smallest;
}

// 找到所有中最大的,同时把current置为这个找到的最大的child
void MergingIterator::FindLargest() {
  IteratorWrapper* largest = NULL;
  for (int i = n_-1; i >= 0; i--) {
    IteratorWrapper* child = &children_[i];
    if (child->Valid()) {
      if (largest == NULL) {
        largest = child;
      } else if (comparator_->Compare(child->key(), largest->key()) > 0) {
        largest = child;
      }
    }
  }
  current_ = largest;
}
}  // namespace

Iterator* NewMergingIterator(const Comparator* cmp, Iterator** list, int n) {
  assert(n >= 0);
  if (n == 0) {
    // 空迭代器
    return NewEmptyIterator();
  } else if (n == 1) {
    // 只有一个元素
    return list[0];
  } else {
    // 否则创建合并的迭代器
    return new MergingIterator(cmp, list, n);
  }
}

}  // namespace leveldb
