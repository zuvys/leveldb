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
// An entry for a particular key-value pair has the form:   * entry结构
//     shared_bytes: varint32                               * key共享部分长度
//     unshared_bytes: varint32                             * key非共享部分长度
//     value_length: varint32                               * value长度
//     key_delta: char[unshared_bytes]                      * key非共享部分数据
//     value: char[value_length]                            * value数据
// shared_bytes == 0 for restart points.                    * 重启点key共享部分长度为 0
//
// The trailer of the block has the form:                   * block尾部结构
//     restarts: uint32[num_restarts]                       * 重启点数组
//     num_restarts: uint32                                 * 重启点数组长度
// restarts[i] contains the offset within the block of the ith restart point.   * 重启点数组中第i个元素存放的是block中第i个重启点的偏移量
//Block结构
//offset_0 record1
//offset_1 record2
//offset_2 record3
//offset_3 record4
//offset_4 record5
//restart1  (offset_0)
//restart2  (offset_3)
//num_restart (2)

#include "table/block_builder.h"

#include <algorithm>
#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/options.h"
#include "util/coding.h"

namespace leveldb {

BlockBuilder::BlockBuilder(const Options* options)
    : options_(options), restarts_(), counter_(0), finished_(false) {
  assert(options->block_restart_interval >= 1);
  //第0个重启点偏移量为0
  restarts_.push_back(0);  // First restart point is at offset 0 
}

void BlockBuilder::Reset() {
  buffer_.clear();
  restarts_.clear();
  restarts_.push_back(0);  // First restart point is at offset 0
  counter_ = 0;
  finished_ = false;
  last_key_.clear();
}

//估算block当前占用空间大小
size_t BlockBuilder::CurrentSizeEstimate() const {
  //数据大小+Block尾部(重启点数组大小+记录重启点数组长度的unit32_t)
  return (buffer_.size() +                       // Raw data buffer
          restarts_.size() * sizeof(uint32_t) +  // Restart array
          sizeof(uint32_t));                     // Restart array length
}

Slice BlockBuilder::Finish() {
  // Append restart array
  //填充Block尾部数据(重启点数组+重启点长度)
  for (size_t i = 0; i < restarts_.size(); i++) {
    PutFixed32(&buffer_, restarts_[i]);
  }
  PutFixed32(&buffer_, restarts_.size());

  //标记完成
  finished_ = true;
  return Slice(buffer_);
}

void BlockBuilder::Add(const Slice& key, const Slice& value) {
  Slice last_key_piece(last_key_);
  assert(!finished_);               //如果已经标记完成了,错误               
  assert(counter_ <= options_->block_restart_interval); //重启点跨度不能超过设置的值
  assert(buffer_.empty()  // No values yet?
         || options_->comparator->Compare(key, last_key_piece) > 0);  //插入的key必须比最后一个插入的key更大,保持有序
  size_t shared = 0;
  if (counter_ < options_->block_restart_interval) {
    // See how much sharing to do with previous string
    //如果重启点跨度还没有超过设置的值
    //就计算当前插入key与最后一次插入key的共享长度
    //跨度太长会影响读取效率
    const size_t min_length = std::min(last_key_piece.size(), key.size());
    while ((shared < min_length) && (last_key_piece[shared] == key[shared])) {
      shared++;
    }
  } else {
    // Restart compression
    //否则重置重启点
    restarts_.push_back(buffer_.size());
    counter_ = 0;
  }
  const size_t non_shared = key.size() - shared;

  // Add "<shared><non_shared><value_size>" to buffer_
  //写入 key共享长度|key非共享长度|value长度
  PutVarint32(&buffer_, shared);
  PutVarint32(&buffer_, non_shared);
  PutVarint32(&buffer_, value.size());

  // Add string delta to buffer_ followed by value
  //写入key非共享部分数据
  //写入value数据
  buffer_.append(key.data() + shared, non_shared);
  buffer_.append(value.data(), value.size());

  // Update state
  //更新最后一个key 类似与last_key_=key 没有直接赋值是因为如果直接引用key,key可能在之后释放了
  last_key_.resize(shared);
  last_key_.append(key.data() + shared, non_shared);
  assert(Slice(last_key_) == key);
  counter_++;
}

}  // namespace leveldb
