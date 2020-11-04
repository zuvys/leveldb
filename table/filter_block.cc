// Copyright (c) 2012 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "table/filter_block.h"

#include "leveldb/filter_policy.h"
#include "util/coding.h"

namespace leveldb {

// See doc/table_format.md for an explanation of the filter block format.

// Generate new filter every 2KB of data
// 每2KB(1<<kFilterBaseLg)生成一个新的filter
// filter block格式:
//    [filter 0]                              
//    [filter 1]
//    [filter 2]
//    ...
//    [filter N-1]
//
//    [offset of filter 0]                  : 4 bytes  记录filter 0 偏移值(位置)
//    [offset of filter 1]                  : 4 bytes  记录filter 1 偏移值(位置)
//    [offset of filter 2]                  : 4 bytes  记录filter 2 偏移值(位置)
//    ...
//    [offset of filter N-1]                : 4 bytes  记录filter n-1 偏移值(位置)
//
//    [offset of beginning of offset array] : 4 bytes  记录偏移数组第一个元素的位置,即[offset of filter 0]元素的偏移值
 //   lg(base)                              : 1 byte   基准值指定data block长度超过多少就要生成一个单独的filter,否则多个data block可以共用一个filter,既下面的kFilterBaseLg

static const size_t kFilterBaseLg = 11;
static const size_t kFilterBase = 1 << kFilterBaseLg;

FilterBlockBuilder::FilterBlockBuilder(const FilterPolicy* policy)
    : policy_(policy) {}

//标记一下从这个Block开始了
void FilterBlockBuilder::StartBlock(uint64_t block_offset) {
  //计算这个block对应第几个filter
  uint64_t filter_index = (block_offset / kFilterBase);
  assert(filter_index >= filter_offsets_.size());
  //要开始一个新的filter了,把已添加key生成filter
  while (filter_index > filter_offsets_.size()) {
    GenerateFilter();
  }
}

//添加key
void FilterBlockBuilder::AddKey(const Slice& key) {
  Slice k = key;
  //记录key的位置
  start_.push_back(keys_.size());
  //key数据写入keys_
  keys_.append(k.data(), k.size());
}

Slice FilterBlockBuilder::Finish() {
  if (!start_.empty()) {
    //如果还有key,则生成filter
    GenerateFilter();
  }

  // Append array of per-filter offsets
  //添加每个filter的偏移值记录
  const uint32_t array_offset = result_.size();
  for (size_t i = 0; i < filter_offsets_.size(); i++) {
    PutFixed32(&result_, filter_offsets_[i]);
  }

  //记录filter_offsets的起始位置
  PutFixed32(&result_, array_offset);
  //记录data block长度基准值
  result_.push_back(kFilterBaseLg);  // Save encoding parameter in result
  return Slice(result_);
}

//生成filter
void FilterBlockBuilder::GenerateFilter() {
  const size_t num_keys = start_.size();
  if (num_keys == 0) {
    //key数目为0,不需要生成result_,记录一下filter偏移值即可
    // Fast path if there are no keys for this filter
    filter_offsets_.push_back(result_.size());
    return;
  }

  // Make list of keys from flattened key structure
  //记录一下keys_长度,下面用这个值做最后一个key的长度计算,len(key[i])=start[i+1]-start[i]
  start_.push_back(keys_.size());  // Simplify length computation
  tmp_keys_.resize(num_keys);
  for (size_t i = 0; i < num_keys; i++) {
    //指向第i个key的指针
    const char* base = keys_.data() + start_[i];
    //取得key的长度,并将内容赋值给tmp_keys_[i]
    size_t length = start_[i + 1] - start_[i];
    tmp_keys_[i] = Slice(base, length);
  }

  // Generate filter for current set of keys and append to result_.
  //记录这个filter偏移值
  filter_offsets_.push_back(result_.size());
  //生成filter数据
  policy_->CreateFilter(&tmp_keys_[0], static_cast<int>(num_keys), &result_);

  //清空,下一个filter好写入
  tmp_keys_.clear();
  keys_.clear();
  start_.clear();
}

//创建FilterBlockReader
FilterBlockReader::FilterBlockReader(const FilterPolicy* policy,
                                     const Slice& contents)
    : policy_(policy), data_(nullptr), offset_(nullptr), num_(0), base_lg_(0) {
  size_t n = contents.size();
  //filter block数据长度如果小于5(1字节data block长度基准值与4字节filter_offsets数组起始位置)字节,说明数据错误
  if (n < 5) return;  // 1 byte for base_lg_ and 4 for start of offset array
  //data block长度基准值在最后一个字节
  base_lg_ = contents[n - 1];
  //读取filter_offsets起始位置
  uint32_t last_word = DecodeFixed32(contents.data() + n - 5);
  //如果filter_offsets起始位置超过了filter block最后5字节的位置,说明数据错误
  if (last_word > n - 5) return;
  data_ = contents.data();
  //位置指向filter_offsets起始位置
  offset_ = data_ + last_word;
  //filter_offsets个数
  num_ = (n - 5 - last_word) / 4;
}

bool FilterBlockReader::KeyMayMatch(uint64_t block_offset, const Slice& key) {
  //第几个filter
  uint64_t index = block_offset >> base_lg_;
  if (index < num_) {
    //start指向filter_offsets_中第index个元素开始位置
    //limit指向filter_offsets_中第index+1个元素开始位置,最后一个元素会读取到offset_这个值
    uint32_t start = DecodeFixed32(offset_ + index * 4);
    uint32_t limit = DecodeFixed32(offset_ + index * 4 + 4);
    //保证limit的值合法
    if (start <= limit && limit <= static_cast<size_t>(offset_ - data_)) {
      //取出filter数据做匹配
      Slice filter = Slice(data_ + start, limit - start);
      return policy_->KeyMayMatch(key, filter);
    } else if (start == limit) {
      // Empty filters do not match any keys
      return false;
    }
  }
  //错误情况,返回true,可能存在
  return true;  // Errors are treated as potential matches
}

}  // namespace leveldb
