// Copyright (c) 2011 The LevelDB Authors. All rights reserved.
// Use of this source code is governed by a BSD-style license that can be
// found in the LICENSE file. See the AUTHORS file for names of contributors.

#include "leveldb/table_builder.h"

#include <cassert>

#include "leveldb/comparator.h"
#include "leveldb/env.h"
#include "leveldb/filter_policy.h"
#include "leveldb/options.h"
#include "table/block_builder.h"
#include "table/filter_block.h"
#include "table/format.h"
#include "util/coding.h"
#include "util/crc32c.h"

namespace leveldb {

struct TableBuilder::Rep {
  Rep(const Options& opt, WritableFile* f)
      : options(opt),
        index_block_options(opt),
        file(f),
        offset(0),           //偏移量从0开始,文件是空的
        data_block(&options),
        index_block(&index_block_options),
        num_entries(0),
        closed(false),
        filter_block(opt.filter_policy == nullptr
                         ? nullptr
                         : new FilterBlockBuilder(opt.filter_policy)),
        //空文件 还不需要生成index
        pending_index_entry(false) {  
    //将data index block 重启点间隔设置为1 
    //因为data index block 中的key是比较的上一个block的最后一个key与下一个block的第一个key
    //不需要前缀压缩
    index_block_options.block_restart_interval = 1;
  }

  //外部传入的选项参数
  Options options;
  //外部传入的data index block选项参数
  Options index_block_options;
  //文件指针
  WritableFile* file;
  //文件写偏移量
  uint64_t offset;
  //写入状态
  Status status;
  //构建data block (k-v)
  BlockBuilder data_block;
  //构建data index block (index)
  BlockBuilder index_block;
  //最后写入的key
  std::string last_key;
  //总共写入数据条数
  int64_t num_entries;
  //是否已关闭
  bool closed;  // Either Finish() or Abandon() has been called.

  //构建filter block
  FilterBlockBuilder* filter_block;

  // We do not emit the index entry for a block until we have seen the
  // first key for the next data block.  This allows us to use shorter
  // keys in the index block.  For example, consider a block boundary
  // between the keys "the quick brown fox" and "the who".  We can use
  // "the r" as the key for the index block entry since it is >= all
  // entries in the first block and < all entries in subsequent
  // blocks.
  //
  // Invariant: r->pending_index_entry is true only if data_block is empty.
  // 将index的生成延迟到下一个block开始写入时,可以产生更短的index-key
  // 如:block_1 最后一个key:"the quick brown fox"  block_2 第一个key:"the who" 
  // 只需要生成"the r"
  // pending_index_entry=true 表示需要生成index了,即已经切换了新的block写入了
  bool pending_index_entry;
  // 记录还没有生成index的data block的handle(文件offset+size)
  BlockHandle pending_handle;  // Handle to add to index block
  // 如果开启压缩 这里记录了压缩后的数据
  std::string compressed_output;
};

TableBuilder::TableBuilder(const Options& options, WritableFile* file)
    : rep_(new Rep(options, file)) {
  if (rep_->filter_block != nullptr) {
    rep_->filter_block->StartBlock(0);
  }
}

TableBuilder::~TableBuilder() {
  assert(rep_->closed);  // Catch errors where caller forgot to call Finish()
  delete rep_->filter_block;
  delete rep_;
}

//动态更改参数
Status TableBuilder::ChangeOptions(const Options& options) {
  // Note: if more fields are added to Options, update
  // this function to catch changes that should not be allowed to
  // change in the middle of building a Table.
  if (options.comparator != rep_->options.comparator) {
    return Status::InvalidArgument("changing comparator while building table");
  }

  // Note that any live BlockBuilders point to rep_->options and therefore
  // will automatically pick up the updated options.
  rep_->options = options;
  rep_->index_block_options = options;
  //data index block的重启点间隔始终为1 不需要前缀压缩
  rep_->index_block_options.block_restart_interval = 1;
  return Status::OK();
}

void TableBuilder::Add(const Slice& key, const Slice& value) {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->num_entries > 0) {
    //必须有序添加
    assert(r->options.comparator->Compare(key, Slice(r->last_key)) > 0);
  }

  //需要添加index
  //说明此时是新的block的第一条数据添加
  if (r->pending_index_entry) {
    assert(r->data_block.empty());
    //当前key与最后插入的key(上一个block的最后一个key),找到一个较短的key 
    r->options.comparator->FindShortestSeparator(&r->last_key, key);
    //编码上一个block的handle(offset+size) 作为value 并添加到data index block中
    std::string handle_encoding;
    r->pending_handle.EncodeTo(&handle_encoding);
    r->index_block.Add(r->last_key, Slice(handle_encoding));
    //下一次不需要添加index了
    r->pending_index_entry = false;
  }

  //如果需要构建 filter block 就将key添加进去
  if (r->filter_block != nullptr) {
    r->filter_block->AddKey(key);
  }

  //将last_key设置为本次添加的key
  r->last_key.assign(key.data(), key.size());
  //更新总共写入数据条数
  r->num_entries++;
  //添加到data block中(实际数据写入)
  r->data_block.Add(key, value);

  // 如果block占用空间超过参数设置的block大小,刷入到磁盘
  const size_t estimated_block_size = r->data_block.CurrentSizeEstimate();
  if (estimated_block_size >= r->options.block_size) {
    Flush();
  }
}

void TableBuilder::Flush() {
  Rep* r = rep_;
  assert(!r->closed);
  if (!ok()) return;
  if (r->data_block.empty()) return;
  assert(!r->pending_index_entry);
  WriteBlock(&r->data_block, &r->pending_handle);
  if (ok()) {
    //写入data block成功
    //下一次添加需要构建index了
    r->pending_index_entry = true;
    //刷入文件
    r->status = r->file->Flush();
  }

  if (r->filter_block != nullptr) {
    r->filter_block->StartBlock(r->offset);
  }
}

void TableBuilder::WriteBlock(BlockBuilder* block, BlockHandle* handle) {
  // File format contains a sequence of blocks where each block has:    每一个block都由如下格式
  //    block_data: uint8[n]                                            数据
  //    type: uint8                                                     是否压缩
  //    crc: uint32                                                     校验码
  assert(ok());
  Rep* r = rep_;
  //获取block数据
  Slice raw = block->Finish();

  Slice block_contents;
  CompressionType type = r->options.compression;
  // TODO(postrelease): Support more compression options: zlib?
  switch (type) {
    case kNoCompression:
      block_contents = raw;
      break;

    //压缩
    case kSnappyCompression: {
      // 如果压缩率低于12.5%,放弃压缩
      std::string* compressed = &r->compressed_output;
      if (port::Snappy_Compress(raw.data(), raw.size(), compressed) &&
          compressed->size() < raw.size() - (raw.size() / 8u)) {
        block_contents = *compressed;
      } else {
        // Snappy not supported, or compressed less than 12.5%, so just
        // store uncompressed form
        block_contents = raw;
        type = kNoCompression;
      }
      break;
    }
  }

  //写入数据
  WriteRawBlock(block_contents, type, handle);
  //清空压缩数据
  r->compressed_output.clear();
  //重置block builder,以便写入下一个block写入
  block->Reset();
}

void TableBuilder::WriteRawBlock(const Slice& block_contents,
                                 CompressionType type, BlockHandle* handle) {
  Rep* r = rep_;
  //记录当前写入block的位置及大小(handle)
  handle->set_offset(r->offset);
  handle->set_size(block_contents.size());
  //数据追加到文件中(buffer)
  r->status = r->file->Append(block_contents);
  if (r->status.ok()) {
    //成功
    //则追加block尾部到文件中 type+crc
    char trailer[kBlockTrailerSize];
    trailer[0] = type;
    uint32_t crc = crc32c::Value(block_contents.data(), block_contents.size());
    crc = crc32c::Extend(crc, trailer, 1);  // Extend crc to cover block type
    EncodeFixed32(trailer + 1, crc32c::Mask(crc));
    r->status = r->file->Append(Slice(trailer, kBlockTrailerSize));
    if (r->status.ok()) {
      //更新文件写入偏移量 下一个block从这里开始
      r->offset += block_contents.size() + kBlockTrailerSize;
    }
  }
}

Status TableBuilder::status() const { return rep_->status; }

Status TableBuilder::Finish() {
  Rep* r = rep_;
  Flush();
  assert(!r->closed);
  r->closed = true;

  BlockHandle filter_block_handle, metaindex_block_handle, index_block_handle;

  // 写filter block
  // Write filter block
  if (ok() && r->filter_block != nullptr) {
    WriteRawBlock(r->filter_block->Finish(), kNoCompression,
                  &filter_block_handle);
  }

  // 写meta index block 
  // Write meta index block
  if (ok()) {
    BlockBuilder meta_index_block(&r->options);
    if (r->filter_block != nullptr) {
      // Add mapping from "filter.Name" to location of filter data
      // 将filter 信息写入 meta index block
      std::string key = "filter.";
      key.append(r->options.filter_policy->Name());
      std::string handle_encoding;
      filter_block_handle.EncodeTo(&handle_encoding);
      meta_index_block.Add(key, handle_encoding);
    }

    //写入 meta index block 数据
    // TODO(postrelease): Add stats and other meta blocks
    WriteBlock(&meta_index_block, &metaindex_block_handle);
  }

  // 写 data index block
  // Write index block
  if (ok()) {
    if (r->pending_index_entry) {
      //如果需要添加index 此时可能是最后一个block刚好写满
      //找到一个比最后一个key大的较短key添加到data index block中
      r->options.comparator->FindShortSuccessor(&r->last_key);
      std::string handle_encoding;
      r->pending_handle.EncodeTo(&handle_encoding);
      r->index_block.Add(r->last_key, Slice(handle_encoding));
      r->pending_index_entry = false;
    }
    //写入data index block
    WriteBlock(&r->index_block, &index_block_handle);
  }

  // 写文件footer
  // Write footer
  if (ok()) {
    Footer footer;
    //写入meta index block 位置(offset +size )
    footer.set_metaindex_handle(metaindex_block_handle);
    //写入data index block 位置(offset +size )
    footer.set_index_handle(index_block_handle);
    std::string footer_encoding;
    footer.EncodeTo(&footer_encoding);
    //footer 追加到文件中
    r->status = r->file->Append(footer_encoding);
    if (r->status.ok()) {
      //更新文件写入偏移值
      r->offset += footer_encoding.size();
    }
  }
  return r->status;
}

void TableBuilder::Abandon() {
  //丢弃这个
  Rep* r = rep_;
  assert(!r->closed);
  r->closed = true;
}

uint64_t TableBuilder::NumEntries() const { return rep_->num_entries; }

uint64_t TableBuilder::FileSize() const { return rep_->offset; }

}  // namespace leveldb
