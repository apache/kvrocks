/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include "types/cuckoo_filter_page.h"

#include <gtest/gtest.h>

#include <limits>
#include <memory>
#include <string>

#include "common/encoding.h"
#include "storage/redis_metadata.h"
#include "test_base.h"
#include "types/redis_cuckoo_chain.h"

class RedisCuckooPageCacheTest : public TestBase {
 public:
  RedisCuckooPageCacheTest(const RedisCuckooPageCacheTest &) = delete;
  RedisCuckooPageCacheTest &operator=(const RedisCuckooPageCacheTest &) = delete;
  RedisCuckooPageCacheTest(RedisCuckooPageCacheTest &&) = delete;
  RedisCuckooPageCacheTest &operator=(RedisCuckooPageCacheTest &&) = delete;

 protected:
  explicit RedisCuckooPageCacheTest()
      : TestBase(), db_(std::make_unique<redis::Database>(storage_.get(), "cuckoo_ns")) {}

  ~RedisCuckooPageCacheTest() override { db_.reset(); }

  void SetUp() override {
    const ::testing::TestInfo *const test_info = ::testing::UnitTest::GetInstance()->current_test_info();
    ns_key_ = db_->AppendNamespacePrefix(std::string("cf_page_test_") + test_info->name());
  }

  static CuckooChainMetadata makeMetadata(uint8_t bucket_size, uint64_t version = 1,
                                          uint32_t page_size = kCuckooFilterDefaultPageSize) {
    CuckooChainMetadata metadata(false);
    metadata.version = version;
    metadata.size = 0;
    metadata.base_capacity = 2;
    metadata.bucket_size = bucket_size;
    metadata.max_iterations = 500;
    metadata.expansion = 0;
    metadata.n_filters = 1;
    metadata.num_deleted_items = 0;
    metadata.page_size = page_size;
    return metadata;
  }

  std::string makePageKey(const CuckooChainMetadata &metadata, uint16_t filter_index, uint32_t page_index) const {
    std::string sub_key;
    PutFixed16(&sub_key, filter_index);
    PutFixed32(&sub_key, page_index);
    return InternalKey(ns_key_, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
  }

  rocksdb::Status readPage(const std::string &page_key, std::string *value) {
    return storage_->Get(*ctx_, ctx_->GetReadOptions(), storage_->GetCFHandle(ColumnFamilyID::PrimarySubkey), page_key,
                         value);
  }

  void writePage(const std::string &page_key, const std::string &value) {
    auto batch = storage_->GetWriteBatchBase();
    auto s = batch->Put(page_key, value);
    ASSERT_TRUE(s.ok()) << s.ToString();
    s = storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  void commitBatch(rocksdb::WriteBatchBase *batch) {
    auto s = storage_->Write(*ctx_, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
    ASSERT_TRUE(s.ok()) << s.ToString();
  }

  std::unique_ptr<redis::Database> db_;
  std::string ns_key_;
};

TEST(CuckooDumpPayloadTest, BuilderUsesRelativeSkippedPageIndexes) {
  std::string data = "prefix";
  redis::CuckooDumpPayload::Builder builder(5, &data);
  builder.AppendPage(rocksdb::Slice("page"));
  ASSERT_TRUE(builder.AppendSkippedPageIndex(7).ok());
  EXPECT_TRUE(builder.AppendSkippedPageIndex(static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 6)
                  .IsInvalidArgument());
  builder.Encode();

  redis::CuckooDumpPayload decoded;
  ASSERT_TRUE(redis::CuckooDumpPayload::Builder::Decode(data, &decoded).ok());
  EXPECT_EQ(decoded.page_bytes.ToString(), "prefixpage");
  EXPECT_EQ(decoded.page_index, 5U);
  ASSERT_EQ(decoded.skipped_page_indexes, std::vector<uint32_t>({2U}));
}

TEST(CuckooDumpPayloadTest, EmptyPayloadContainsMetadata) {
  std::string data;
  redis::CuckooDumpPayload::Builder builder(42, &data);
  builder.Encode();

  ASSERT_EQ(data.size(), 2 * sizeof(uint32_t));
  redis::CuckooDumpPayload decoded;
  ASSERT_TRUE(redis::CuckooDumpPayload::Builder::Decode(data, &decoded).ok());
  EXPECT_TRUE(decoded.page_bytes.empty());
  EXPECT_TRUE(decoded.skipped_page_indexes.empty());
  EXPECT_EQ(decoded.page_index, 42U);
}

TEST(CuckooDumpPayloadTest, BuilderEncodesMultiplePagesAndRelativeIndexes) {
  std::string data;
  redis::CuckooDumpPayload::Builder builder(100, &data);
  builder.AppendPage(rocksdb::Slice("page-1page-2"));
  ASSERT_TRUE(builder.AppendSkippedPageIndex(101).ok());
  ASSERT_TRUE(builder.AppendSkippedPageIndex(105).ok());
  builder.Encode();

  redis::CuckooDumpPayload decoded;
  ASSERT_TRUE(redis::CuckooDumpPayload::Builder::Decode(data, &decoded).ok());
  EXPECT_EQ(decoded.page_bytes.ToString(), "page-1page-2");
  EXPECT_EQ(decoded.page_index, 100U);
  ASSERT_EQ(decoded.skipped_page_indexes, std::vector<uint32_t>({1U, 5U}));
}

TEST(CuckooDumpPayloadTest, AppendSkippedPageIndexAcceptsUint32Boundary) {
  std::string data;
  redis::CuckooDumpPayload::Builder builder(5, &data);
  ASSERT_TRUE(builder.AppendSkippedPageIndex(static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 5).ok());
  EXPECT_TRUE(builder.AppendSkippedPageIndex(static_cast<size_t>(std::numeric_limits<uint32_t>::max()) + 6)
                  .IsInvalidArgument());
}

TEST(CuckooDumpPayloadTest, DecodeRejectsTruncatedAndInvalidPayloads) {
  redis::CuckooDumpPayload decoded;
  EXPECT_TRUE(redis::CuckooDumpPayload::Builder::Decode(rocksdb::Slice("short"), &decoded).IsCorruption());

  std::string invalid;
  PutFixed32(&invalid, 1);  // One skipped index, but no index bytes are present.
  PutFixed32(&invalid, 0);
  EXPECT_TRUE(redis::CuckooDumpPayload::Builder::Decode(invalid, &decoded).IsCorruption());
}

TEST_F(RedisCuckooPageCacheTest, TryInsertWritesSmallPage) {
  auto metadata = makeMetadata(4);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  bool inserted = false;
  auto s = pages.TryInsertInBucket(0, 2, 1, 11, &inserted);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(inserted);

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(makePageKey(metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), 8);
  EXPECT_EQ(page.substr(0, 4), std::string(4, 0));
  EXPECT_EQ(static_cast<uint8_t>(page[4]), 11);
  EXPECT_EQ(page.substr(5, 3), std::string(3, 0));
}

TEST_F(RedisCuckooPageCacheTest, TryInsertWritesLastPartialPage) {
  auto metadata = makeMetadata(4);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  bool inserted = false;
  auto s = pages.TryInsertInBucket(0, 513, 512, 33, &inserted);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(inserted);

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(makePageKey(metadata, 0, 1), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), 4);
  EXPECT_EQ(static_cast<uint8_t>(page[0]), 33);
  EXPECT_EQ(page.substr(1, 3), std::string(3, 0));

  s = readPage(makePageKey(metadata, 0, 0), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, PrefetchBucketsOnSamePageLoadsCachedPage) {
  auto metadata = makeMetadata(2);
  auto page_key = makePageKey(metadata, 0, 0);
  writePage(page_key, std::string{static_cast<char>(11), static_cast<char>(12), static_cast<char>(21),
                                  static_cast<char>(22), 0, 0, 0, 0});
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  auto s = pages.PrefetchBuckets(0, 4, 0, 1);
  ASSERT_TRUE(s.ok()) << s.ToString();
  writePage(page_key, std::string(8, static_cast<char>(99)));

  uint8_t fingerprint = 0;
  s = pages.GetBucketSlot(0, 4, 0, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 11);
  s = pages.GetBucketSlot(0, 4, 1, 1, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 22);
}

TEST_F(RedisCuckooPageCacheTest, PrefetchBucketsOnDifferentPagesLoadsBothPages) {
  auto metadata = makeMetadata(4);
  auto page0_key = makePageKey(metadata, 0, 0);
  auto page1_key = makePageKey(metadata, 0, 1);
  std::string page0(metadata.page_size, 0);
  std::string page1(metadata.page_size, 0);
  page0[0] = static_cast<char>(44);
  page1[0] = static_cast<char>(55);
  writePage(page0_key, page0);
  writePage(page1_key, page1);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  auto s = pages.PrefetchBuckets(0, 1025, 0, 512);
  ASSERT_TRUE(s.ok()) << s.ToString();
  writePage(page0_key, std::string(metadata.page_size, static_cast<char>(99)));
  writePage(page1_key, std::string(metadata.page_size, static_cast<char>(88)));

  uint8_t fingerprint = 0;
  s = pages.GetBucketSlot(0, 1025, 0, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 44);
  s = pages.GetBucketSlot(0, 1025, 512, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 55);
}

TEST_F(RedisCuckooPageCacheTest, ScanPageBytesReadsAcrossPagesAndFillsMissingPages) {
  auto metadata = makeMetadata(1, 1, 4);
  auto page_key = makePageKey(metadata, 0, 1);
  writePage(page_key,
            std::string{static_cast<char>(10), static_cast<char>(11), static_cast<char>(12), static_cast<char>(13)});
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  std::string data;
  auto s = pages.ScanPageBytes({{0, 12, 0, 3}}, 0, &data);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(data.size(), 20U);
  EXPECT_EQ(data.substr(0, 4), std::string({10, 11, 12, 13}));
  EXPECT_EQ(DecodeFixed32(data.data() + 4), 0U);
  EXPECT_EQ(DecodeFixed32(data.data() + 8), 2U);
  EXPECT_EQ(DecodeFixed32(data.data() + 12), 2U);
  EXPECT_EQ(DecodeFixed32(data.data() + 16), 0U);
}

TEST_F(RedisCuckooPageCacheTest, ScanPageBytesReadsExistingPages) {
  auto metadata = makeMetadata(1, 1, 4);
  writePage(makePageKey(metadata, 0, 0), std::string({1, 2, 3, 4}));
  writePage(makePageKey(metadata, 0, 1), std::string({5, 6, 7, 8}));
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  std::string data;
  auto s = pages.ScanPageBytes({{0, 8, 0, 2}}, 0, &data);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(data.size(), 16U);
  EXPECT_EQ(data.substr(0, 8), std::string({1, 2, 3, 4, 5, 6, 7, 8}));
  EXPECT_EQ(DecodeFixed32(data.data() + 8), 0U);
  EXPECT_EQ(DecodeFixed32(data.data() + 12), 0U);
}

TEST_F(RedisCuckooPageCacheTest, ScanPageBytesReadsAcrossSubFilters) {
  auto metadata = makeMetadata(1, 1, 4);
  writePage(makePageKey(metadata, 0, 0), std::string({1, 2, 3, 4}));
  writePage(makePageKey(metadata, 1, 0), std::string({5, 6, 7, 8}));

  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);
  std::string data;
  auto s = pages.ScanPageBytes({{0, 8, 0, 2}, {1, 8, 0, 2}}, 10, &data);
  ASSERT_TRUE(s.ok()) << s.ToString();

  redis::CuckooDumpPayload payload;
  s = redis::CuckooDumpPayload::Builder::Decode(data, &payload);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(payload.page_index, 10U);
  EXPECT_EQ(payload.page_bytes.ToString(), std::string({1, 2, 3, 4, 5, 6, 7, 8}));
  EXPECT_EQ(payload.skipped_page_indexes, std::vector<uint32_t>({1, 3}));
}

TEST_F(RedisCuckooPageCacheTest, ScanPageBytesRejectsCorruptedPageSize) {
  auto metadata = makeMetadata(1, 1, 4);
  writePage(makePageKey(metadata, 0, 0), std::string({1, 2, 3}));
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  std::string data;
  auto s = pages.ScanPageBytes({{0, 4, 0, 1}}, 0, &data);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, ScanPageBytesRejectsInvalidRange) {
  auto metadata = makeMetadata(2, 1, 4);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  std::string data;
  auto s = pages.ScanPageBytes({{0, 2, 3, 2}}, 3, &data);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, ScanPageBytesBatchesLargePageRanges) {
  auto metadata = makeMetadata(1, 1, 1);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  std::string data;
  auto s = pages.ScanPageBytes({{0, 8193, 0, 8193}}, 0, &data);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(data.size(), 8193U * sizeof(uint32_t) + 2 * sizeof(uint32_t));
  EXPECT_EQ(DecodeFixed32(data.data() + data.size() - 2 * sizeof(uint32_t)), 8193U);
  EXPECT_EQ(DecodeFixed32(data.data() + data.size() - sizeof(uint32_t)), 0U);
  EXPECT_EQ(DecodeFixed32(data.data()), 0U);
  EXPECT_EQ(DecodeFixed32(data.data() + (8193U - 1) * sizeof(uint32_t)), 8192U);
}

TEST_F(RedisCuckooPageCacheTest, NonDefaultPageSizeControlsBucketMapping) {
  auto metadata = makeMetadata(4, 1, 8);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  bool inserted = false;
  auto s = pages.TryInsertInBucket(0, 3, 2, 66, &inserted);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(inserted);

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(makePageKey(metadata, 0, 0), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();

  s = readPage(makePageKey(metadata, 0, 1), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), 4);
  EXPECT_EQ(static_cast<uint8_t>(page[0]), 66);
  EXPECT_EQ(page.substr(1, 3), std::string(3, 0));
}

TEST_F(RedisCuckooPageCacheTest, SetBucketSlotWritesOnlyTargetSlot) {
  auto metadata = makeMetadata(4);
  std::string expected(16, static_cast<char>(7));
  writePage(makePageKey(metadata, 0, 0), expected);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  auto s = pages.SetBucketSlot(0, 4, 2, 1, 88);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  expected[9] = static_cast<char>(88);
  std::string page;
  s = readPage(makePageKey(metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(page, expected);
}

TEST_F(RedisCuckooPageCacheTest, SkipsWritingNewPageWhenItBecomesEmpty) {
  auto metadata = makeMetadata(1);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  auto s = pages.SetBucketSlot(0, 1, 0, 0, 88);
  ASSERT_TRUE(s.ok()) << s.ToString();
  s = pages.SetBucketSlot(0, 1, 0, 0, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(batch->GetWriteBatch()->Count(), 0);
}

TEST_F(RedisCuckooPageCacheTest, DeletesExistingPageWhenItBecomesEmpty) {
  auto metadata = makeMetadata(1);
  auto page_key = makePageKey(metadata, 0, 0);
  writePage(page_key, std::string{static_cast<char>(88)});
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  auto s = pages.SetBucketSlot(0, 1, 0, 0, 0);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(batch->GetWriteBatch()->Count(), 1);
  commitBatch(batch.Get());

  std::string page;
  s = readPage(page_key, &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, InvalidBucketAndSlotArguments) {
  auto metadata = makeMetadata(4);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  uint8_t fingerprint = 0;
  auto s = pages.GetBucketSlot(0, 2, 0, 4, &fingerprint);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();

  s = pages.SetBucketSlot(0, 2, 0, 4, 11);
  EXPECT_TRUE(s.IsInvalidArgument()) << s.ToString();

  bool inserted = true;
  s = pages.TryInsertInBucket(0, 2, 2, 11, &inserted);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();

  s = pages.TryInsertInBucket(0, 0, 0, 11, &inserted);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(makePageKey(metadata, 0, 0), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, OversizedPageReturnsCorruption) {
  auto metadata = makeMetadata(4);
  writePage(makePageKey(metadata, 0, 0), std::string(9, static_cast<char>(1)));
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  uint8_t fingerprint = 0;
  auto s = pages.GetBucketSlot(0, 2, 0, 0, &fingerprint);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();

  std::string page;
  s = readPage(makePageKey(metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(page, std::string(9, static_cast<char>(1)));
}

TEST_F(RedisCuckooPageCacheTest, UndersizedPageReturnsCorruption) {
  auto metadata = makeMetadata(4);
  auto page_key = makePageKey(metadata, 0, 0);
  writePage(page_key, std::string{static_cast<char>(1), static_cast<char>(2), static_cast<char>(3)});
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  uint8_t fingerprint = 0;
  auto s = pages.GetBucketSlot(0, 2, 0, 0, &fingerprint);
  EXPECT_TRUE(s.IsCorruption()) << s.ToString();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(page_key, &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(page, std::string({static_cast<char>(1), static_cast<char>(2), static_cast<char>(3)}));
}

TEST_F(RedisCuckooPageCacheTest, DirtyPagesAreDiscardedWithoutWriteBack) {
  auto metadata = makeMetadata(4);
  {
    redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                                 metadata.bucket_size, metadata.page_size);
    bool inserted = false;
    auto s = pages.TryInsertInBucket(0, 2, 0, 11, &inserted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(inserted);
  }

  std::string page;
  auto s = readPage(makePageKey(metadata, 0, 0), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, DiscardCachedPagesDropsDirtyPages) {
  auto metadata = makeMetadata(4);
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  bool inserted = false;
  auto s = pages.TryInsertInBucket(0, 2, 0, 11, &inserted);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_TRUE(inserted);

  pages.DiscardCachedPages();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(makePageKey(metadata, 0, 0), &page);
  EXPECT_TRUE(s.IsNotFound()) << s.ToString();
}

TEST_F(RedisCuckooPageCacheTest, DiscardCleanPagesPreservesDirtyPages) {
  auto metadata = makeMetadata(4);
  auto dirty_page_key = makePageKey(metadata, 0, 0);
  auto clean_page_key = makePageKey(metadata, 1, 0);
  writePage(dirty_page_key, std::string(8, static_cast<char>(1)));
  writePage(clean_page_key, std::string(8, static_cast<char>(2)));
  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), metadata.version,
                               metadata.bucket_size, metadata.page_size);

  auto s = pages.SetBucketSlot(0, 2, 0, 0, 11);
  ASSERT_TRUE(s.ok()) << s.ToString();
  uint8_t fingerprint = 0;
  s = pages.GetBucketSlot(1, 2, 0, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(fingerprint, 2);

  pages.DiscardCleanPages();
  writePage(dirty_page_key, std::string(8, static_cast<char>(3)));
  writePage(clean_page_key, std::string(8, static_cast<char>(4)));

  s = pages.GetBucketSlot(0, 2, 0, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 11);
  s = pages.GetBucketSlot(1, 2, 0, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 4);

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(dirty_page_key, &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(static_cast<uint8_t>(page[0]), 11);
  s = readPage(clean_page_key, &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(static_cast<uint8_t>(page[0]), 4);
}

TEST_F(RedisCuckooPageCacheTest, PageKeyUsesMetadataVersion) {
  auto old_metadata = makeMetadata(4, 100);
  auto new_metadata = makeMetadata(4, 101);
  {
    redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), old_metadata.version,
                                 old_metadata.bucket_size, old_metadata.page_size);
    bool inserted = false;
    auto s = pages.TryInsertInBucket(0, 2, 0, 11, &inserted);
    ASSERT_TRUE(s.ok()) << s.ToString();
    ASSERT_TRUE(inserted);

    auto batch = storage_->GetWriteBatchBase();
    s = pages.WriteBackDirtyPages(batch.Get());
    ASSERT_TRUE(s.ok()) << s.ToString();
    commitBatch(batch.Get());
  }

  redis::CuckooPageCache pages(storage_.get(), *ctx_, ns_key_, storage_->IsSlotIdEncoded(), new_metadata.version,
                               new_metadata.bucket_size, new_metadata.page_size);
  uint8_t fingerprint = 0;
  auto s = pages.GetBucketSlot(0, 2, 0, 0, &fingerprint);
  ASSERT_TRUE(s.ok()) << s.ToString();
  EXPECT_EQ(fingerprint, 0);
  s = pages.SetBucketSlot(0, 2, 0, 0, 22);
  ASSERT_TRUE(s.ok()) << s.ToString();

  auto batch = storage_->GetWriteBatchBase();
  s = pages.WriteBackDirtyPages(batch.Get());
  ASSERT_TRUE(s.ok()) << s.ToString();
  commitBatch(batch.Get());

  std::string page;
  s = readPage(makePageKey(old_metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), 8);
  EXPECT_EQ(static_cast<uint8_t>(page[0]), 11);

  s = readPage(makePageKey(new_metadata, 0, 0), &page);
  ASSERT_TRUE(s.ok()) << s.ToString();
  ASSERT_EQ(page.size(), 8);
  EXPECT_EQ(static_cast<uint8_t>(page[0]), 22);
}
