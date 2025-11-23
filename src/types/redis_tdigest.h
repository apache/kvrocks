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

#pragma once

#include <rocksdb/db.h>
#include <rocksdb/slice.h>
#include <rocksdb/status.h>

#include <optional>
#include <vector>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "storage/storage.h"
#include "tdigest.h"

namespace redis {

// TODO: It should be replaced by a iteration of the rocksdb iterator
class DummyCentroids {
 public:
  class BaseIterator {
   public:
    virtual ~BaseIterator() = default;
    virtual bool Next() = 0;
    virtual bool Prev() = 0;
    virtual bool Valid() const = 0;
    virtual std::unique_ptr<BaseIterator> Clone() const = 0;
    virtual StatusOr<Centroid> GetCentroid() const = 0;
  };

  DummyCentroids(const TDigestMetadata& meta_data, const std::vector<Centroid>& centroids)
      : meta_data_(meta_data), centroids_(centroids) {}
  class Iterator : public BaseIterator {
   public:
    Iterator(std::vector<Centroid>::const_iterator&& iter, const std::vector<Centroid>& centroids)
        : iter_(iter), centroids_(centroids) {}
    std::unique_ptr<BaseIterator> Clone() const override {
      if (iter_ != centroids_.cend()) {
        return std::make_unique<Iterator>(std::next(centroids_.cbegin(), std::distance(centroids_.cbegin(), iter_)),
                                          centroids_);
      }
      return std::make_unique<Iterator>(centroids_.cend(), centroids_);
    }
    bool Next() override {
      if (Valid()) {
        std::advance(iter_, 1);
      }
      return iter_ != centroids_.cend();
    }

    // The Prev function can only be called for item is not cend,
    // because we must guarantee the iterator to be inside the valid range before iteration.
    bool Prev() override {
      if (Valid() && iter_ != centroids_.cbegin()) {
        std::advance(iter_, -1);
      }
      return Valid();
    }
    bool Valid() const override { return iter_ != centroids_.cend(); }
    StatusOr<Centroid> GetCentroid() const override {
      if (iter_ == centroids_.cend()) {
        return {::Status::NotOK, "invalid iterator during decoding tdigest centroid"};
      }
      return *iter_;
    }

   private:
    std::vector<Centroid>::const_iterator iter_;
    const std::vector<Centroid>& centroids_;
  };

  class ReverseIterator final : public BaseIterator {
   public:
    ReverseIterator(std::vector<Centroid>::const_reverse_iterator&& iter, const std::vector<Centroid>& centroids)
        : iter_(iter), centroids_(centroids) {}
    std::unique_ptr<BaseIterator> Clone() const override {
      if (iter_ != centroids_.crend()) {
        return std::make_unique<ReverseIterator>(
            std::next(centroids_.crbegin(), std::distance(centroids_.crbegin(), iter_)), centroids_);
      }
      return std::make_unique<ReverseIterator>(centroids_.crend(), centroids_);
    }
    bool Next() override {
      if (Valid()) {
        std::advance(iter_, 1);
      }
      return iter_ != centroids_.crend();
    }

    bool Prev() override {
      if (Valid() && iter_ != centroids_.crbegin()) {
        std::advance(iter_, -1);
      }
      return Valid();
    }
    bool Valid() const override { return iter_ != centroids_.crend(); }
    StatusOr<Centroid> GetCentroid() const override {
      if (iter_ == centroids_.crend()) {
        return {::Status::NotOK, "invalid iterator during decoding tdigest centroid"};
      }
      return *iter_;
    }

   private:
    std::vector<Centroid>::const_reverse_iterator iter_;
    const std::vector<Centroid>& centroids_;
  };

  std::unique_ptr<BaseIterator> Begin(const bool reverse = false) const {
    if (reverse) {
      return std::make_unique<ReverseIterator>(centroids_.crbegin(), centroids_);
    }
    return std::make_unique<Iterator>(centroids_.cbegin(), centroids_);
  }
  std::unique_ptr<BaseIterator> End(const bool reverse = false) const {
    if (centroids_.empty()) {
      if (reverse) {
        return std::make_unique<ReverseIterator>(centroids_.crend(), centroids_);
      }
      return std::make_unique<Iterator>(centroids_.cend(), centroids_);
    }
    if (reverse) {
      return std::make_unique<ReverseIterator>(std::prev(centroids_.crend()), centroids_);
    }
    return std::make_unique<Iterator>(std::prev(centroids_.cend()), centroids_);
  }
  double TotalWeight() const { return static_cast<double>(meta_data_.total_weight); }
  double Min() const { return meta_data_.minimum; }
  double Max() const { return meta_data_.maximum; }
  uint64_t Size() const { return meta_data_.merged_nodes; }

 private:
  const TDigestMetadata& meta_data_;
  const std::vector<Centroid>& centroids_;
};

inline constexpr uint32_t kTDigestMaxCompression = 1000;  // limit the compression to 1k

struct CentroidWithKey {
  Centroid centroid;
  rocksdb::Slice key;
};

struct TDigestCreateOptions {
  uint32_t compression;
};

struct TDigestMergeOptions {
  uint32_t compression = 0;
  bool override_flag = false;
};

struct TDigestQuantitleResult {
  std::optional<std::vector<double>> quantiles;
};

class TDigest : public SubKeyScanner {
 public:
  using Slice = rocksdb::Slice;
  explicit TDigest(engine::Storage* storage, const std::string& ns)
      : SubKeyScanner(storage, ns), cf_handle_(storage->GetCFHandle(ColumnFamilyID::PrimarySubkey)) {}
  /**
   * @brief Create a t-digest structure.
   *
   * @param ctx The context of the operation.
   * @param digest_name The name of the t-digest.
   * @param options The options of the t-digest.
   * @param exists The output parameter to indicate whether the t-digest already exists.
   * @return rocksdb::Status
   */
  rocksdb::Status Create(engine::Context& ctx, const Slice& digest_name, const TDigestCreateOptions& options,
                         bool* exists);
  rocksdb::Status Add(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& inputs);
  rocksdb::Status Quantile(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& qs,
                           TDigestQuantitleResult* result);

  rocksdb::Status Reset(engine::Context& ctx, const Slice& digest_name);

  rocksdb::Status Merge(engine::Context& ctx, const Slice& dest_digest, const std::vector<std::string>& source_digests,
                        const TDigestMergeOptions& options);
  template <bool Reverse>
  rocksdb::Status Rank(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& inputs,
                       std::vector<int>& result);
  rocksdb::Status GetMetaData(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata);

 private:
  enum class SegmentType : uint8_t { kBuffer = 0, kCentroids = 1, kGuardFlag = 0xFF };

  rocksdb::ColumnFamilyHandle* cf_handle_;

  rocksdb::Status getMetaDataByNsKey(engine::Context& context, const Slice& digest_name, TDigestMetadata* metadata);

  rocksdb::Status appendBuffer(engine::Context& ctx, ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch,
                               const std::string& ns_key, const std::vector<double>& inputs, TDigestMetadata* metadata);

  rocksdb::Status dumpCentroids(engine::Context& ctx, const std::string& ns_key, const TDigestMetadata& metadata,
                                std::vector<Centroid>* centroids) {
    return dumpCentroidsAndBuffer(ctx, ns_key, metadata, centroids, nullptr, nullptr);
  }

  /**
   * @brief Dumps the centroids and buffer of the t-digest.
   *
   * This function reads the centroids and buffer from persistent storage and removes them from the storage.
   * @param ctx The context of the operation.
   * @param ns_key The namespace key of the t-digest.
   * @param metadata The metadata of the t-digest.
   * @param centroids The output vector to store the centroids.
   * @param buffer The output vector to store the buffer. If it is nullptr, the buffer will not be read.
   * @param clean_after_dump_batch The write batch to store the clean operations. If it is nullptr, the clean operations
   * @return rocksdb::Status
   */
  rocksdb::Status dumpCentroidsAndBuffer(engine::Context& ctx, const std::string& ns_key,
                                         const TDigestMetadata& metadata, std::vector<Centroid>* centroids,
                                         std::vector<double>* buffer,
                                         ObserverOrUniquePtr<rocksdb::WriteBatchBase>* clean_after_dump_batch);
  rocksdb::Status applyNewCentroids(ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch, const std::string& ns_key,
                                    const TDigestMetadata& metadata, const std::vector<Centroid>& centroids);

  std::string internalSegmentGuardPrefixKey(const TDigestMetadata& metadata, const std::string& ns_key,
                                            SegmentType seg) const;

  rocksdb::Status mergeNodes(engine::Context& ctx, const std::string& ns_key, TDigestMetadata* metadata);

  rocksdb::Status mergeCurrentBuffer(engine::Context& ctx, const std::string& ns_key,
                                     ObserverOrUniquePtr<rocksdb::WriteBatchBase>& batch, TDigestMetadata* metadata,
                                     const std::vector<double>* additional_buffer = nullptr,
                                     std::vector<Centroid>* dump_centroids = nullptr);
  std::string internalBufferKey(const std::string& ns_key, const TDigestMetadata& metadata) const;
  std::string internalKeyFromCentroid(const std::string& ns_key, const TDigestMetadata& metadata,
                                      const Centroid& centroid, uint32_t seq) const;
  static std::string internalValueFromCentroid(const Centroid& centroid);
  rocksdb::Status decodeCentroidFromKeyValue(const rocksdb::Slice& key, const rocksdb::Slice& value,
                                             Centroid* centroid) const;
};

template <bool Reverse>
rocksdb::Status TDigest::Rank(engine::Context& ctx, const Slice& digest_name, const std::vector<double>& inputs,
                              std::vector<int>& result) {
  auto ns_key = AppendNamespacePrefix(digest_name);
  TDigestMetadata metadata;
  {
    LockGuard guard(storage_->GetLockManager(), ns_key);

    if (auto status = getMetaDataByNsKey(ctx, ns_key, &metadata); !status.ok()) {
      return status;
    }

    if (metadata.total_observations == 0) {
      result.resize(inputs.size(), -2);
      return rocksdb::Status::OK();
    }

    if (auto status = mergeNodes(ctx, ns_key, &metadata); !status.ok()) {
      return status;
    }
  }

  std::vector<Centroid> centroids;
  if (auto status = dumpCentroids(ctx, ns_key, metadata, &centroids); !status.ok()) {
    return status;
  }

  auto dump_centroids = DummyCentroids(metadata, centroids);
  auto status = TDigestRank<Reverse>(dump_centroids, inputs, result);
  if (!status) {
    return rocksdb::Status::InvalidArgument(status.Msg());
  }
  return rocksdb::Status::OK();
}

}  // namespace redis
