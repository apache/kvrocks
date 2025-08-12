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

#include <cstdint>

#include "storage/redis_db.h"
#include "storage/redis_metadata.h"

namespace redis {

enum class TSSubkeyType : uint8_t {
  CHUNK = 0,
  LABEL = 1,
  DOWNSTREAM = 2,
};

// Enum prefix for new CF.
enum class IndexKeyType : uint8_t {
  TS_LABEL = 0,
};

enum class TSAggregatorType : uint8_t {
  AVG = 0,
  SUM = 1,
  MIN = 2,
  MAX = 3,
  RANGE = 4,
  COUNT = 5,
  FIRST = 6,
  LAST = 7,
  STD_P = 8,
  STD_S = 9,
  VAR_P = 10,
  VAR_S = 11,
};

struct TSDownStreamMeta {
  TSAggregatorType aggregator;
  uint64_t bucket_duration;
  uint64_t alignment;
  uint64_t latest_bucket_idx;

  // store auxiliary info for each aggregator.
  // e.g. for avg, need to store sum and count: u64_auxs={count}, f64_auxs={sum}
  std::vector<uint64_t> u64_auxs;
  std::vector<double> f64_auxs;

  TSDownStreamMeta() = default;
  TSDownStreamMeta(TSAggregatorType aggregator, uint64_t bucket_duration, uint64_t alignment,
                   uint64_t latest_bucket_idx)
      : aggregator(aggregator),
        bucket_duration(bucket_duration),
        alignment(alignment),
        latest_bucket_idx(latest_bucket_idx) {}

  void Encode(std::string *dst) const;
  rocksdb::Status Decode(Slice *input);
};

struct TSRevLabelKey {
  Slice ns;
  Slice label_key;
  Slice label_value;
  Slice user_key;

  TSRevLabelKey(Slice ns, Slice label_key, Slice label_value, Slice user_key = Slice())
      : ns(ns), label_key(label_key), label_value(label_value), user_key(user_key) {}

  [[nodiscard]] std::string Encode() const;
};

class TimeSeries : public SubKeyScanner {
 public:
  TimeSeries(engine::Storage *storage, const std::string &ns) : SubKeyScanner(storage, ns) {}

 private:
  std::string internalKeyFromChunkID(const std::string &ns_key, const TimeSeriesMetadata &metadata, uint64_t id) const;
  std::string internalKeyFromLabelKey(const std::string &ns_key, const TimeSeriesMetadata &metadata,
                                      Slice label_key) const;
  std::string internalKeyFromDownstreamKey(const std::string &ns_key, const TimeSeriesMetadata &metadata,
                                           Slice downstream_key) const;
};

}  // namespace redis
