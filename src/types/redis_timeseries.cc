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

#include "redis_timeseries.h"

#include <queue>

#include "commands/error_constants.h"
#include "db_util.h"

namespace redis {

// TODO: make it configurable
constexpr uint64_t kDefaultRetentionTime = 0;
constexpr uint64_t kDefaultChunkSize = 1024;
constexpr auto kDefaultChunkType = TimeSeriesMetadata::ChunkType::UNCOMPRESSED;
constexpr auto kDefaultDuplicatePolicy = TimeSeriesMetadata::DuplicatePolicy::BLOCK;

struct Reducer {
  static inline double Sum(nonstd::span<const TSSample> samples) {
    return std::accumulate(samples.begin(), samples.end(), 0.0,
                           [](double acc, const TSSample &sample) { return acc + sample.v; });
  }
  static inline double SquareSum(nonstd::span<const TSSample> samples) {
    return std::accumulate(samples.begin(), samples.end(), 0.0,
                           [](double acc, const TSSample &sample) { return acc + sample.v * sample.v; });
  }
  static inline double Min(nonstd::span<const TSSample> samples) {
    return std::min_element(samples.begin(), samples.end(),
                            [](const TSSample &a, const TSSample &b) { return a.v < b.v; })
        ->v;
  }
  static inline double Max(nonstd::span<const TSSample> samples) {
    return std::max_element(samples.begin(), samples.end(),
                            [](const TSSample &a, const TSSample &b) { return a.v < b.v; })
        ->v;
  }
  static inline double VarP(nonstd::span<const TSSample> samples) {
    auto sample_size = static_cast<double>(samples.size());
    double sum = Sum(samples);
    double square_sum = SquareSum(samples);
    return (square_sum - sum * sum / sample_size) / sample_size;
  }
  static inline double VarS(nonstd::span<const TSSample> samples) {
    if (samples.size() <= 1) return 0.0;
    auto sample_size = static_cast<double>(samples.size());
    return VarP(samples) * sample_size / (sample_size - 1);
  }
  static inline double StdP(nonstd::span<const TSSample> samples) { return std::sqrt(VarP(samples)); }

  static inline double StdS(nonstd::span<const TSSample> samples) { return std::sqrt(VarS(samples)); }
  static inline double Range(nonstd::span<const TSSample> samples) {
    if (samples.empty()) return 0.0;
    auto [min, max] = std::minmax_element(samples.begin(), samples.end(),
                                          [](const TSSample &a, const TSSample &b) { return a.v < b.v; });
    return max->v - min->v;
  }
};

std::vector<TSSample> AggregateSamplesByRangeOption(std::vector<TSSample> samples, const TSRangeOption &option) {
  const auto &aggregator = option.aggregator;
  std::vector<TSSample> res;
  if (aggregator.type == TSAggregatorType::NONE || samples.empty()) {
    res = std::move(samples);
    return res;
  }
  auto spans = aggregator.SplitSamplesToBuckets(samples);

  auto get_bucket_ts = [&](uint64_t left) -> uint64_t {
    using BucketTimestampType = TSRangeOption::BucketTimestampType;
    switch (option.bucket_timestamp_type) {
      case BucketTimestampType::Start:
        return left;
      case BucketTimestampType::End:
        return left + aggregator.bucket_duration;
      case BucketTimestampType::Mid:
        return left + aggregator.bucket_duration / 2;
      default:
        unreachable();
    }
    return 0;
  };
  res.reserve(spans.size());
  uint64_t bucket_left = aggregator.CalculateAlignedBucketLeft(samples.front().ts);
  for (size_t i = 0; i < spans.size(); i++) {
    if (option.count_limit && res.size() >= option.count_limit) {
      break;
    }
    TSSample sample;
    if (i != 0) {
      bucket_left = aggregator.CalculateAlignedBucketRight(bucket_left);
    }
    sample.ts = get_bucket_ts(bucket_left);
    if (option.is_return_empty && spans[i].empty()) {
      switch (aggregator.type) {
        case TSAggregatorType::SUM:
        case TSAggregatorType::COUNT:
          sample.v = 0;
          break;
        case TSAggregatorType::LAST:
          if (i == 0 || spans[i - 1].empty()) {
            sample.v = TSSample::NAN_VALUE;
          } else {
            sample.v = spans[i].back().v;
          }
          break;
        default:
          sample.v = TSSample::NAN_VALUE;
      }
    } else if (!spans[i].empty()) {
      sample.v = aggregator.AggregateSamplesValue(spans[i]);
    } else {
      continue;
    }
    res.emplace_back(sample);
  }
  return res;
}

LabelKVList ExtractSelectedLabels(LabelKVList labels, const std::set<std::string> &selected_labels) {
  std::unordered_map<std::string_view, LabelKVPair *> labels_map;
  labels_map.reserve(labels.size());
  for (auto &label : labels) {
    labels_map[label.k] = &label;
  }
  LabelKVList res;
  res.reserve(selected_labels.size());
  for (const auto &selected_key : selected_labels) {
    auto it = labels_map.find(selected_key);
    if (it != labels_map.end()) {
      res.emplace_back(std::move(*(it->second)));
    } else {
      res.push_back({selected_key, ""});
    }
  }
  return res;
}

std::vector<TSSample> GroupSamplesAndReduce(const std::vector<std::vector<TSSample>> &all_samples,
                                            TSMRangeOption::GroupReducerType reducer_type) {
  if (reducer_type == TSMRangeOption::GroupReducerType::NONE) {
    return {};
  }
  struct SamplePtr {
    const TSSample *sample;
    size_t vector_idx;
    size_t sample_idx;

    bool operator>(const SamplePtr &other) const { return sample->ts > other.sample->ts; }
  };
  std::vector<TSSample> result;
  std::priority_queue<SamplePtr, std::vector<SamplePtr>, std::greater<SamplePtr>> min_heap;

  // Initialize the min-heap with the first element of each vector
  for (size_t i = 0; i < all_samples.size(); ++i) {
    if (!all_samples[i].empty()) {
      min_heap.push({&all_samples[i][0], i, 0});
    }
  }
  if (min_heap.empty()) {
    return result;
  }

  auto reduce = [&](nonstd::span<const TSSample> samples) -> double {
    auto sample_size = static_cast<double>(samples.size());
    switch (reducer_type) {
      case TSMRangeOption::GroupReducerType::SUM:
        return Reducer::Sum(samples);
      case TSMRangeOption::GroupReducerType::AVG:
        return samples.empty() ? 0.0 : Reducer::Sum(samples) / sample_size;
      case TSMRangeOption::GroupReducerType::MIN:
        return Reducer::Min(samples);
      case TSMRangeOption::GroupReducerType::MAX:
        return Reducer::Max(samples);
      case TSMRangeOption::GroupReducerType::RANGE:
        return Reducer::Range(samples);
      case TSMRangeOption::GroupReducerType::COUNT:
        return sample_size;
      case TSMRangeOption::GroupReducerType::STD_P:
        return Reducer::StdP(samples);
      case TSMRangeOption::GroupReducerType::STD_S:
        return Reducer::StdS(samples);
      case TSMRangeOption::GroupReducerType::VAR_P:
        return Reducer::VarP(samples);
      case TSMRangeOption::GroupReducerType::VAR_S:
        return Reducer::VarS(samples);
      case TSMRangeOption::GroupReducerType::NONE:
        return 0.0;
    }
    return 0.0;
  };
  std::vector<TSSample> current_group;
  current_group.reserve(all_samples.size());

  while (!min_heap.empty()) {
    // Get the top element from the min-heap
    SamplePtr top = min_heap.top();
    min_heap.pop();

    // Check if the timestamp is the same as the current group
    if (!current_group.empty() && top.sample->ts != current_group.back().ts) {
      // Different timestamp, reduce the current group and start a new one
      uint64_t group_ts = current_group.back().ts;
      nonstd::span<const TSSample> group_span(current_group);
      double reduced_value = reduce(group_span);

      result.push_back({group_ts, reduced_value});
      current_group.clear();
    }
    current_group.push_back(*top.sample);

    // Push the next element from the same vector into the min-heap
    size_t next_sample_idx = top.sample_idx + 1;
    if (next_sample_idx < all_samples[top.vector_idx].size()) {
      min_heap.push({&all_samples[top.vector_idx][next_sample_idx], top.vector_idx, next_sample_idx});
    }
  }

  // Process the last group if it exists
  if (!current_group.empty()) {
    uint64_t group_ts = current_group.back().ts;
    nonstd::span<const TSSample> group_span(current_group);
    double reduced_value = reduce(group_span);

    result.push_back({group_ts, reduced_value});
  }

  return result;
}

std::vector<TSSample> TSDownStreamMeta::AggregateMultiBuckets(
    const std::vector<nonstd::span<const TSSample>> &bucket_spans, bool skip_last_bucket) {
  std::vector<TSSample> res;
  for (size_t i = 0; i < bucket_spans.size(); i++) {
    const auto &span = bucket_spans[i];
    if (span.empty()) {
      continue;
    }
    auto bucket_idx = aggregator.CalculateAlignedBucketLeft(span.front().ts);
    if (bucket_idx < latest_bucket_idx) {
      continue;
    }
    if (bucket_idx > latest_bucket_idx) {
      // Aggregate the previous bucket from aux info and push to result
      TSSample sample;
      sample.ts = latest_bucket_idx;
      double v = 0.0;
      double temp_n = 0.0;
      switch (aggregator.type) {
        case TSAggregatorType::SUM:
        case TSAggregatorType::MIN:
        case TSAggregatorType::MAX:
        case TSAggregatorType::COUNT:
        case TSAggregatorType::FIRST:
        case TSAggregatorType::LAST:
          sample.v = f64_auxs[0];
          break;
        case TSAggregatorType::AVG:
          temp_n = static_cast<double>(u64_auxs[0]);
          sample.v = f64_auxs[0] / temp_n;
          break;
        case TSAggregatorType::STD_P:
        case TSAggregatorType::STD_S:
        case TSAggregatorType::VAR_P:
        case TSAggregatorType::VAR_S:
          temp_n = static_cast<double>(u64_auxs[0]);
          v = f64_auxs[1] - f64_auxs[0] * f64_auxs[0] / temp_n;
          if (aggregator.type == TSAggregatorType::STD_S || aggregator.type == TSAggregatorType::VAR_S) {
            if (u64_auxs[0] > 1) {
              v = v / (temp_n - 1);
            } else {
              v = 0.0;
            }
          } else {
            v = v / temp_n;
          }
          if (aggregator.type == TSAggregatorType::STD_P || aggregator.type == TSAggregatorType::STD_S) {
            sample.v = std::sqrt(v);
          } else {
            sample.v = v;
          }
          break;
        case TSAggregatorType::RANGE:
          sample.v = f64_auxs[1] - f64_auxs[0];
          break;
        default:
          unreachable();
      }
      res.push_back(sample);
      // Reset aux info for the new bucket
      ResetAuxs();
      latest_bucket_idx = bucket_idx;
    }
    if (skip_last_bucket && i == bucket_spans.size() - 1) {
      // Skip updating aux info for the last bucket
      break;
    }
    AggregateLatestBucket(span);
  }

  return res;
}

void TSDownStreamMeta::AggregateLatestBucket(nonstd::span<const TSSample> samples) {
  if (samples.empty()) return;
  double temp_v = 0.0;
  switch (aggregator.type) {
    case TSAggregatorType::SUM:
      f64_auxs[0] += Reducer::Sum(samples);
      break;
    case TSAggregatorType::MIN:
      temp_v = Reducer::Min(samples);
      f64_auxs[0] = std::isnan(f64_auxs[0]) ? temp_v : std::min(f64_auxs[0], temp_v);
      break;
    case TSAggregatorType::MAX:
      temp_v = Reducer::Max(samples);
      f64_auxs[0] = std::isnan(f64_auxs[0]) ? temp_v : std::max(f64_auxs[0], temp_v);
      break;
    case TSAggregatorType::COUNT:
      f64_auxs[0] += static_cast<double>(samples.size());
      break;
    case TSAggregatorType::FIRST:
      if (std::isnan(f64_auxs[0]) || samples.front().ts < u64_auxs[0]) {
        f64_auxs[0] = samples.front().v;
        u64_auxs[0] = samples.front().ts;
      }
      break;
    case TSAggregatorType::LAST:
      if (std::isnan(f64_auxs[0]) || samples.back().ts > u64_auxs[0]) {
        f64_auxs[0] = samples.back().v;
        u64_auxs[0] = samples.back().ts;
      }
      break;
    case TSAggregatorType::AVG:
      u64_auxs[0] += static_cast<uint64_t>(samples.size());
      f64_auxs[0] += Reducer::Sum(samples);
      break;
    case TSAggregatorType::STD_P:
    case TSAggregatorType::STD_S:
    case TSAggregatorType::VAR_P:
    case TSAggregatorType::VAR_S:
      u64_auxs[0] += static_cast<uint64_t>(samples.size());
      f64_auxs[0] += Reducer::Sum(samples);
      f64_auxs[1] += Reducer::SquareSum(samples);
      break;
    case TSAggregatorType::RANGE:
      if (std::isnan(f64_auxs[0])) {
        f64_auxs[0] = Reducer::Min(samples);
        f64_auxs[1] = Reducer::Max(samples);
      } else {
        f64_auxs[0] = std::min(f64_auxs[0], Reducer::Min(samples));
        f64_auxs[1] = std::max(f64_auxs[1], Reducer::Max(samples));
      }
      break;
    default:
      unreachable();
  }
}

void TSDownStreamMeta::ResetAuxs() {
  auto type = aggregator.type;
  switch (type) {
    case TSAggregatorType::SUM:
      f64_auxs = {0.0};
      break;
    case TSAggregatorType::MIN:
    case TSAggregatorType::MAX:
      f64_auxs = {TSSample::NAN_VALUE};
      break;
    case TSAggregatorType::COUNT:
      f64_auxs = {0};
      break;
    case TSAggregatorType::FIRST:
      u64_auxs = {TSSample::MAX_TIMESTAMP};
      f64_auxs = {TSSample::NAN_VALUE};
      break;
    case TSAggregatorType::LAST:
      u64_auxs = {0};
      f64_auxs = {TSSample::NAN_VALUE};
      break;
    case TSAggregatorType::AVG:
      u64_auxs = {0};
      f64_auxs = {0.0};
      break;
    case TSAggregatorType::STD_P:
    case TSAggregatorType::STD_S:
    case TSAggregatorType::VAR_P:
    case TSAggregatorType::VAR_S:
      u64_auxs = {0};
      f64_auxs = {0.0, 0.0};
      break;
    case TSAggregatorType::RANGE:
      f64_auxs = {TSSample::NAN_VALUE, TSSample::NAN_VALUE};
      break;
    default:
      unreachable();
  }
}

void TSDownStreamMeta::Encode(std::string *dst) const {
  PutFixed8(dst, static_cast<uint8_t>(aggregator.type));
  PutFixed64(dst, aggregator.bucket_duration);
  PutFixed64(dst, aggregator.alignment);
  PutFixed64(dst, latest_bucket_idx);
  PutFixed8(dst, static_cast<uint8_t>(u64_auxs.size()));
  PutFixed8(dst, static_cast<uint8_t>(f64_auxs.size()));
  for (const auto &aux : u64_auxs) {
    PutFixed64(dst, aux);
  }
  for (const auto &aux : f64_auxs) {
    PutDouble(dst, aux);
  }
}

rocksdb::Status TSDownStreamMeta::Decode(Slice *input) {
  if (input->size() < sizeof(uint8_t) * 3 + sizeof(uint64_t) * 3) {
    return rocksdb::Status::InvalidArgument("TSDownStreamMeta size is too short");
  }

  GetFixed8(input, reinterpret_cast<uint8_t *>(&aggregator.type));
  GetFixed64(input, &aggregator.bucket_duration);
  GetFixed64(input, &aggregator.alignment);
  GetFixed64(input, &latest_bucket_idx);
  uint8_t u64_auxs_size = 0;
  GetFixed8(input, &u64_auxs_size);
  uint8_t f64_auxs_size = 0;
  GetFixed8(input, &f64_auxs_size);

  // Strict checking to prevent accidental overwrites
  if (input->size() != sizeof(uint64_t) * u64_auxs_size + sizeof(double) * f64_auxs_size) {
    return rocksdb::Status::InvalidArgument("Invalid auxinfo size");
  }

  for (uint8_t i = 0; i < u64_auxs_size; i++) {
    uint64_t aux = 0;
    GetFixed64(input, &aux);
    u64_auxs.push_back(aux);
  }
  for (uint8_t i = 0; i < f64_auxs_size; i++) {
    double aux = 0;
    GetDouble(input, &aux);
    f64_auxs.push_back(aux);
  }

  return rocksdb::Status::OK();
}

IndexInternalKey::IndexInternalKey(Slice input) {
  // Get namespace
  uint8_t ns_size = 0;
  GetFixed8(&input, &ns_size);
  ns = Slice(input.data(), ns_size);
  input.remove_prefix(ns_size);
  // Get index key type
  GetFixed8(&input, reinterpret_cast<uint8_t *>(&type));
}

TSRevLabelKey::TSRevLabelKey(Slice input) : IndexInternalKey(input) {
  // Remove the part of namespace and index key type
  input.remove_prefix(ns.size() + sizeof(uint8_t) * 2);
  // Get label key and value
  GetSizedString(&input, &label_key);
  GetSizedString(&input, &label_value);
  // Get user key
  user_key = Slice(input.data(), input.size());
}

std::string TSRevLabelKey::Encode() const {
  std::string encoded;
  size_t total = 1 + ns.size() + 1 + 4 + label_key.size() + 4 + label_value.size() + user_key.size();

  encoded.resize(total);
  auto buf = encoded.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(ns.size()));
  buf = EncodeBuffer(buf, ns);
  buf = EncodeFixed8(buf, static_cast<uint8_t>(IndexKeyType::TS_LABEL));
  buf = EncodeFixed32(buf, static_cast<uint32_t>(label_key.size()));
  buf = EncodeBuffer(buf, label_key);
  buf = EncodeFixed32(buf, static_cast<uint32_t>(label_value.size()));
  buf = EncodeBuffer(buf, label_value);
  EncodeBuffer(buf, user_key);

  return encoded;
}

std::string TSRevLabelKey::UpperBound(Slice ns) {
  std::string encoded;
  size_t total = 1 + ns.size() + 1;
  encoded.resize(total);
  auto buf = encoded.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(ns.size()));
  buf = EncodeBuffer(buf, ns);
  EncodeFixed8(buf, static_cast<uint8_t>(IndexKeyType::TS_LABEL) + 1);
  return encoded;
}

TSCreateOption::TSCreateOption()
    : retention_time(kDefaultRetentionTime),
      chunk_size(kDefaultChunkSize),
      chunk_type(kDefaultChunkType),
      duplicate_policy(kDefaultDuplicatePolicy) {}

Status TSMQueryFilterParser::Parse(std::string_view expr) {
  if (expr.empty()) return Status::OK();
  // Locate "!=" or "="
  const auto [op_pos, op_len] = findOperator(expr);
  if (op_pos == std::string_view::npos) {
    return {Status::RedisParseErr, "failed parsing labels"};
  }
  // Extract label and value
  std::string_view label = expr.substr(0, op_pos);
  label = trim(label);

  std::string_view value_str = expr.substr(op_pos + op_len);
  std::string_view op = expr.substr(op_pos, op_len);  // "=" or "!="
  if (op == "=") {
    handleEquals(label, value_str);
  } else if (op == "!=") {
    handleNotEquals(label, value_str);
  }
  return Status::OK();
}

Status TSMQueryFilterParser::Check() const {
  if (option_.labels_equals.empty() || !has_matcher_) {
    return {Status::RedisParseErr, "please provide at least one matcher"};
  }
  return Status::OK();
}

std::pair<size_t, size_t> TSMQueryFilterParser::findOperator(std::string_view expr) {
  char quote = 0;
  for (size_t i = 0; i < expr.size(); i++) {
    char c = expr[i];
    if (c == '\'' || c == '"') {
      if (quote == 0)
        quote = c;
      else if (quote == c)
        quote = 0;
    } else if (quote == 0) {
      if (c == '!' && i + 1 < expr.size() && expr[i + 1] == '=') {
        return {i, 2};
      } else if (c == '=') {
        return {i, 1};
      }
    }
  }
  return {std::string_view::npos, 0};
}

std::string_view TSMQueryFilterParser::trim(std::string_view s) {
  while (!s.empty() && std::isspace(s.front())) {
    s.remove_prefix(1);
  }
  while (!s.empty() && std::isspace(s.back())) {
    s.remove_suffix(1);
  }
  return s;
}

std::string_view TSMQueryFilterParser::unquote(std::string_view s) {
  if (s.size() >= 2) {
    char first = s.front();
    char last = s.back();
    if ((first == '"' && last == '"') || (first == '\'' && last == '\'')) {
      return s.substr(1, s.size() - 2);
    }
  }
  return s;
}

std::vector<std::string_view> TSMQueryFilterParser::splitValueList(std::string_view list) {
  std::vector<std::string_view> values;
  if (list.empty()) return values;

  char quote = 0;
  int depth = 0;
  size_t start = 0;

  for (size_t i = 0; i <= list.size(); i++) {
    if (i == list.size()) {
      if (start < i) {
        auto val = trim(unquote(list.substr(start, i - start)));
        if (!val.empty()) {
          values.push_back(val);
        }
      }
      break;
    }
    char c = list[i];
    if (c == '\'' || c == '"') {
      if (quote == 0)
        quote = c;
      else if (quote == c)
        quote = 0;
    } else if (quote == 0) {
      if (c == '(')
        depth++;
      else if (c == ')')
        if (depth > 0) depth--;
    }
    if (c == ',' && quote == 0 && depth == 0) {
      auto val = trim(unquote(list.substr(start, i - start)));
      if (!val.empty()) {
        values.push_back(val);
      }
      start = i + 1;
    }
  }
  return values;
}

void TSMQueryFilterParser::handleEquals(std::string_view label, std::string_view value_str) {
  std::string label_str(label);
  if (value_str.empty()) {
    // Label not exists: label=
    option_.labels_equals[std::move(label_str)].clear();
  } else {
    has_matcher_ = true;
    // If label exists, but value is empty, means label not exists, skip it
    if (option_.labels_equals.count(label_str) && option_.labels_equals[label_str].empty()) {
      return;
    }
    std::set<std::string> values;
    if (value_str.front() == '(' && value_str.back() == ')') {
      // List: label=(v1,v2)
      for (auto val : splitValueList(value_str.substr(1, value_str.size() - 2))) {
        values.emplace(val);
      }
    } else {
      // Single value: label=value
      values.emplace(unquote(value_str));
    }
    option_.labels_equals[std::move(label_str)].merge(std::move(values));
  }
}

void TSMQueryFilterParser::handleNotEquals(std::string_view label, std::string_view value_str) {
  std::string label_str(label);
  if (value_str.empty()) {
    // Label exists: label!=
    option_.labels_not_equals[std::move(label_str)].insert("");  // Use empty string to indicate label exists
  } else {
    std::set<std::string> values;
    if (value_str.front() == '(' && value_str.back() == ')') {
      // List: label!=(v1,v2)
      for (auto val : splitValueList(value_str.substr(1, value_str.size() - 2))) {
        values.emplace(val);
      }
    } else {
      // Single value: label!=value
      values.emplace(unquote(value_str));
    }
    option_.labels_not_equals[std::move(label_str)].merge(std::move(values));
  }
}

TimeSeriesMetadata CreateMetadataFromOption(const TSCreateOption &option) {
  TimeSeriesMetadata metadata;
  metadata.retention_time = option.retention_time;
  metadata.chunk_size = option.chunk_size;
  metadata.chunk_type = option.chunk_type;
  metadata.duplicate_policy = option.duplicate_policy;
  metadata.SetSourceKey(option.source_key);

  return metadata;
}

TSDownStreamMeta CreateDownStreamMetaFromAgg(const TSAggregator &aggregator) {
  TSDownStreamMeta meta;
  meta.aggregator = aggregator;
  meta.latest_bucket_idx = 0;
  meta.ResetAuxs();
  return meta;
}

uint64_t TSAggregator::CalculateAlignedBucketLeft(uint64_t ts) const {
  uint64_t x = 0;

  if (ts >= alignment) {
    uint64_t diff = ts - alignment;
    uint64_t k = diff / bucket_duration;
    x = alignment + k * bucket_duration;
  } else {
    uint64_t diff = alignment - ts;
    uint64_t m0 = diff / bucket_duration + (diff % bucket_duration == 0 ? 0 : 1);
    if (m0 <= alignment / bucket_duration) {
      x = alignment - m0 * bucket_duration;
    }
  }

  return x;
}

uint64_t TSAggregator::CalculateAlignedBucketRight(uint64_t ts) const {
  uint64_t x = TSSample::MAX_TIMESTAMP;
  if (ts < alignment) {
    uint64_t diff = alignment - ts;
    uint64_t k = diff / bucket_duration;
    x = alignment - k * bucket_duration;
  } else {
    uint64_t diff = ts - alignment;
    uint64_t m0 = diff / bucket_duration + 1;
    if (m0 <= (TSSample::MAX_TIMESTAMP - alignment) / bucket_duration) {
      x = alignment + m0 * bucket_duration;
    }
  }

  return x;
}

std::vector<nonstd::span<const TSSample>> TSAggregator::SplitSamplesToBuckets(
    nonstd::span<const TSSample> samples) const {
  std::vector<nonstd::span<const TSSample>> spans;
  if (type == TSAggregatorType::NONE || samples.empty()) {
    return spans;
  }
  uint64_t start_bucket = CalculateAlignedBucketLeft(samples.front().ts);
  uint64_t end_bucket = CalculateAlignedBucketLeft(samples.back().ts);
  uint64_t bucket_count = (end_bucket - start_bucket) / bucket_duration + 1;

  spans.reserve(bucket_count);
  auto it = samples.begin();
  const auto end = samples.end();
  uint64_t bucket_left = start_bucket;
  while (it != end) {
    uint64_t bucket_right = CalculateAlignedBucketRight(bucket_left);
    auto lower = std::lower_bound(it, end, TSSample{bucket_left, 0.0});
    auto upper = std::lower_bound(lower, end, TSSample{bucket_right, 0.0});
    spans.emplace_back(lower, upper);
    it = upper;

    bucket_left = bucket_right;
  }
  return spans;
}

nonstd::span<const TSSample> TSAggregator::GetBucketByTimestamp(nonstd::span<const TSSample> samples, uint64_t ts,
                                                                uint64_t less_than) const {
  if (type == TSAggregatorType::NONE || samples.empty()) {
    return {};
  }
  uint64_t start_bucket = CalculateAlignedBucketLeft(ts);
  uint64_t end_bucket = std::min(CalculateAlignedBucketRight(ts), less_than);
  auto lower = std::lower_bound(samples.begin(), samples.end(), TSSample{start_bucket, 0.0});
  auto upper = std::lower_bound(lower, samples.end(), TSSample{end_bucket, 0.0});
  if (lower == upper) {
    return {};
  }
  return {lower, upper};
}

double TSAggregator::AggregateSamplesValue(nonstd::span<const TSSample> samples) const {
  double res = TSSample::NAN_VALUE;
  if (samples.empty()) {
    return res;
  }
  auto sample_size = static_cast<double>(samples.size());
  switch (type) {
    case TSAggregatorType::AVG:
      res = Reducer::Sum(samples) / sample_size;
      break;
    case TSAggregatorType::SUM:
      res = Reducer::Sum(samples);
      break;
    case TSAggregatorType::MIN:
      res = Reducer::Min(samples);
      break;
    case TSAggregatorType::MAX:
      res = Reducer::Max(samples);
      break;
    case TSAggregatorType::RANGE:
      res = Reducer::Range(samples);
      break;
    case TSAggregatorType::COUNT:
      res = sample_size;
      break;
    case TSAggregatorType::FIRST:
      res = samples.front().v;
      break;
    case TSAggregatorType::LAST:
      res = samples.back().v;
      break;
    case TSAggregatorType::STD_P:
      res = Reducer::StdP(samples);
      break;
    case TSAggregatorType::STD_S:
      res = Reducer::StdS(samples);
      break;
    case TSAggregatorType::VAR_P:
      res = Reducer::VarP(samples);
      break;
    case TSAggregatorType::VAR_S:
      res = Reducer::VarS(samples);
      break;
    default:
      unreachable();
  }

  return res;
}

rocksdb::Status TimeSeries::getTimeSeriesMetadata(engine::Context &ctx, const Slice &ns_key,
                                                  TimeSeriesMetadata *metadata) {
  return Database::GetMetadata(ctx, {kRedisTimeSeries}, ns_key, metadata);
}

rocksdb::Status TimeSeries::createTimeSeries(engine::Context &ctx, const Slice &ns_key,
                                             TimeSeriesMetadata *metadata_out, const TSCreateOption *option) {
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries, {"createTimeSeries"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  *metadata_out = CreateMetadataFromOption(option ? *option : TSCreateOption{});
  std::string bytes;
  metadata_out->Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_key, bytes);
  if (!s.ok()) return s;

  if (option && !option->labels.empty()) {
    createLabelIndexInBatch(ns_key, *metadata_out, batch, option->labels);
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::getOrCreateTimeSeries(engine::Context &ctx, const Slice &ns_key,
                                                  TimeSeriesMetadata *metadata_out, const TSCreateOption *option) {
  auto s = getTimeSeriesMetadata(ctx, ns_key, metadata_out);
  if (s.ok()) {
    return s;
  }
  return createTimeSeries(ctx, ns_key, metadata_out, option);
}

rocksdb::Status TimeSeries::upsertCommon(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                         SampleBatch &sample_batch, DownstreamUpsertArgs *ds_args) {
  auto batch = storage_->GetWriteBatchBase();
  auto s = upsertCommonInBatch(ctx, ns_key, metadata, sample_batch, batch, ds_args);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::upsertCommonInBatch(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                                SampleBatch &sample_batch,
                                                ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                DownstreamUpsertArgs *ds_args) {
  if (ds_args != nullptr) ds_args->new_chunks.clear();

  auto all_batch_slice = sample_batch.AsSlice();
  if (all_batch_slice.GetSampleSpan().empty()) {
    return rocksdb::Status::OK();
  }

  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  uint64_t chunk_count = metadata.size;

  // Get the latest chunk
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  TSChunkPtr latest_chunk;
  std::string latest_chunk_key, latest_chunk_value;
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    // Create a new empty chunk if there is no chunk
    auto [chunk_ptr_, data_] = CreateEmptyOwnedTSChunk();
    latest_chunk_value = std::move(data_);
    latest_chunk = std::move(chunk_ptr_);
  } else {
    latest_chunk_key = iter->key().ToString();
    latest_chunk_value = iter->value().ToString();
    latest_chunk = CreateTSChunkFromData(latest_chunk_value);
  }

  // Filter out samples older than retention time
  sample_batch.Expire(latest_chunk->GetLastTimestamp(), metadata.retention_time);
  if (all_batch_slice.GetValidCount() == 0) {
    return rocksdb::Status::OK();
  }

  WriteBatchLogData log_data(kRedisTimeSeries);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Get the first chunk
  auto start_key = internalKeyFromChunkID(ns_key, metadata, all_batch_slice.GetFirstTimestamp());
  iter->SeekForPrev(start_key);
  if (!iter->Valid()) {
    iter->Seek(start_key);
  } else if (!iter->key().starts_with(prefix)) {
    iter->Next();
  }

  // Process samples added to sealed chunks
  uint64_t start_ts = 0;
  uint64_t end_ts = TSSample::MAX_TIMESTAMP;
  bool is_chunk = (iter->Valid() && iter->key().starts_with(prefix));
  while (is_chunk) {
    auto cur_chunk_data = iter->value().ToString();
    auto cur_chunk_key = iter->key().ToString();
    iter->Next();
    is_chunk = (iter->Valid() && iter->key().starts_with(prefix));
    if (!is_chunk) {
      // Process last chunk
      break;
    }
    end_ts = chunkIDFromInternalKey(iter->key());

    auto chunk = CreateTSChunkFromData(cur_chunk_data);
    auto sample_slice = all_batch_slice.SliceByTimestamps(start_ts, end_ts);
    if (sample_slice.GetValidCount() == 0) {
      continue;
    }
    auto new_data_list = chunk->UpsertSampleAndSplit(sample_slice, metadata.chunk_size, false);
    for (size_t i = 0; i < new_data_list.size(); i++) {
      auto &new_data = new_data_list[i];
      auto new_chunk = CreateTSChunkFromData(new_data);
      auto new_key = internalKeyFromChunkID(ns_key, metadata, new_chunk->GetFirstTimestamp());
      // Process samples older than the first chunk, should update the key
      if (i == 0 && new_key != cur_chunk_key) {
        s = batch->Delete(cur_chunk_key);
        if (!s.ok()) return s;
      }
      s = batch->Put(new_key, new_data);
      if (!s.ok()) return s;
    }
    if (!new_data_list.empty()) {
      chunk_count += new_data_list.size() - 1;
    }
  }

  // Process samples added to latest chunk(unseal)
  auto remained_samples = all_batch_slice.SliceByTimestamps(start_ts, TSSample::MAX_TIMESTAMP, true);

  auto new_data_list = latest_chunk->UpsertSampleAndSplit(remained_samples, metadata.chunk_size, true);
  for (size_t i = 0; i < new_data_list.size(); i++) {
    auto &new_data = new_data_list[i];
    auto new_chunk = CreateTSChunkFromData(new_data);
    auto new_key = internalKeyFromChunkID(ns_key, metadata, new_chunk->GetFirstTimestamp());
    if (i == 0 && new_key != latest_chunk_key) {
      s = batch->Delete(latest_chunk_key);
      if (!s.ok()) return s;
    }
    s = batch->Put(new_key, new_data);
    if (!s.ok()) return s;
  }
  if (!new_data_list.empty()) {
    chunk_count += new_data_list.size() - (metadata.size == 0 ? 0 : 1);
  }
  if (chunk_count != metadata.size) {
    metadata.size = chunk_count;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  // For downstream processing
  if (ds_args != nullptr) {
    if (new_data_list.size()) {
      ds_args->new_chunks = std::move(new_data_list);
    } else {
      ds_args->new_chunks = {std::move(latest_chunk_value)};
    }
    ds_args->sample_batch = &sample_batch;
    if (latest_chunk_key.empty()) ds_args->was_source_empty = true;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::rangeCommon(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                        const TSRangeOption &option, std::vector<TSSample> *res, bool apply_retention) {
  if (option.end_ts < option.start_ts) {
    return rocksdb::Status::OK();
  }

  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  // Get the latest chunk
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    return rocksdb::Status::OK();
  }
  auto chunk = CreateTSChunkFromData(iter->value());
  uint64_t last_timestamp = chunk->GetLastTimestamp();
  uint64_t retention_bound =
      (apply_retention && metadata.retention_time != 0 && last_timestamp > metadata.retention_time)
          ? last_timestamp - metadata.retention_time
          : 0;
  uint64_t start_timestamp = std::max(retention_bound, option.start_ts);
  uint64_t end_timestamp = std::min(last_timestamp, option.end_ts);

  // Update iterator options
  auto start_key = internalKeyFromChunkID(ns_key, metadata, start_timestamp);
  if (end_timestamp != TSSample::MAX_TIMESTAMP) {
    end_key = internalKeyFromChunkID(ns_key, metadata, end_timestamp + 1);
  }
  upper_bound = Slice(end_key);
  read_options.iterate_upper_bound = &upper_bound;
  iter = util::UniqueIterator(ctx, read_options);

  iter->SeekForPrev(start_key);
  if (!iter->Valid()) {
    iter->Seek(start_key);
  } else if (!iter->key().starts_with(prefix)) {
    iter->Next();
  }
  // Prepare to store results
  std::vector<TSSample> temp_results;
  const auto &aggregator = option.aggregator;
  bool has_aggregator = aggregator.type != TSAggregatorType::NONE;
  if (iter->Valid()) {
    if (option.count_limit != 0 && !has_aggregator) {
      temp_results.reserve(option.count_limit);
    } else {
      chunk = CreateTSChunkFromData(iter->value());
      auto range = chunk->GetLastTimestamp() - chunk->GetFirstTimestamp() + 1;
      auto estimate_chunks = std::min((end_timestamp - start_timestamp) / range, uint64_t(32));
      temp_results.reserve(estimate_chunks * metadata.chunk_size);
    }
  }
  // Get samples from chunks
  uint64_t bucket_count = 0;
  uint64_t last_bucket = 0;
  bool is_not_enough = true;
  for (; iter->Valid() && is_not_enough; iter->Next()) {
    chunk = CreateTSChunkFromData(iter->value());
    auto it = chunk->CreateIterator();
    while (it->HasNext()) {
      auto sample = it->Next().value();
      // Early termination check
      if (!has_aggregator && option.count_limit && temp_results.size() >= option.count_limit) {
        is_not_enough = false;
        break;
      }
      const bool in_time_range = sample->ts >= start_timestamp && sample->ts <= end_timestamp;
      const bool not_time_filtered = option.filter_by_ts.empty() || option.filter_by_ts.count(sample->ts);
      const bool value_in_range = !option.filter_by_value || (sample->v >= option.filter_by_value->first &&
                                                              sample->v <= option.filter_by_value->second);

      if (!in_time_range || !not_time_filtered || !value_in_range) {
        continue;
      }

      // Do checks for early termination when `count_limit` is set.
      if (has_aggregator && option.count_limit > 0) {
        const auto bucket = aggregator.CalculateAlignedBucketRight(sample->ts);
        const bool is_empty_count = (last_bucket > 0 && option.is_return_empty);
        const size_t increment = is_empty_count ? (bucket - last_bucket) / aggregator.bucket_duration : 1;
        bucket_count += increment;
        last_bucket = bucket;
        if (bucket_count > option.count_limit) {
          is_not_enough = false;
          temp_results.push_back(*sample);  // Ensure empty bucket is reported
          break;
        }
      }
      temp_results.push_back(*sample);
    }
  }

  // Process compaction logic
  *res = AggregateSamplesByRangeOption(std::move(temp_results), option);

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::upsertDownStream(engine::Context &ctx, const Slice &ns_key,
                                             const TimeSeriesMetadata &metadata, DownstreamUpsertArgs &ds_args) {
  // If no valid written
  auto &new_chunks = ds_args.new_chunks;
  auto *sample_batch = ds_args.sample_batch;
  if (new_chunks.empty()) return rocksdb::Status::OK();
  std::vector<std::string> downstream_keys;
  std::vector<TSDownStreamMeta> downstream_metas;
  auto s = getDownStreamRules(ctx, ns_key, metadata, &downstream_keys, &downstream_metas);
  if (!s.ok()) return s;
  if (downstream_keys.empty()) return rocksdb::Status::OK();

  auto all_batch_slice = sample_batch->AsSlice();
  uint64_t new_chunk_first_ts = CreateTSChunkFromData(new_chunks[0])->GetFirstTimestamp();

  nonstd::span<const AddResult> add_results = all_batch_slice.GetAddResultSpan();
  auto samples_span = all_batch_slice.GetSampleSpan();
  std::vector<std::vector<TSSample>> all_agg_samples(downstream_metas.size());
  std::vector<std::vector<TSSample>> all_agg_samples_inc(downstream_metas.size());
  std::vector<uint64_t> last_buckets(downstream_metas.size());
  std::vector<bool> is_meta_updates(downstream_metas.size(), false);

  using AddResultType = TSChunk::AddResultType;
  struct ProcessingInfo {
    uint64_t start_ts;
    uint64_t end_ts;
    size_t sample_idx;
    std::vector<size_t> downstream_indices;
  };
  std::vector<ProcessingInfo> processing_infos;
  processing_infos.reserve(add_results.size());

  for (size_t i = 0; i < add_results.size(); i++) {
    const auto &add_result = add_results[i];
    auto sample_ts = add_result.sample.ts;
    const auto type = add_result.type;
    if (type != AddResultType::kInsert && type != AddResultType::kUpdate) {
      continue;
    }

    // Prepare  info for samples added to sealed chunks
    ProcessingInfo info;
    info.sample_idx = i;
    info.start_ts = TSSample::MAX_TIMESTAMP;
    info.end_ts = 0;

    for (size_t j = 0; j < downstream_metas.size(); j++) {
      const auto &agg = downstream_metas[j].aggregator;
      uint64_t latest_bucket_idx = downstream_metas[j].latest_bucket_idx;
      uint64_t bkt_left = agg.CalculateAlignedBucketLeft(sample_ts);

      // Skip samples with timestamps beyond the retrieval boundary
      // Boundary is defined as the later of:
      //   - New chunk start time (new_chunk_first_ts)
      //   - Latest bucket index (latest_bucket_idx)
      auto boundary = std::max(new_chunk_first_ts, latest_bucket_idx);
      if (sample_ts >= boundary) {
        continue;
      }
      // For these type, no need retrieve source samples
      if (IsIncrementalAggregatorType(agg.type)) {
        info.downstream_indices.push_back(j);
        continue;
      }
      if ((i > 0 && bkt_left == last_buckets[j])) {
        continue;
      }

      info.downstream_indices.push_back(j);
      uint64_t bkt_right = agg.CalculateAlignedBucketRight(sample_ts);
      info.start_ts = std::min(info.start_ts, bkt_left);
      info.end_ts = std::max(info.end_ts, bkt_right);
      info.end_ts = std::min(info.end_ts, boundary - 1);  // Exclusive. Boundary > 0
    }

    if (info.downstream_indices.size()) {
      processing_infos.push_back(info);
    }
  }

  // Process samples added to sealed chunks
  for (const auto &info : processing_infos) {
    const auto &add_result = add_results[info.sample_idx];
    const auto &sample = samples_span[info.sample_idx];

    TSRangeOption option;
    option.start_ts = info.start_ts;
    option.end_ts = info.end_ts;
    std::vector<TSSample> retrieve_samples;
    s = rangeCommon(ctx, ns_key, metadata, option, &retrieve_samples, false);
    if (!s.ok()) return s;

    for (size_t j : info.downstream_indices) {
      auto &meta = downstream_metas[j];
      const auto &agg = meta.aggregator;
      uint64_t bkt_left = agg.CalculateAlignedBucketLeft(add_result.sample.ts);

      if (IsIncrementalAggregatorType(agg.type)) {
        std::vector<TSSample> sample_temp = {{bkt_left, add_result.sample.v}};
        switch (agg.type) {
          case TSAggregatorType::MIN:
          case TSAggregatorType::MAX:
            sample_temp[0].v = sample.v;
            break;
          case TSAggregatorType::COUNT:
            sample_temp[0].v = 1.0;
            break;
          default:
            break;
        }
        if (bkt_left == meta.latest_bucket_idx) {
          meta.AggregateLatestBucket(sample_temp);
          is_meta_updates[j] = true;
        } else {
          all_agg_samples_inc[j].push_back({bkt_left, sample_temp[0].v});
        }
      } else {
        auto span = agg.GetBucketByTimestamp(retrieve_samples, bkt_left);
        CHECK(!span.empty());
        last_buckets[j] = bkt_left;
        if (bkt_left == meta.latest_bucket_idx) {
          meta.ResetAuxs();
          meta.AggregateLatestBucket(span);
          is_meta_updates[j] = true;
        } else {
          all_agg_samples[j].push_back({bkt_left, agg.AggregateSamplesValue(span)});
        }
      }
    }
  }

  // Process samples added to the latest chunk
  for (size_t i = 0; i < downstream_metas.size(); i++) {
    auto &agg_samples = all_agg_samples[i];
    auto &meta = downstream_metas[i];
    const auto &agg = meta.aggregator;
    if (new_chunks.size() > 1) {
      is_meta_updates[i] = true;
    }

    // Avoid incorrect aggregation of the `bucket_idx=0` bucket,
    // when inserting a sample with `bucket_idx>0` while the source series is empty.
    if (meta.latest_bucket_idx == 0 && ds_args.was_source_empty) {
      auto chunk = CreateTSChunkFromData(new_chunks.front());
      auto buckets = agg.SplitSamplesToBuckets(chunk->GetSamplesSpan());
      if (buckets.size()) {
        auto bkt_idx = agg.CalculateAlignedBucketLeft(buckets[0][0].ts);
        if (bkt_idx > meta.latest_bucket_idx) {
          meta.latest_bucket_idx = bkt_idx;
          is_meta_updates[i] = true;
        }
      }
    }

    auto aggregate_chunk = [&](const auto &chunk, bool is_unsealed) {
      auto buckets = agg.SplitSamplesToBuckets(chunk->GetSamplesSpan());
      if (buckets.empty()) return;
      auto samples = meta.AggregateMultiBuckets(buckets, is_unsealed);
      agg_samples.insert(agg_samples.end(), samples.begin(), samples.end());
    };
    // For chunk except the last chunk(sealed)
    for (size_t j = 0; j < new_chunks.size() - 1; j++) {
      auto chunk = CreateTSChunkFromData(new_chunks[j]);
      aggregate_chunk(chunk, false /* is_unsealed = false */);
    }
    // For last chunk(unsealed)
    auto last_chunk = CreateTSChunkFromData(new_chunks.back());
    auto newest_bucket_idx = agg.CalculateAlignedBucketLeft(last_chunk->GetLastTimestamp());
    if (meta.latest_bucket_idx < newest_bucket_idx) {
      aggregate_chunk(last_chunk, true /* is_unsealed = true */);
      is_meta_updates[i] = true;
    }
  }

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries, {"upsertDownStream"});
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Write downstream metadata
  for (size_t i = 0; i < downstream_metas.size(); i++) {
    if (!is_meta_updates[i]) {
      continue;
    }
    const auto &meta = downstream_metas[i];
    const auto &key = downstream_keys[i];
    std::string bytes;
    meta.Encode(&bytes);
    s = batch->Put(key, bytes);
    if (!s.ok()) return s;
  }
  // Write aggregated samples
  for (size_t i = 0; i < downstream_metas.size(); i++) {
    const auto &ds_key = downstream_keys[i];
    auto key = downstreamKeyFromInternalKey(ds_key);
    auto ns_key = AppendNamespacePrefix(key);
    auto &agg_samples = all_agg_samples[i];
    auto &agg_samples_inc = all_agg_samples_inc[i];

    if (agg_samples.empty() && agg_samples_inc.empty()) {
      continue;
    }
    TimeSeriesMetadata metadata;
    s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
    if (!s.ok()) return s;

    if (agg_samples.size()) {
      auto sample_batch_t = SampleBatch(std::move(agg_samples), DuplicatePolicy::LAST);
      s = upsertCommon(ctx, ns_key, metadata, sample_batch_t);
      if (!s.ok()) return s;
    }

    if (agg_samples_inc.size()) {
      const auto &agg = downstream_metas[i].aggregator;
      DuplicatePolicy policy = DuplicatePolicy::LAST;
      if (agg.type == TSAggregatorType::SUM || agg.type == TSAggregatorType::COUNT) {
        policy = DuplicatePolicy::SUM;
      } else if (agg.type == TSAggregatorType::MIN) {
        policy = DuplicatePolicy::MIN;
      } else if (agg.type == TSAggregatorType::MAX) {
        policy = DuplicatePolicy::MAX;
      }
      auto sample_batch_t = SampleBatch(std::move(agg_samples_inc), policy);
      s = upsertCommon(ctx, ns_key, metadata, sample_batch_t);
      if (!s.ok()) return s;
    }
  }
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::getCommon(engine::Context &ctx, const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                      bool is_return_latest, std::vector<TSSample> *res) {
  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  // Get the latest chunk
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    return rocksdb::Status::OK();
  }
  auto chunk = CreateTSChunkFromData(iter->value());

  if (is_return_latest) {
    // TODO: need process `latest` option
  }
  res->push_back(chunk->GetLatestSample(0));
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::delRangeCommon(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                           uint64_t from, uint64_t to, uint64_t *deleted, bool inclusive_to) {
  auto batch = storage_->GetWriteBatchBase();
  auto s = delRangeCommonInBatch(ctx, ns_key, metadata, from, to, batch, deleted, inclusive_to);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::delRangeCommonInBatch(engine::Context &ctx, const Slice &ns_key,
                                                  TimeSeriesMetadata &metadata, uint64_t from, uint64_t to,
                                                  ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                  uint64_t *deleted, bool inclusive_to) {
  *deleted = 0;
  if (from > to || (from == to && !inclusive_to)) {
    return rocksdb::Status::OK();
  }
  // In the emun `TSSubkeyType`, `LABEL` is the next of `CHUNK`
  std::string start_key = internalKeyFromChunkID(ns_key, metadata, from);
  std::string prefix = start_key.substr(0, start_key.size() - sizeof(uint64_t));
  std::string end_key;
  if (to == TSSample::MAX_TIMESTAMP && inclusive_to) {
    end_key = internalKeyFromLabelKey(ns_key, metadata, "");
  } else if (inclusive_to) {
    end_key = internalKeyFromChunkID(ns_key, metadata, to + 1);
  } else {
    end_key = internalKeyFromChunkID(ns_key, metadata, to);
  }

  uint64_t chunk_count = metadata.size;

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(end_key);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  WriteBatchLogData log_data(kRedisTimeSeries);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(start_key);
  if (!iter->Valid()) {
    iter->Seek(start_key);
  } else if (!iter->key().starts_with(prefix)) {
    iter->Next();
  }
  for (; iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    auto chunk = CreateTSChunkFromData(iter->value());
    uint64_t deleted_temp = 0;
    auto new_chunk_data = chunk->RemoveSamplesBetween(from, to, &deleted_temp, inclusive_to);
    if (new_chunk_data.empty() || deleted_temp == 0) {
      // No samples deleted
      continue;
    }
    *deleted += deleted_temp;
    auto new_chunk = CreateTSChunkFromData(new_chunk_data);
    bool need_delete_old_key = false;
    if (new_chunk->GetCount() == 0) {
      // Delete the whole chunk
      need_delete_old_key = true;
      if (chunk_count > 0) chunk_count--;
    } else {
      auto new_key = internalKeyFromChunkID(ns_key, metadata, new_chunk->GetFirstTimestamp());
      if (new_key != iter->key()) {
        // Change the chunk key
        need_delete_old_key = true;
      }
      s = batch->Put(new_key, new_chunk_data);
      if (!s.ok()) return s;
    }
    if (need_delete_old_key) {
      s = batch->Delete(iter->key());
      if (!s.ok()) return s;
    }
  }
  if (chunk_count != metadata.size) {
    metadata.size = chunk_count;
    std::string bytes;
    metadata.Encode(&bytes);
    s = batch->Put(metadata_cf_handle_, ns_key, bytes);
    if (!s.ok()) return s;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::delRangeDownStream(engine::Context &ctx, const Slice &ns_key, TimeSeriesMetadata &metadata,
                                               std::vector<std::string> &ds_keys,
                                               std::vector<TSDownStreamMeta> &ds_metas, uint64_t from, uint64_t to) {
  if (from > to || ds_keys.empty()) return rocksdb::Status::OK();

  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries);
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  // Calculate key boundaries for latest chunk retrieval
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));
  // Configure read options for reverse iteration
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;
  // Retrieve the latest chunk for boundary calculations
  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);

  // If no chunks found
  uint64_t last_chunk_start = 0;
  uint64_t last_chunk_end = 0;
  // Check if any chunks exist for the source time series.
  bool has_chunk = iter->Valid() && iter->key().starts_with(prefix);
  if (has_chunk) {
    auto last_chunk = CreateTSChunkFromData(iter->value());
    last_chunk_start = last_chunk->GetFirstTimestamp();
    last_chunk_end = last_chunk->GetLastTimestamp();
  }
  iter->Reset();  // Release iterator resources

  // Determine global time range needed for sample retrieval
  uint64_t retrieve_start_ts = TSSample::MAX_TIMESTAMP;
  uint64_t retrieve_end_ts = 0;
  if (has_chunk) {
    for (const auto &ds_meta : ds_metas) {
      retrieve_start_ts = std::min(retrieve_start_ts, ds_meta.aggregator.CalculateAlignedBucketLeft(from));
      retrieve_end_ts = std::max(retrieve_end_ts, ds_meta.aggregator.CalculateAlignedBucketRight(to) - 1);
    }
  }

  // Retrieve samples needed for downstream recalculation
  std::vector<TSSample> retrieved_samples;
  if (has_chunk) {
    TSRangeOption range_option;
    range_option.start_ts = retrieve_start_ts;
    range_option.end_ts = retrieve_end_ts;
    s = rangeCommon(ctx, ns_key, metadata, range_option, &retrieved_samples, true);
    if (!s.ok()) return s;
  }

  // Process each downstream rule
  for (size_t i = 0; i < ds_keys.size(); i++) {
    auto &ds_meta = ds_metas[i];
    auto &agg = ds_meta.aggregator;

    TimeSeriesMetadata meta;
    auto ds_ns_key = AppendNamespacePrefix(downstreamKeyFromInternalKey(ds_keys[i]));
    s = getTimeSeriesMetadata(ctx, ds_ns_key, &meta);
    if (!s.ok()) return s;

    // Calculate the range of buckets affected by this deletion.
    uint64_t start_bucket = agg.CalculateAlignedBucketLeft(from);
    uint64_t end_bucket = agg.CalculateAlignedBucketLeft(to);
    CHECK(start_bucket <= ds_meta.latest_bucket_idx);

    std::vector<TSSample> new_samples;  // To store re-aggregated boundary buckets.

    // Recalculate the start bucket.
    auto start_span = agg.GetBucketByTimestamp(retrieved_samples, start_bucket);
    // If start_span is empty, the entire bucket will be deleted. Otherwise, it's re-aggregated,
    // and the deletion starts from the next bucket.
    uint64_t del_start = start_span.empty() ? start_bucket : start_bucket + 1;
    if (!start_span.empty() && start_bucket < ds_meta.latest_bucket_idx) {
      new_samples.push_back({start_bucket, agg.AggregateSamplesValue(start_span)});
    }

    // Recalculate the end bucket.
    auto end_span = (start_bucket == end_bucket) ? start_span : agg.GetBucketByTimestamp(retrieved_samples, end_bucket);
    // If end_span is empty, the bucket is included in the deletion. Otherwise, it's re-aggregated
    // and excluded from deletion.
    bool inclusive_end = end_span.empty();
    if (!end_span.empty() && end_bucket < ds_meta.latest_bucket_idx && start_bucket != end_bucket) {
      new_samples.push_back({end_bucket, agg.AggregateSamplesValue(end_span)});
    }

    // Update recalculated buckets
    auto sample_batch = SampleBatch(std::move(new_samples), DuplicatePolicy::LAST);
    s = upsertCommonInBatch(ctx, ds_ns_key, meta, sample_batch, batch);
    if (!s.ok()) return s;

    // Delete affected buckets in downstream
    uint64_t deleted = 0;
    s = delRangeCommonInBatch(ctx, ds_ns_key, meta, del_start, end_bucket, batch, &deleted, inclusive_end);
    if (!s.ok()) return s;

    // Update latest bucket if deletion affects the end
    if (end_bucket < ds_meta.latest_bucket_idx) continue;

    if (!has_chunk) {
      ds_meta.latest_bucket_idx = 0;
    } else if (to > last_chunk_end) {
      ds_meta.latest_bucket_idx = agg.CalculateAlignedBucketLeft(last_chunk_end);
    }

    // Reaggregate latest bucket if needed
    ds_meta.ResetAuxs();
    if (has_chunk && last_chunk_start > 0) {
      auto span = agg.GetBucketByTimestamp(retrieved_samples, ds_meta.latest_bucket_idx, last_chunk_start - 1);
      ds_meta.AggregateLatestBucket(span);
    }

    // Persist downstream metadata updates if needed
    std::string bytes;
    ds_meta.Encode(&bytes);
    batch->Put(ds_keys[i], bytes);
  }

  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::createLabelIndexInBatch(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                                    ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                    const LabelKVList &labels) {
  for (auto &label : labels) {
    auto internal_key = internalKeyFromLabelKey(ns_key, metadata, label.k);
    auto s = batch->Put(internal_key, label.v);
    if (!s.ok()) return s;
  }
  auto [ns, user_key] = ExtractNamespaceKey(ns_key, storage_->IsSlotIdEncoded());
  // Reverse index
  for (auto &label : labels) {
    auto rev_index_key = TSRevLabelKey(ns, label.k, label.v, user_key).Encode();
    auto s = batch->Put(index_cf_handle_, rev_index_key, Slice());
    if (!s.ok()) return s;
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::getLabelKVList(engine::Context &ctx, const Slice &ns_key,
                                           const TimeSeriesMetadata &metadata, LabelKVList *labels) {
  // In the emun `TSSubkeyType`, `DOWNSTREAM` is the next of `LABEL`
  std::string label_upper_bound = internalKeyFromDownstreamKey(ns_key, metadata, "");
  std::string prefix = internalKeyFromLabelKey(ns_key, metadata, "");

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(label_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  labels->clear();
  for (iter->Seek(lower_bound); iter->Valid(); iter->Next()) {
    labels->push_back({labelKeyFromInternalKey(iter->key()), iter->value().ToString()});
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::createDownStreamMetadataInBatch(engine::Context &ctx, const Slice &ns_src_key,
                                                            const Slice &dst_key,
                                                            const TimeSeriesMetadata &src_metadata,
                                                            const TSAggregator &aggregator,
                                                            ObserverOrUniquePtr<rocksdb::WriteBatchBase> &batch,
                                                            TSDownStreamMeta *ds_metadata) {
  WriteBatchLogData log_data(kRedisTimeSeries, {"createDownStreamMetadata"});
  auto s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  *ds_metadata = CreateDownStreamMetaFromAgg(aggregator);
  std::string bytes;
  ds_metadata->Encode(&bytes);
  auto ikey = internalKeyFromDownstreamKey(ns_src_key, src_metadata, dst_key);
  s = batch->Put(ikey, bytes);
  if (!s.ok()) return s;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::getDownStreamRules(engine::Context &ctx, const Slice &ns_src_key,
                                               const TimeSeriesMetadata &src_metadata, std::vector<std::string> *keys,
                                               std::vector<TSDownStreamMeta> *metas) {
  std::string prefix = internalKeyFromDownstreamKey(ns_src_key, src_metadata, "");
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  keys->clear();
  if (metas != nullptr) {
    metas->clear();
  }
  for (iter->Seek(lower_bound); iter->Valid() && iter->key().starts_with(prefix); iter->Next()) {
    keys->push_back(iter->key().ToString());
    if (metas != nullptr) {
      TSDownStreamMeta meta;
      Slice slice = iter->value().ToStringView();
      meta.Decode(&slice);
      metas->push_back(meta);
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::getTSKeyByFilter(engine::Context &ctx, const TSMGetOption::FilterOption &filter,
                                             std::vector<std::string> *user_keys, std::vector<LabelKVList> *labels_vec,
                                             std::vector<TimeSeriesMetadata> *metas) {
  std::set<std::string> temp_keys;
  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  auto rev_index_upper_bound = TSRevLabelKey::UpperBound(namespace_);
  for (const auto &[label_k, label_v_set] : filter.labels_equals) {
    if (label_v_set.empty()) {
      continue;
    }
    for (const auto &label_v : label_v_set) {
      auto rev_label_key = TSRevLabelKey(namespace_, label_k, label_v);
      auto rev_index_prefix = rev_label_key.Encode();

      Slice lower_bound(rev_index_prefix);
      read_options.iterate_lower_bound = &lower_bound;
      Slice upper_bound(rev_index_upper_bound);
      read_options.iterate_upper_bound = &upper_bound;

      auto iter = util::UniqueIterator(ctx, read_options, index_cf_handle_);
      for (iter->Seek(lower_bound); iter->Valid() && iter->key().starts_with(rev_index_prefix); iter->Next()) {
        auto user_key = iter->key();
        user_key.remove_prefix(rev_index_prefix.size());
        temp_keys.emplace(user_key.data(), user_key.size());
      }
    }
  }

  // Filter
  user_keys->clear();
  user_keys->reserve(temp_keys.size());
  if (labels_vec != nullptr) {
    labels_vec->clear();
    labels_vec->reserve(temp_keys.size());
  }
  if (metas != nullptr) {
    metas->clear();
    metas->reserve(temp_keys.size());
  }
  for (auto &user_key : temp_keys) {
    std::string ns_key = AppendNamespacePrefix(user_key);
    TimeSeriesMetadata metadata;
    auto s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
    if (!s.ok()) continue;

    LabelKVList labels;
    getLabelKVList(ctx, ns_key, metadata, &labels);
    std::unordered_map<std::string_view, std::string *> label_map;
    for (auto &label : labels) {
      label_map[label.k] = &label.v;
    }

    // Check labels_equals conditions
    bool match = std::all_of(filter.labels_equals.begin(), filter.labels_equals.end(), [&label_map](const auto &kv) {
      auto it = label_map.find(kv.first);
      // If labels_equals value set is empty, means the label key must not exist
      return (kv.second.empty() && it == label_map.end()) ||
             (it != label_map.end() && kv.second.count(*(it->second)) > 0);
    });
    if (!match) continue;

    // Check labels_not_equals conditions
    match = std::all_of(filter.labels_not_equals.begin(), filter.labels_not_equals.end(), [&label_map](const auto &kv) {
      auto it = label_map.find(kv.first);
      const std::string &str = (it != label_map.end()) ? *(it->second) : "";
      return kv.second.count(str) == 0;
    });
    if (!match) continue;

    user_keys->push_back(user_key);
    if (labels_vec != nullptr) {
      labels_vec->push_back(std::move(labels));
    }
    if (metas != nullptr) {
      metas->push_back(std::move(metadata));
    }
  }
  return rocksdb::Status::OK();
}

std::string TimeSeries::internalKeyFromChunkID(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                               uint64_t id) const {
  std::string sub_key;
  PutFixed8(&sub_key, static_cast<uint8_t>(TSSubkeyType::CHUNK));
  PutFixed64(&sub_key, id);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TimeSeries::internalKeyFromLabelKey(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                                Slice label_key) const {
  std::string sub_key;
  sub_key.resize(1 + label_key.size());
  auto buf = sub_key.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(TSSubkeyType::LABEL));
  EncodeBuffer(buf, label_key);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

std::string TimeSeries::internalKeyFromDownstreamKey(const Slice &ns_key, const TimeSeriesMetadata &metadata,
                                                     Slice downstream_key) const {
  std::string sub_key;
  sub_key.resize(1 + downstream_key.size());
  auto buf = sub_key.data();
  buf = EncodeFixed8(buf, static_cast<uint8_t>(TSSubkeyType::DOWNSTREAM));
  EncodeBuffer(buf, downstream_key);

  return InternalKey(ns_key, sub_key, metadata.version, storage_->IsSlotIdEncoded()).Encode();
}

uint64_t TimeSeries::chunkIDFromInternalKey(Slice internal_key) {
  auto size = internal_key.size();
  internal_key.remove_prefix(size - sizeof(uint64_t));
  return DecodeFixed64(internal_key.data());
}

std::string TimeSeries::labelKeyFromInternalKey(Slice internal_key) const {
  auto key = InternalKey(internal_key, storage_->IsSlotIdEncoded());
  auto label_key = key.GetSubKey();
  label_key.remove_prefix(sizeof(TSSubkeyType));
  return label_key.ToString();
}

std::string TimeSeries::downstreamKeyFromInternalKey(Slice internal_key) const {
  auto key = InternalKey(internal_key, storage_->IsSlotIdEncoded());
  auto ds_key = key.GetSubKey();
  ds_key.remove_prefix(sizeof(TSSubkeyType));
  return ds_key.ToString();
}

rocksdb::Status TimeSeries::Create(engine::Context &ctx, const Slice &user_key, const TSCreateOption &option) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata;
  auto s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (s.ok()) {
    return rocksdb::Status::InvalidArgument("key already exists");
  }
  return createTimeSeries(ctx, ns_key, &metadata, &option);
}

rocksdb::Status TimeSeries::Add(engine::Context &ctx, const Slice &user_key, TSSample sample,
                                const TSCreateOption &option, AddResult *res, const DuplicatePolicy *on_dup_policy) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getOrCreateTimeSeries(ctx, ns_key, &metadata, &option);
  if (!s.ok()) return s;
  auto sample_batch = SampleBatch({sample}, on_dup_policy ? *on_dup_policy : metadata.duplicate_policy);

  DownstreamUpsertArgs ds_args;
  s = upsertCommon(ctx, ns_key, metadata, sample_batch, &ds_args);
  if (!s.ok()) return s;
  s = upsertDownStream(ctx, ns_key, metadata, ds_args);
  if (!s.ok()) return s;
  *res = sample_batch.GetFinalResults()[0];
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::MAdd(engine::Context &ctx, const Slice &user_key, std::vector<TSSample> samples,
                                 std::vector<AddResult> *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  auto sample_batch = SampleBatch(std::move(samples), metadata.duplicate_policy);
  DownstreamUpsertArgs ds_args;
  s = upsertCommon(ctx, ns_key, metadata, sample_batch, &ds_args);
  if (!s.ok()) return s;
  s = upsertDownStream(ctx, ns_key, metadata, ds_args);
  if (!s.ok()) return s;
  *res = sample_batch.GetFinalResults();
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::Info(engine::Context &ctx, const Slice &user_key, TSInfoResult *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &res->metadata);
  if (!s.ok()) {
    return s;
  }
  auto chunk_count = res->metadata.size;
  auto &metadata = res->metadata;
  // Approximate total samples
  res->total_samples = chunk_count * res->metadata.chunk_size;
  // TODO: Estimate disk usage for the field `memoryUsage`
  res->memory_usage = 0;
  // Retrieve the first and last timestamp
  std::string chunk_upper_bound = internalKeyFromLabelKey(ns_key, metadata, "");
  std::string end_key = internalKeyFromChunkID(ns_key, metadata, TSSample::MAX_TIMESTAMP);
  std::string prefix = end_key.substr(0, end_key.size() - sizeof(uint64_t));

  rocksdb::ReadOptions read_options = ctx.DefaultScanOptions();
  rocksdb::Slice upper_bound(chunk_upper_bound);
  read_options.iterate_upper_bound = &upper_bound;
  rocksdb::Slice lower_bound(prefix);
  read_options.iterate_lower_bound = &lower_bound;

  auto iter = util::UniqueIterator(ctx, read_options);
  iter->SeekForPrev(end_key);
  if (!iter->Valid() || !iter->key().starts_with(prefix)) {
    // no chunk
    res->first_timestamp = 0;
    res->last_timestamp = 0;
  } else {
    auto chunk = CreateTSChunkFromData(iter->value());
    res->last_timestamp = chunk->GetLastTimestamp();
    // Get the first timestamp
    TSRangeOption range_option;
    range_option.count_limit = 1;
    std::vector<TSSample> samples;
    s = rangeCommon(ctx, ns_key, metadata, range_option, &samples);
    if (!s.ok()) return s;
    CHECK(samples.size() == 1);
    res->first_timestamp = samples[0].ts;
  }
  getLabelKVList(ctx, ns_key, metadata, &res->labels);

  // Retrieve downstream downstream_rules
  std::vector<std::string> downstream_keys;
  std::vector<TSDownStreamMeta> downstream_rules;
  getDownStreamRules(ctx, ns_key, metadata, &downstream_keys, &downstream_rules);
  for (size_t i = 0; i < downstream_keys.size(); i++) {
    auto key = downstreamKeyFromInternalKey(downstream_keys[i]);
    res->downstream_rules.emplace_back(std::move(key), std::move(downstream_rules[i]));
  }

  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::Range(engine::Context &ctx, const Slice &user_key, const TSRangeOption &option,
                                  std::vector<TSSample> *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  s = rangeCommon(ctx, ns_key, metadata, option, res);
  return s;
}

rocksdb::Status TimeSeries::Get(engine::Context &ctx, const Slice &user_key, bool is_return_latest,
                                std::vector<TSSample> *res) {
  res->clear();
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) {
    return s;
  }
  s = getCommon(ctx, ns_key, metadata, is_return_latest, res);
  return s;
}

rocksdb::Status TimeSeries::CreateRule(engine::Context &ctx, const Slice &src_key, const Slice &dst_key,
                                       const TSAggregator &aggregator, TSCreateRuleResult *res) {
  if (src_key == dst_key) {
    *res = TSCreateRuleResult::kSrcEqDst;
    return rocksdb::Status::OK();
  }
  std::string ns_src_key = AppendNamespacePrefix(src_key);
  TimeSeriesMetadata src_metadata;
  auto s = getTimeSeriesMetadata(ctx, ns_src_key, &src_metadata);
  if (!s.ok()) {
    *res = TSCreateRuleResult::kSrcNotExist;
    return rocksdb::Status::OK();
  }
  TimeSeriesMetadata dst_metadata;
  std::string ns_dst_key = AppendNamespacePrefix(dst_key);
  s = getTimeSeriesMetadata(ctx, ns_dst_key, &dst_metadata);
  if (!s.ok()) {
    *res = TSCreateRuleResult::kDstNotExist;
    return rocksdb::Status::OK();
  }

  if (src_metadata.source_key.size()) {
    *res = TSCreateRuleResult::kSrcHasSourceRule;
    return rocksdb::Status::OK();
  }
  if (dst_metadata.source_key.size()) {
    *res = TSCreateRuleResult::kDstHasSourceRule;
    return rocksdb::Status::OK();
  }
  std::vector<std::string> dst_ds_keys;
  s = getDownStreamRules(ctx, ns_dst_key, dst_metadata, &dst_ds_keys);
  if (!s.ok()) return s;
  if (dst_ds_keys.size()) {
    *res = TSCreateRuleResult::kDstHasDestRule;
    return rocksdb::Status::OK();
  }

  // Create downstream metadata
  auto batch = storage_->GetWriteBatchBase();
  WriteBatchLogData log_data(kRedisTimeSeries);
  s = batch->PutLogData(log_data.Encode());
  if (!s.ok()) return s;

  TSDownStreamMeta downstream_metadata;
  s = createDownStreamMetadataInBatch(ctx, ns_src_key, dst_key, src_metadata, aggregator, batch, &downstream_metadata);
  if (!s.ok()) return s;
  dst_metadata.SetSourceKey(src_key);

  std::string bytes;
  dst_metadata.Encode(&bytes);
  s = batch->Put(metadata_cf_handle_, ns_dst_key, bytes);
  if (!s.ok()) return s;

  *res = TSCreateRuleResult::kOK;
  return storage_->Write(ctx, storage_->DefaultWriteOptions(), batch->GetWriteBatch());
}

rocksdb::Status TimeSeries::MGet(engine::Context &ctx, const TSMGetOption &option, bool is_return_latest,
                                 std::vector<TSMGetResult> *res) {
  std::vector<std::string> user_keys;
  std::vector<LabelKVList> labels_vec;
  std::vector<TimeSeriesMetadata> metas;

  auto s = getTSKeyByFilter(ctx, option.filter, &user_keys, &labels_vec, &metas);
  if (!s.ok()) return s;

  res->resize(user_keys.size());
  for (size_t i = 0; i < user_keys.size(); i++) {
    std::string ns_key = AppendNamespacePrefix(user_keys[i]);
    auto &res_i = (*res)[i];
    auto &metadata = metas[i];
    auto &labels = labels_vec[i];

    s = getCommon(ctx, ns_key, metadata, is_return_latest, &res_i.samples);
    if (!s.ok()) return s;
    res_i.name = std::move(user_keys[i]);
    if (option.with_labels) {
      res_i.labels = std::move(labels);
    } else if (!option.selected_labels.empty()) {
      res_i.labels = ExtractSelectedLabels(std::move(labels), option.selected_labels);
    }
  }
  return s;
}

rocksdb::Status TimeSeries::MRange(engine::Context &ctx, const TSMRangeOption &option,
                                   std::vector<TSMRangeResult> *res) {
  std::vector<std::string> user_keys;
  std::vector<LabelKVList> labels_vec;
  std::vector<TimeSeriesMetadata> metas;

  auto s = getTSKeyByFilter(ctx, option.filter, &user_keys, &labels_vec, &metas);
  if (!s.ok()) return s;

  res->clear();
  res->reserve(user_keys.size());
  // Group
  using GroupReducerType = TSMRangeOption::GroupReducerType;
  bool is_group_by = option.group_by_label.size() && option.reducer != GroupReducerType::NONE;
  std::map<std::string_view, std::vector<size_t>> group_map;
  if (is_group_by) {
    for (size_t i = 0; i < user_keys.size(); i++) {
      auto &labels = labels_vec[i];
      auto it = std::lower_bound(labels.begin(), labels.end(), option.group_by_label,
                                 [](const LabelKVPair &label, const std::string &key) { return label.k < key; });
      if (it != labels.end() && it->k == option.group_by_label) {
        group_map[it->v].push_back(i);
      }
    }
    if (group_map.empty()) {
      // No matched group
      return rocksdb::Status::OK();
    }
  }

  if (is_group_by) {
    for (const auto &[group_value, indices] : group_map) {
      TSMRangeResult group_res;
      // Labels
      LabelKVList group_labels = {LabelKVPair{option.group_by_label, std::string(group_value)}};
      if (option.with_labels) {
        group_res.labels = std::move(group_labels);
      } else if (option.selected_labels.size()) {
        group_res.labels = ExtractSelectedLabels(std::move(group_labels), option.selected_labels);
      }
      // Samples
      std::vector<std::vector<TSSample>> all_samples;
      all_samples.reserve(indices.size());
      for (size_t i : indices) {
        std::vector<TSSample> samples;
        s = rangeCommon(ctx, AppendNamespacePrefix(user_keys[i]), metas[i], option, &samples);
        if (!s.ok()) return s;
        all_samples.push_back(std::move(samples));
      }
      group_res.samples = GroupSamplesAndReduce(all_samples, option.reducer);
      // Sources
      for (size_t i : indices) {
        group_res.source_keys.push_back(std::move(user_keys[i]));
      }
      // Name
      group_res.name = group_value;

      res->push_back(std::move(group_res));
    }
  } else {
    for (size_t i = 0; i < user_keys.size(); i++) {
      TSMRangeResult group_res;
      // Labels
      if (option.with_labels) {
        group_res.labels = std::move(labels_vec[i]);
      } else if (option.selected_labels.size()) {
        group_res.labels = ExtractSelectedLabels(std::move(labels_vec[i]), option.selected_labels);
      }
      // Samples
      s = rangeCommon(ctx, AppendNamespacePrefix(user_keys[i]), metas[i], option, &group_res.samples);
      if (!s.ok()) return s;
      // Name
      group_res.name = std::move(user_keys[i]);

      res->push_back(std::move(group_res));
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::IncrBy(engine::Context &ctx, const Slice &user_key, TSSample sample,
                                   const TSCreateOption &option, AddResult *res) {
  std::string ns_key = AppendNamespacePrefix(user_key);

  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getOrCreateTimeSeries(ctx, ns_key, &metadata, &option);
  if (!s.ok()) return s;

  std::vector<TSSample> get_samples;
  s = getCommon(ctx, ns_key, metadata, true, &get_samples);
  if (!s.ok()) return s;
  if (get_samples.size() && sample < get_samples.back()) {
    res->type = TSChunk::AddResultType::kOld;
    return rocksdb::Status::OK();
  }

  if (get_samples.size()) {
    sample.v += get_samples.back().v;
  }
  auto sample_batch = SampleBatch({sample}, DuplicatePolicy::LAST);

  DownstreamUpsertArgs ds_args;
  s = upsertCommon(ctx, ns_key, metadata, sample_batch, &ds_args);
  if (!s.ok()) return s;
  s = upsertDownStream(ctx, ns_key, metadata, ds_args);
  if (!s.ok()) return s;
  *res = sample_batch.GetFinalResults()[0];
  return rocksdb::Status::OK();
}

rocksdb::Status TimeSeries::Del(engine::Context &ctx, const Slice &user_key, uint64_t from, uint64_t to,
                                uint64_t *deleted) {
  std::string ns_key = AppendNamespacePrefix(user_key);
  TimeSeriesMetadata metadata(false);
  rocksdb::Status s = getTimeSeriesMetadata(ctx, ns_key, &metadata);
  if (!s.ok()) return s;

  // Get downstream rules
  std::vector<std::string> ds_keys;
  std::vector<TSDownStreamMeta> ds_metas;
  s = getDownStreamRules(ctx, ns_key, metadata, &ds_keys, &ds_metas);
  if (!s.ok()) return s;

  // Check retention and compaction rules
  std::vector<TSSample> get_samples;
  s = getCommon(ctx, ns_key, metadata, true, &get_samples);
  if (!s.ok()) return s;
  if (get_samples.empty()) return rocksdb::Status::OK();
  uint64_t last_ts = get_samples.back().ts;
  uint64_t retention_bound =
      (metadata.retention_time > 0 && metadata.retention_time < last_ts) ? last_ts - metadata.retention_time : 0;
  for (const auto &ds_meta : ds_metas) {
    const auto &agg = ds_meta.aggregator;
    if (agg.CalculateAlignedBucketLeft(from) < retention_bound) {
      return rocksdb::Status::InvalidArgument(
          "When a series has compactions, deleting samples or compaction buckets beyond the series retention period is "
          "not possible");
    }
  }

  s = delRangeCommon(ctx, ns_key, metadata, from, to, deleted);
  if (!s.ok()) return s;
  if (*deleted == 0) return rocksdb::Status::OK();
  s = delRangeDownStream(ctx, ns_key, metadata, ds_keys, ds_metas, from, to);
  return s;
}

}  // namespace redis
