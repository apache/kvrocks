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

#include <fmt/format.h>

#include <algorithm>
#include <cmath>
#include <iterator>
#include <limits>
#include <map>
#include <vector>

#include "common/status.h"

struct Centroid {
  double mean;
  double weight = 1.0;

  // merge with another centroid
  void Merge(const Centroid& centroid) {
    weight += centroid.weight;
    mean += (centroid.mean - mean) * centroid.weight / weight;
  }

  std::string ToString() const { return fmt::format("centroid<mean: {}, weight: {}>", mean, weight); }

  explicit Centroid() = default;
  explicit Centroid(double mean, double weight) : mean(mean), weight(weight) {}
};

struct CentroidsWithDelta {
  std::vector<Centroid> centroids;
  uint64_t delta = 0;
  double min = std::numeric_limits<double>::max();
  double max = std::numeric_limits<double>::lowest();
  double total_weight = 0;
};

StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<CentroidsWithDelta>& centroids_list, uint64_t delta);
StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<double>& buffer,
                                          const std::vector<CentroidsWithDelta>& centroids_lists, uint64_t delta);
StatusOr<CentroidsWithDelta> TDigestMerge(const std::vector<double>& buffer, const CentroidsWithDelta& centroid_list);

/**

TD should looks like below:
class TDSample {
  public:
  struct Iterator {
    Iterator* Clone() const;
    bool Next();
    bool Valid() const;
    StatusOr<Centroid> GetCentroid() const;
  };
  Iterator* Begin();
  Iterator* End();
  double TotalWeight();
  double Min() const;
  double Max() const;
};

**/

// a numerically stable lerp is unbelievably complex
// but we are *approximating* the quantile, so let's keep it simple
// reference:
// https://github.com/apache/arrow/blob/27bbd593625122a4a25d9471c8aaf5df54a6dcf9/cpp/src/arrow/util/tdigest.cc#L38
static inline double Lerp(double a, double b, double t) { return a + t * (b - a); }

template <typename TD>
inline StatusOr<double> TDigestQuantile(TD&& td, double q) {
  if (q < 0 || q > 1 || td.Size() == 0) {
    return Status{Status::InvalidArgument, "invalid quantile or empty tdigest"};
  }

  const double index = q * td.TotalWeight();
  if (index <= 1) {
    return td.Min();
  } else if (index >= td.TotalWeight() - 1) {
    return td.Max();
  }

  // find centroid contains the index
  double weight_sum = 0;
  auto iter = td.Begin();
  for (; iter->Valid(); iter->Next()) {
    weight_sum += GET_OR_RET(iter->GetCentroid()).weight;
    if (index <= weight_sum) {
      break;
    }
  }

  // since index is in (1, total_weight - 1), iter should be valid
  if (!iter->Valid()) {
    return Status{Status::InvalidArgument, "invalid iterator during decoding tdigest centroid"};
  }

  auto centroid = GET_OR_RET(iter->GetCentroid());

  // deviation of index from the centroid center
  double diff = index + centroid.weight / 2 - weight_sum;

  // index happen to be in a unit weight centroid
  if (centroid.weight == 1 && std::abs(diff) < 0.5) {
    return centroid.mean;
  }

  // find adjacent centroids for interpolation
  auto ci_left = iter->Clone();
  auto ci_right = iter->Clone();
  if (diff > 0) {
    if (ci_right == td.End()) {
      // index larger than center of last bin
      auto c = GET_OR_RET(ci_left->GetCentroid());
      CHECK(c.weight >= 2);
      return Lerp(c.mean, td.Max(), diff / (c.weight / 2));
    }
    ci_right->Next();
  } else {
    if (ci_left == td.Begin()) {
      // index smaller than center of first bin
      auto c = GET_OR_RET(ci_left->GetCentroid());
      CHECK(c.weight >= 2);
      return Lerp(td.Min(), c.mean, index / (c.weight / 2));
    }
    ci_left->Prev();
    auto lc = GET_OR_RET(ci_left->GetCentroid());
    auto rc = GET_OR_RET(ci_right->GetCentroid());
    diff += lc.weight / 2 + rc.weight / 2;
  }

  auto lc = GET_OR_RET(ci_left->GetCentroid());
  auto rc = GET_OR_RET(ci_right->GetCentroid());

  // interpolate from adjacent centroids
  diff /= (lc.weight / 2 + rc.weight / 2);
  return Lerp(lc.mean, rc.mean, diff);
}

inline int DoubleCompare(double a, double b, double rel_eps = 1e-12, double abs_eps = 1e-9) {
  double diff = a - b;
  double adiff = std::abs(diff);
  if (adiff <= abs_eps) return 0;
  double maxab = std::max(std::abs(a), std::abs(b));
  if (adiff <= maxab * rel_eps) return 0;
  return (diff < 0) ? -1 : 1;
}

inline bool DoubleEqual(double a, double b, double rel_eps = 1e-12, double abs_eps = 1e-9) {
  return DoubleCompare(a, b, rel_eps, abs_eps) == 0;
}

struct DoubleComparator {
  bool operator()(const double& a, const double& b) const { return DoubleCompare(a, b) == -1; }
};

// Match RedisBloom t-digest-c CDF behavior: if min/max is outside the first/last centroid mean, the exact
// boundary sample is treated as a singleton with weight 1. Its center rank is 0.5 at min and
// total_weight - 0.5 at max; interpolation toward an inner centroid starts after the singleton, at rank 1 or
// total_weight - 1.
// refer to implementation:
// https://github.com/RedisBloom/t-digest-c/blob/50edef336eb27ed5b19e7f9be05494683ca58515/src/tdigest.c#L223
inline Status TDigestCDF(const std::vector<Centroid>& centroids, double centroids_min, double centroids_max,
                         double total_weight, const std::vector<double>& inputs, std::vector<double>* result) {
  if (centroids.empty() || total_weight <= 0) {
    return Status{Status::InvalidArgument, "invalid or empty tdigest"};
  }

  std::map<double, std::vector<size_t>> sorted_unique_input_idx_map;
  for (size_t i = 0; i < inputs.size(); ++i) {
    sorted_unique_input_idx_map[inputs[i]].push_back(i);
  }

  std::vector<double> sorted_unique_inputs;
  sorted_unique_inputs.reserve(sorted_unique_input_idx_map.size());
  std::transform(sorted_unique_input_idx_map.cbegin(), sorted_unique_input_idx_map.cend(),
                 std::back_inserter(sorted_unique_inputs), [](const auto& pair) { return pair.first; });

  constexpr double kSingletonBoundaryWeight = 1.0;
  constexpr double kHalfSingletonBoundaryWeight = kSingletonBoundaryWeight / 2;

  std::vector<double> sorted_result_weights;
  sorted_result_weights.reserve(sorted_unique_inputs.size());
  if (centroids.size() == 1) {
    // only one centroid, min should equal max, and all inputs should be either less than, equal to,
    // or greater than the centroid mean
    const double width = centroids_max - centroids_min;
    for (const auto input : sorted_unique_inputs) {
      if (input < centroids_min) {
        sorted_result_weights.push_back(0.0);
        continue;
      }

      if (input > centroids_max) {
        sorted_result_weights.push_back(total_weight);
        continue;
      }

      if (input - centroids_min <= width) {
        // min and max are too close to do any viable interpolation, treat the centroid as a singleton
        sorted_result_weights.push_back(total_weight / 2);
      } else {
        // interpolate if somehow we have weight > 0 and max != min, which should not happen in a valid tdigest
        sorted_result_weights.push_back((input - centroids_min) / width * total_weight);
      }
    }
  } else {
    auto first_valid_input_it = std::find_if(sorted_unique_inputs.cbegin(), sorted_unique_inputs.cend(),
                                             [centroids_min](double input) { return input >= centroids_min; });
    auto last_valid_input_it = std::find_if(sorted_unique_inputs.crbegin(), sorted_unique_inputs.crend(),
                                            [centroids_max](double input) { return input <= centroids_max; });
    auto input_idx = (first_valid_input_it == sorted_unique_inputs.cend())
                         ? sorted_unique_inputs.size()
                         : std::distance(sorted_unique_inputs.cbegin(), first_valid_input_it);
    auto last_valid_input_idx = (last_valid_input_it == sorted_unique_inputs.crend())
                                    ? 0
                                    : std::distance(sorted_unique_inputs.cbegin(), last_valid_input_it.base());

    // fill in 0 for inputs less than the min boundary
    for (auto i = 0; i < input_idx; ++i) {
      sorted_result_weights.push_back(0.);
    }

    size_t centroid_idx = 0;

    // greater than the min boundary, but less than the first centroid mean
    while (centroid_idx == 0 && input_idx < last_valid_input_idx &&
           sorted_unique_inputs[input_idx] < centroids[centroid_idx].mean) {
      auto cdf_input = sorted_unique_inputs[input_idx];
      auto current_centroid = centroids[centroid_idx];
      const auto width = current_centroid.mean - centroids_min;
      double interpolated_weight = std::numeric_limits<double>::quiet_NaN();
      if (width > 0) {
        if (cdf_input == centroids_min) {
          interpolated_weight = kHalfSingletonBoundaryWeight;
        } else {
          // there must be a singleton at the min boundary, so the interpolation starts after it, at rank 1
          interpolated_weight =
              Lerp(kHalfSingletonBoundaryWeight, current_centroid.weight / 2, (cdf_input - centroids_min) / width);
        }
      } else {
        // this should be redundant of the check cdf_input < centroids_min, but for clarity
        interpolated_weight = 0.;
      }
      sorted_result_weights.push_back(interpolated_weight);
      ++input_idx;
    }

    double weight_so_far = 0.;
    while (centroid_idx < centroids.size() - 1 && input_idx < last_valid_input_idx) {
      auto cdf_input = sorted_unique_inputs[input_idx];
      auto current_centroid = centroids[centroid_idx];
      auto next_centroid = centroids[centroid_idx + 1];

      if (cdf_input == current_centroid.mean) {
        double dw = 0.;
        auto same_mean_idx = centroid_idx;
        while (same_mean_idx < centroids.size() && centroids[same_mean_idx].mean == current_centroid.mean) {
          dw += centroids[same_mean_idx].weight;
          ++same_mean_idx;
        }
        sorted_result_weights.push_back(weight_so_far + dw / 2);
        ++input_idx;
        continue;
      }

      if (current_centroid.mean < cdf_input && cdf_input < next_centroid.mean) {
        if (next_centroid.mean - current_centroid.mean > 0) {
          double left_exclude_weight = 0;
          double right_exclude_weight = 0;
          if (current_centroid.weight == kSingletonBoundaryWeight) {
            if (next_centroid.weight == kSingletonBoundaryWeight) {
              // both adjacent centroids are singletons, include the left exact sample and exclude the right one.
              sorted_result_weights.push_back(weight_so_far + kSingletonBoundaryWeight);
              // weight_so_far += current_centroid.weight;
              ++input_idx;
              continue;
            } else {
              left_exclude_weight = kHalfSingletonBoundaryWeight;
            }
          } else if (next_centroid.weight == kSingletonBoundaryWeight) {
            right_exclude_weight = kHalfSingletonBoundaryWeight;
          }

          double dw = (current_centroid.weight + next_centroid.weight) / 2;
          double dw_no_singleton = dw - left_exclude_weight - right_exclude_weight;
          double base_weight = weight_so_far + current_centroid.weight / 2 + left_exclude_weight;
          auto interpolated_weight =
              Lerp(base_weight, base_weight + dw_no_singleton,
                   (cdf_input - current_centroid.mean) / (next_centroid.mean - current_centroid.mean));
          sorted_result_weights.push_back(interpolated_weight);
          ++input_idx;
        }
        continue;
      }

      ++centroid_idx;
      weight_so_far += current_centroid.weight;
    }

    while (centroid_idx == centroids.size() - 1 && input_idx < last_valid_input_idx &&
           sorted_unique_inputs[input_idx] < centroids[centroid_idx].mean) {
      auto cdf_input = sorted_unique_inputs[input_idx];
      auto current_centroid = centroids[centroid_idx];
      const auto width = current_centroid.mean - centroids_min;
      double interpolated_weight = std::numeric_limits<double>::quiet_NaN();
      if (width > 0) {
        if (cdf_input == centroids_min) {
          interpolated_weight = kHalfSingletonBoundaryWeight;
        } else {
          // there must be a singleton at the min boundary, so the interpolation starts after it, at rank
          // kHalfSingletonBoundaryWeight
          interpolated_weight =
              Lerp(kHalfSingletonBoundaryWeight, current_centroid.weight / 2, (cdf_input - centroids_min) / width);
        }
      } else {
        // this should be redundant of the check cdf_input < centroids_min, but for clarity
        interpolated_weight = 0;
      }
      sorted_result_weights.push_back(interpolated_weight);
      ++input_idx;
    }

    // fill in 1 for inputs greater than the max boundary
    while (input_idx < sorted_unique_inputs.size()) {
      // handle remaining inputs
      sorted_result_weights.push_back(total_weight);
      ++input_idx;
    }
  }

  result->clear();
  result->resize(inputs.size(), std::numeric_limits<double>::quiet_NaN());
  for (size_t i = 0; i < sorted_unique_inputs.size(); ++i) {
    for (auto idx : sorted_unique_input_idx_map[sorted_unique_inputs[i]]) {
      (*result)[idx] = std::clamp(sorted_result_weights[i] / total_weight, 0.0, 1.0);
    }
  }

  return Status::OK();
}

template <bool Reverse, typename TD>
inline Status TDigestByRank(TD&& td, const std::vector<int>& inputs, std::vector<double>* result) {
  result->clear();
  result->resize(inputs.size(), std::numeric_limits<double>::quiet_NaN());

  std::map<int, size_t> rank_to_index;
  for (size_t i = 0; i < inputs.size(); ++i) {
    rank_to_index[inputs[i]] = i;
  }

  auto it = rank_to_index.begin();
  auto is_end = [&it, &rank_to_index]() -> bool { return it == rank_to_index.end(); };
  auto iter = td.Begin();
  double cumulative_weight = 0;
  while (iter->Valid() && !is_end()) {
    auto centroid = GET_OR_RET(iter->GetCentroid());
    cumulative_weight += centroid.weight;
    while (!is_end() && it->first < static_cast<int>(cumulative_weight)) {
      (*result)[it->second] = centroid.mean;
      ++it;
    }
    iter->Next();
  }

  while (!is_end() && it->first >= static_cast<int>(td.TotalWeight())) {
    if constexpr (Reverse) {
      (*result)[it->second] = -std::numeric_limits<double>::infinity();
    } else {
      (*result)[it->second] = std::numeric_limits<double>::infinity();
    }
    ++it;
  }

  // check if all results are valid
  for (auto r : *result) {
    if (std::isnan(r)) {
      return Status{Status::InvalidArgument, "invalid result when getting byrank or byrevrank"};
    }
  }
  return Status::OK();
}

template <bool Reverse, typename TD>
inline Status TDigestRank(TD&& td, const std::vector<double>& inputs, std::vector<int>* result) {
  std::map<double, size_t, DoubleComparator> value_to_index;
  for (size_t i = 0; i < inputs.size(); ++i) {
    value_to_index[inputs[i]] = i;
  }

  result->clear();
  result->resize(inputs.size(), -2);

  using MapType = decltype(value_to_index);
  using IterType = std::conditional_t<Reverse, typename MapType::reverse_iterator, typename MapType::iterator>;
  IterType it;
  if constexpr (Reverse) {
    it = value_to_index.rbegin();
  } else {
    it = value_to_index.begin();
  }

  auto is_end = [&it, &value_to_index]() -> bool {
    if constexpr (Reverse) {
      return it == value_to_index.rend();
    } else {
      return it == value_to_index.end();
    }
  };

  // handle inputs larger than maximum in reverse order or smaller than minimum in forward order
  if constexpr (Reverse) {
    while (!is_end() && it->first > td.Max()) {
      (*result)[it->second] = -1;
      ++it;
    }
  } else {
    while (!is_end() && it->first < td.Min()) {
      (*result)[it->second] = -1;
      ++it;
    }
  }

  auto iter = td.Begin();
  double cumulative_weight = 0;
  while (iter->Valid() && !is_end()) {
    auto centroid = GET_OR_RET(iter->GetCentroid());
    auto input_value = it->first;
    if (DoubleEqual(centroid.mean, input_value)) {
      auto current_mean = centroid.mean;
      auto current_mean_cumulative_weight = cumulative_weight + centroid.weight / 2;
      cumulative_weight += centroid.weight;

      // handle all next centroids which has the same mean
      while (iter->Next()) {
        auto next_centroid = GET_OR_RET(iter->GetCentroid());
        if (!DoubleEqual(current_mean, next_centroid.mean)) {
          // move back to the last equal centroid, because we will process it in the next loop
          iter->Prev();
          break;
        }
        current_mean_cumulative_weight += next_centroid.weight / 2;
        cumulative_weight += next_centroid.weight;
      }

      (*result)[it->second] = static_cast<int>(current_mean_cumulative_weight);
      ++it;
      iter->Next();
    } else if constexpr (Reverse) {
      if (DoubleCompare(centroid.mean, input_value) > 0) {
        cumulative_weight += centroid.weight;
        iter->Next();
      } else {
        (*result)[it->second] = static_cast<int>(cumulative_weight);
        ++it;
      }
    } else {
      if (DoubleCompare(centroid.mean, input_value) < 0) {
        cumulative_weight += centroid.weight;
        iter->Next();
      } else {
        (*result)[it->second] = static_cast<int>(cumulative_weight);
        ++it;
      }
    }
  }

  while (!is_end()) {
    (*result)[it->second] = static_cast<int>(td.TotalWeight());
    ++it;
  }

  for (auto r : *result) {
    if (r <= -2) {
      return Status{Status::InvalidArgument, "invalid result when computing rank or revrank"};
    }
  }
  return Status::OK();
}

template <typename TD>
inline StatusOr<double> TDigestTrimmedMean(TD&& td, double low_cut_quantile, double high_cut_quantile) {
  if (td.Size() == 0) {
    return std::numeric_limits<double>::quiet_NaN();
  }

  const double total_weight = td.TotalWeight();
  const double leftmost_weight = std::floor(total_weight * low_cut_quantile);
  const double rightmost_weight = std::ceil(total_weight * high_cut_quantile);

  double count_done = 0.0;
  double trimmed_sum = 0.0;
  double trimmed_count = 0.0;

  auto iter = td.Begin();
  while (iter->Valid()) {
    auto centroid = GET_OR_RET(iter->GetCentroid());
    const double n_weight = centroid.weight;
    double count_add = n_weight;

    // Keep only the portion of this centroid that overlaps with the trimmed rank range.
    count_add -= std::min(std::max(0.0, leftmost_weight - count_done), count_add);
    count_add = std::min(std::max(0.0, rightmost_weight - count_done), count_add);

    count_done += n_weight;

    trimmed_sum += centroid.mean * count_add;
    trimmed_count += count_add;

    if (count_done >= rightmost_weight) {
      break;
    }

    iter->Next();
  }

  if (trimmed_count == 0.0) {
    return std::numeric_limits<double>::quiet_NaN();
  }

  return trimmed_sum / trimmed_count;
}
