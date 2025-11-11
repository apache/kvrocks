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

#include <map>
#include <numeric>
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
  uint64_t delta;
  double min;
  double max;
  double total_weight;
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

template <typename TD>
inline Status TDigestRevRank(TD&& td, const std::vector<double>& inputs, std::vector<int>& result) {
  std::map<double, size_t, DoubleComparator> value_to_indices;
  for (size_t i = 0; i < inputs.size(); ++i) {
    value_to_indices[inputs[i]] = i;
  }

  result.clear();
  result.resize(inputs.size(), -2);
  auto it = value_to_indices.rbegin();

  // handle inputs larger than maximum
  while (it != value_to_indices.rend() && it->first > td.Max()) {
    result[it->second] = -1;
    ++it;
  }

  auto iter = td.End();
  double cumulative_weight = 0;
  while (iter->Valid() && it != value_to_indices.rend()) {
    auto centroid = GET_OR_RET(iter->GetCentroid());
    auto input_value = it->first;
    if (DoubleEqual(centroid.mean, input_value)) {
      auto current_mean = centroid.mean;
      auto current_mean_cumulative_weight = cumulative_weight + centroid.weight / 2;
      cumulative_weight += centroid.weight;

      // handle all the previous centroids which has the same mean
      while (!iter->IsBegin() && iter->Prev()) {
        auto next_centroid = GET_OR_RET(iter->GetCentroid());
        if (!DoubleEqual(current_mean, next_centroid.mean)) {
          // move back to the last equal centroid, because we will process it in the next loop
          iter->Next();
          break;
        }
        current_mean_cumulative_weight += next_centroid.weight / 2;
        cumulative_weight += next_centroid.weight;
      }

      // handle the prev inputs which have the same value
      result[it->second] = static_cast<int>(current_mean_cumulative_weight);
      ++it;
      if (iter->IsBegin()) {
        break;
      }
      iter->Prev();
    } else if (DoubleCompare(centroid.mean, input_value) > 0) {
      cumulative_weight += centroid.weight;
      if (iter->IsBegin()) {
        break;
      }
      iter->Prev();
    } else {
      result[it->second] = static_cast<int>(cumulative_weight);
      ++it;
    }
  }

  // handle inputs less than minimum
  while (it != value_to_indices.rend()) {
    result[it->second] = static_cast<int>(td.TotalWeight());
    ++it;
  }

  for (auto r : result) {
    if (r <= -2) {
      return Status{Status::InvalidArgument, "invalid result when computing revrank"};
    }
  }
  return Status::OK();
}
