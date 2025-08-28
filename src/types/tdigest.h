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

template <typename TD>
inline Status TDigestRank(TD&& td, const std::vector<double>& inputs, std::vector<int>& result) {
  std::vector<size_t> indices(inputs.size());
  std::iota(indices.begin(), indices.end(), 0);
  std::sort(indices.begin(), indices.end(), [&inputs](size_t a, size_t b) { return inputs[a] < inputs[b]; });

  result.resize(inputs.size());
  size_t i = indices.size();
  double cumulative_weight = 0;

  // handle inputs larger than maximum
  while (i > 0 && inputs[indices[i - 1]] > td.Max()) {
    result[indices[i - 1]] = -1;
    i--;
  }

  // reverse iterate through centroids and calculate reverse rank for each input
  auto iter = td.End();
  while (i > 0) {
    auto centroid = GET_OR_RET(iter->GetCentroid());
    
    if (centroid.mean > inputs[indices[i - 1]]) {
      // mean > input, accumulate weight and move to prev centroid
      cumulative_weight += centroid.weight;
    } else if (centroid.mean == inputs[indices[i - 1]]) {
      // mean == input, calculate reverse rank with half weight of current centroid
      cumulative_weight += centroid.weight;
      auto current_mean = centroid.mean;
      auto current_mean_cumulative_weight = cumulative_weight + centroid.weight / 2;
      
      // handle all the prev centroids which has the same mean
      while (!iter->IsAtBegin() && iter->Prev()) {
        auto next_centroid = GET_OR_RET(iter->GetCentroid());
        if (current_mean != next_centroid.mean) {
          // move back to the last equal centroid, because we will process it in the next loop
          iter->Next();
          break;
        }
        current_mean_cumulative_weight += centroid.weight / 2;
        cumulative_weight += centroid.weight;
      }

      // assign the reverse rank for the inputs[indices[i - 1]]
      result[indices[i - 1]] = static_cast<int>(current_mean_cumulative_weight);
      i--;

      // handle the prev inputs which has the same value
      while ((i > 0) && (inputs[indices[i]] == inputs[indices[i - 1]])) {
        result[indices[i - 1]] = result[indices[i]];
        i--;
      }
    } else {
      // mean < input, calculate reverse rank
      result[indices[i - 1]] = static_cast<int>(cumulative_weight);
      i--;
    }
    
    if (iter->IsAtBegin()) {
      break;
    }
    iter->Prev();
  }

  // handle inputs less than minimum
  while (i > 0) {
    result[indices[i - 1]] = static_cast<int>(td.TotalWeight());
    i--;
  }
  return Status::OK();
}