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
 */

#include <algorithm>
#include <chrono>
#include <cstddef>
#include <iomanip>
#include <iostream>
#include <string>
#include <string_view>
#include <vector>

#include "server/redis_reply.h"

namespace {

constexpr size_t kSamples = 7;
constexpr size_t kKiB = 1024;
constexpr size_t kMiB = 1024 * kKiB;
constexpr size_t kBytesPerBenchmark = 64 * kMiB;
constexpr size_t kReplicationBatchBytes = 16 * kKiB;

void AppendBulkStringOld(std::string *dst, std::string_view data) { dst->append(redis::BulkString(data)); }

template <typename Append>
double RunPubSubBenchmark(size_t payload_size, size_t iterations, Append append) {
  const std::string payload(payload_size, 'x');
  std::string output;
  size_t bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < iterations; ++i) {
    output = redis::MultiLen(3);
    append(&output, "message");
    append(&output, "benchmark-channel");
    append(&output, payload);
    bytes += output.size();
  }
  const auto elapsed = std::chrono::steady_clock::now() - start;
  return static_cast<double>(bytes) / std::chrono::duration<double>(elapsed).count() / kMiB;
}

template <typename Append>
double RunReplicationBenchmark(size_t payload_size, size_t iterations, Append append) {
  const std::string payload(payload_size, 'x');
  std::string output;
  size_t bytes = 0;
  const auto start = std::chrono::steady_clock::now();
  for (size_t i = 0; i < iterations; ++i) {
    append(&output, payload);
    if (output.size() >= kReplicationBatchBytes) {
      bytes += output.size();
      output.clear();
    }
  }
  bytes += output.size();
  const auto elapsed = std::chrono::steady_clock::now() - start;
  return static_cast<double>(bytes) / std::chrono::duration<double>(elapsed).count() / kMiB;
}

template <typename Benchmark, typename Append>
double Median(Benchmark benchmark, size_t payload_size, size_t iterations, Append append) {
  std::vector<double> samples;
  samples.reserve(kSamples);
  for (size_t i = 0; i < kSamples; ++i) {
    samples.emplace_back(benchmark(payload_size, iterations, append));
  }
  std::sort(samples.begin(), samples.end());
  return samples[samples.size() / 2];
}

void PrintResult(std::string_view workload, size_t payload_size, double old_rate, double new_rate) {
  std::cout << std::left << std::setw(18) << workload << std::right << std::setw(8) << payload_size << " B"
            << std::setw(14) << std::fixed << std::setprecision(2) << old_rate << " MiB/s" << std::setw(14) << new_rate
            << " MiB/s" << std::setw(10) << new_rate / old_rate << "x\n";
}

}  // namespace

int main() {
  std::cout << "workload           payload           old           new   speedup\n";
  for (const auto payload_size : {64U, 1024U, 16384U}) {
    const auto iterations = std::max<size_t>(10000, kBytesPerBenchmark / payload_size);
    const auto old_rate =
        Median(RunPubSubBenchmark<decltype(&AppendBulkStringOld)>, payload_size, iterations, AppendBulkStringOld);
    const auto new_rate = Median(RunPubSubBenchmark<decltype(&redis::AppendBulkString)>, payload_size, iterations,
                                 redis::AppendBulkString);
    PrintResult("pub/sub reply", payload_size, old_rate, new_rate);
  }

  for (const auto payload_size : {1024U, 16384U}) {
    const auto iterations = kBytesPerBenchmark / payload_size;
    const auto old_rate =
        Median(RunReplicationBenchmark<decltype(&AppendBulkStringOld)>, payload_size, iterations, AppendBulkStringOld);
    const auto new_rate = Median(RunReplicationBenchmark<decltype(&redis::AppendBulkString)>, payload_size, iterations,
                                 redis::AppendBulkString);
    PrintResult("replication batch", payload_size, old_rate, new_rate);
  }
}
