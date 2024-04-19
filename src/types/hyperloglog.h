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

#include <cstdint>
#include <vector>

#include "redis_bitmap.h"

constexpr uint32_t kHyperLogLogRegisterCountPow = 14; /* The greater is Pow, the smaller the error. */
constexpr uint32_t kHyperLogLogHashBitCount =
    64 - kHyperLogLogRegisterCountPow; /* The number of bits of the hash value used for determining the number of
                                                                                     leading zeros. */
constexpr uint32_t kHyperLogLogRegisterCount = 1 << kHyperLogLogRegisterCountPow; /* With Pow=14, 16384 registers. */

// NOTICE: adapt to the requirements of use Bitmap::SegmentCacheStore
constexpr uint32_t kHyperLogLogRegisterCountPerSegment = redis::kBitmapSegmentBits / 8;

constexpr uint32_t kHyperLogLogSegmentCount = kHyperLogLogRegisterCount / kHyperLogLogRegisterCountPerSegment;
constexpr uint32_t kHyperLogLogBits = 8;
constexpr uint32_t kHyperLogLogRegisterCountMask = kHyperLogLogRegisterCount - 1; /* Mask to index register. */
constexpr uint32_t kHyperLogLogRegisterMax = ((1 << kHyperLogLogBits) - 1);
constexpr double kHyperLogLogAlphaInf = 0.721347520444481703680; /* constant for 0.5/ln(2) */
constexpr uint32_t kHyperLogLogRegisterBytesPerSegment = kHyperLogLogRegisterCountPerSegment * kHyperLogLogBits / 8;
constexpr uint32_t kHyperLogLogRegisterBytes = kHyperLogLogRegisterCount * kHyperLogLogBits / 8;

void HllDenseGetRegister(uint8_t *val, uint8_t *registers, uint32_t index);
void HllDenseSetRegister(uint8_t *registers, uint32_t index, uint8_t val);
uint8_t HllPatLen(const std::vector<uint8_t> &element, uint32_t *register_index);
uint64_t HllCount(const std::vector<uint8_t> &registers);
void HllMerge(std::vector<uint8_t> *registers_max, const std::vector<uint8_t> &registers);
