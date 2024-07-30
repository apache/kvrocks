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

/* Redis HyperLogLog probabilistic cardinality approximation.
 * This file implements the algorithm and the exported Redis commands.
 *
 * Copyright (c) 2014, Salvatore Sanfilippo <antirez at gmail dot com>
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *   * Redistributions of source code must retain the above copyright notice,
 *     this list of conditions and the following disclaimer.
 *   * Redistributions in binary form must reproduce the above copyright
 *     notice, this list of conditions and the following disclaimer in the
 *     documentation and/or other materials provided with the distribution.
 *   * Neither the name of Redis nor the names of its contributors may be used
 *     to endorse or promote products derived from this software without
 *     specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS BE
 * LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
 * POSSIBILITY OF SUCH DAMAGE.
 */

// NOTE: this file is copy from redis's source: `src/hyperloglog.c`

#include "hyperloglog.h"

#include "vendor/murmurhash2.h"

uint8_t HllDenseGetRegister(const uint8_t *registers, uint32_t register_index) {
  uint32_t byte = (register_index * kHyperLogLogRegisterBits) / 8;
  uint8_t fb = (register_index * kHyperLogLogRegisterBits) & 7;
  uint8_t fb8 = 8 - fb;
  uint8_t b0 = registers[byte];
  uint8_t b1 = 0;
  if (fb > 8 - kHyperLogLogRegisterBits) {
    b1 = registers[byte + 1];
  }
  return ((b0 >> fb) | (b1 << fb8)) & kHyperLogLogRegisterMax;
}

void HllDenseSetRegister(uint8_t *registers, uint32_t register_index, uint8_t val) {
  uint32_t byte = register_index * kHyperLogLogRegisterBits / 8;
  uint8_t fb = register_index * kHyperLogLogRegisterBits & 7;
  uint8_t fb8 = 8 - fb;
  uint8_t v = val;
  registers[byte] &= ~(kHyperLogLogRegisterMax << fb);
  registers[byte] |= v << fb;
  if (fb > 8 - kHyperLogLogRegisterBits) {
    registers[byte + 1] &= ~(kHyperLogLogRegisterMax >> fb8);
    registers[byte + 1] |= v >> fb8;
  }
}

/* ========================= HyperLogLog algorithm  ========================= */

// Reference:
// https://github.com/valkey-io/valkey/blob/14e09e981e0039edbf8c41a208a258c18624cbb7/src/hyperloglog.c#L457
//
// Given a string element to add to the HyperLogLog, returns the length of the pattern 000..1 of the element
// hash. As a side effect 'regp' is *set to the register index this element hashes to
DenseHllResult ExtractDenseHllResult(uint64_t hash) {
  /* Count the number of zeroes starting from bit kHyperLogLogRegisterCount
   * (that is a power of two corresponding to the first bit we don't use
   * as index). The max run can be 64-kHyperLogLogRegisterCountPow+1 = kHyperLogLogHashBitCount+1 bits.
   *
   * Note that the final "1" ending the sequence of zeroes must be
   * included in the count, so if we find "001" the count is 3, and
   * the smallest count possible is no zeroes at all, just a 1 bit
   * at the first position, that is a count of 1.
   *
   * This may sound like inefficient, but actually in the average case
   * there are high probabilities to find a 1 after a few iterations. */
  uint32_t index = hash & kHyperLogLogRegisterCountMask; /* Register index. */
  DCHECK_LT(index, kHyperLogLogRegisterCount);
  hash >>= kHyperLogLogRegisterCountPow;             /* Remove bits used to address the register. */
  hash |= (static_cast<uint64_t>(1U) << kHyperLogLogHashBitCount);
  uint8_t ctz = __builtin_ctzll(hash) + 1;
  return DenseHllResult{index, ctz};
}

/*
 * Compute the register histogram in the dense representation.
 */
void HllDenseRegHisto(nonstd::span<const uint8_t> registers, int *reghisto) {
  /* Redis default is to use 16384 registers 6 bits each. The code works
   * with other values by modifying the defines, but for our target value
   * we take a faster path with unrolled loops. */
  const uint8_t *r = registers.data();
  unsigned long r0 = 0, r1 = 0, r2 = 0, r3 = 0, r4 = 0, r5 = 0, r6 = 0, r7 = 0, r8 = 0, r9 = 0, r10 = 0, r11 = 0,
                r12 = 0, r13 = 0, r14 = 0, r15 = 0;
  for (size_t j = 0; j < kHyperLogLogSegmentRegisters / 16; j++) {
    /* Handle 16 registers per iteration. */
    r0 = r[0] & kHyperLogLogRegisterMax;
    r1 = (r[0] >> 6 | r[1] << 2) & kHyperLogLogRegisterMax;
    r2 = (r[1] >> 4 | r[2] << 4) & kHyperLogLogRegisterMax;
    r3 = (r[2] >> 2) & kHyperLogLogRegisterMax;
    r4 = r[3] & kHyperLogLogRegisterMax;
    r5 = (r[3] >> 6 | r[4] << 2) & kHyperLogLogRegisterMax;
    r6 = (r[4] >> 4 | r[5] << 4) & kHyperLogLogRegisterMax;
    r7 = (r[5] >> 2) & kHyperLogLogRegisterMax;
    r8 = r[6] & kHyperLogLogRegisterMax;
    r9 = (r[6] >> 6 | r[7] << 2) & kHyperLogLogRegisterMax;
    r10 = (r[7] >> 4 | r[8] << 4) & kHyperLogLogRegisterMax;
    r11 = (r[8] >> 2) & kHyperLogLogRegisterMax;
    r12 = r[9] & kHyperLogLogRegisterMax;
    r13 = (r[9] >> 6 | r[10] << 2) & kHyperLogLogRegisterMax;
    r14 = (r[10] >> 4 | r[11] << 4) & kHyperLogLogRegisterMax;
    r15 = (r[11] >> 2) & kHyperLogLogRegisterMax;

    reghisto[r0]++;
    reghisto[r1]++;
    reghisto[r2]++;
    reghisto[r3]++;
    reghisto[r4]++;
    reghisto[r5]++;
    reghisto[r6]++;
    reghisto[r7]++;
    reghisto[r8]++;
    reghisto[r9]++;
    reghisto[r10]++;
    reghisto[r11]++;
    reghisto[r12]++;
    reghisto[r13]++;
    reghisto[r14]++;
    reghisto[r15]++;

    r += 12;
  }
}

/* ========================= HyperLogLog Count ==============================
 * This is the core of the algorithm where the approximated count is computed.
 * The function uses the lower level HllDenseRegHisto()
 * functions as helpers to compute histogram of register values part of the
 * computation, which is representation-specific, while all the rest is common. */

/* Helper function sigma as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double HllSigma(double x) {
  if (x == 1.) return INFINITY;
  double z_prime = NAN;
  double y = 1;
  double z = x;
  do {
    x *= x;
    z_prime = z;
    z += x * y;
    y += y;
  } while (z_prime != z);
  return z;
}

/* Helper function tau as defined in
 * "New cardinality estimation algorithms for HyperLogLog sketches"
 * Otmar Ertl, arXiv:1702.01284 */
double HllTau(double x) {
  if (x == 0. || x == 1.) return 0.;
  double z_prime = NAN;
  double y = 1.0;
  double z = 1 - x;
  do {
    x = sqrt(x);
    z_prime = z;
    y *= 0.5;
    z -= pow(1 - x, 2) * y;
  } while (z_prime != z);
  return z / 3;
}

/* Return the approximated cardinality of the set based on the harmonic
 * mean of the registers values. */
uint64_t HllDenseEstimate(const std::vector<nonstd::span<const uint8_t>> &registers) {
  constexpr double m = kHyperLogLogRegisterCount;
  int j = 0;
  /* Note that reghisto size could be just kHyperLogLogHashBitCount+2, because kHyperLogLogHashBitCount+1 is
   * the maximum frequency of the "000...1" sequence the hash function is
   * able to return. However it is slow to check for sanity of the
   * input: instead we history array at a safe size: overflows will
   * just write data to wrong, but correctly allocated, places. */
  int reghisto[64] = {0};

  /* Compute register histogram */
  for (const auto &r : registers) {
    if (r.empty()) {
      // Empty segment
      reghisto[0] += kHyperLogLogSegmentRegisters;
    } else {
      HllDenseRegHisto(r, reghisto);
    }
  }

  /* Estimate cardinality from register histogram. See:
   * "New cardinality estimation algorithms for HyperLogLog sketches"
   * Otmar Ertl, arXiv:1702.01284 */
  double z = m * HllTau((m - reghisto[kHyperLogLogHashBitCount + 1]) / m);
  for (j = kHyperLogLogHashBitCount; j >= 1; --j) {
    z += reghisto[j];
    z *= 0.5;
  }
  z += m * HllSigma(reghisto[0] / m);
  return static_cast<int64_t>(llroundl(kHyperLogLogAlpha * m * m / z));
}

/* Merge by computing MAX(registers_max[i],registers[i]) the HyperLogLog 'registers'
 * with an array of uint8_t kHyperLogLogRegisterCount registers pointed by 'registers_max'. */
void HllMerge(std::vector<uint8_t> *registers_max, const std::vector<uint8_t> &registers) {
  uint8_t val = 0, max_val = 0;

  for (uint32_t i = 0; i < kHyperLogLogRegisterCount; i++) {
    val = HllDenseGetRegister(registers.data(), i);
    max_val = HllDenseGetRegister(registers_max->data(), i);
    if (val > max_val) {
      HllDenseSetRegister(registers_max->data(), i, val);
    }
  }
}
