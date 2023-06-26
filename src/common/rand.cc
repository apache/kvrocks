/* Pseudo random number generation functions derived from the drand48()
 * function obtained from pysam source code.
 *
 * This functions are used in order to replace the default math.random()
 * Lua implementation with something having exactly the same behavior
 * across different systems (by default Lua uses libc's rand() that is not
 * required to implement a specific PRNG generating the same sequence
 * in different systems if seeded with the same integer).
 *
 * The original code appears to be under the public domain.
 * I modified it removing the non needed functions and all the
 * 1960-style C coding stuff...
 *
 * ----------------------------------------------------------------------------
 *
 * Copyright (c) 2010-2012, Salvatore Sanfilippo <antirez at gmail dot com>
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

/*
 * Rewritten in C++ with constexprs and templates by @tanruixiang in
 * https://github.com/apache/kvrocks/commit/900e0f44781d9459649e9864ba09abe6000ce987
 */

#include "rand.h"

#include <stdint.h>

namespace redis_rand_impl {

constexpr const int N = 16;
constexpr const int MASK = ((1 << (N - 1)) + (1 << (N - 1)) - 1);
template <typename T>
constexpr T LOW(const T x) {
  return ((unsigned)(x)&MASK);
}

template <typename T>
constexpr T HIGH(const T x) {
  return LOW(x >> N);
}

template <typename T>
constexpr void MUL(const T x, const T y, T *z) {
  int32_t l = (int64_t)(x) * (int64_t)(y);
  (z)[0] = LOW(l);
  (z)[1] = HIGH(l);
}

template <typename T>
constexpr T CARRY(const T x, const T y) {
  return ((int32_t)(x) + (int64_t)(y) > MASK);
}

template <typename T>
constexpr void ADDEQU(T &x, const T y, T &z) {
  z = CARRY(x, (y)), x = LOW(x + (y));
}

constexpr const uint32_t X0 = 0x330E;
constexpr const uint32_t X1 = 0xABCD;
constexpr const uint32_t X2 = 0x1234;
constexpr const uint32_t A0 = 0xE66D;
constexpr const uint32_t A1 = 0xDEEC;
constexpr const uint32_t A2 = 0x5;
constexpr const uint32_t C = 0xB;

static uint32_t x[3] = {X0, X1, X2}, a[3] = {A0, A1, A2}, c = C;

template <typename T>
constexpr void SET3(T *x, const T x0, const T x1, const T x2) {
  (x)[0] = (x0), (x)[1] = (x1), (x)[2] = (x2);
}

template <typename T>
constexpr void SETLOW(T *x, const T *y, const T n) {
  SET3(x, LOW((y)[n]), LOW((y)[(n) + 1]), LOW((y)[(n) + 2]));
}

template <typename T>
constexpr void SEED(const T x0, const T x1, const T x2) {
  SET3(x, x0, x1, x2), SET3(a, A0, A1, A2), c = C;
}

template <typename T>
constexpr T HiBit(const T x) {
  return (1L << (2 * N - 1));
}

static void Next() {
  uint32_t p[2], q[2], r[2], carry0 = 0, carry1 = 0;

  MUL(a[0], x[0], p);
  ADDEQU(p[0], c, carry0);
  ADDEQU(p[1], carry0, carry1);
  MUL(a[0], x[1], q);
  ADDEQU(p[1], q[0], carry0);
  MUL(a[1], x[0], r);
  x[2] = LOW(carry0 + carry1 + CARRY(p[1], r[0]) + q[1] + r[1] + a[0] * x[2] + a[1] * x[1] + a[2] * x[0]);
  x[1] = LOW(p[1] + r[0]);
  x[0] = LOW(p[0]);
}

}  // namespace redis_rand_impl

int32_t RedisLrand48() {
  using namespace redis_rand_impl;
  Next();
  return static_cast<int32_t>(x[2] << (N - 1)) + static_cast<int32_t>(x[1] >> 1);
}

void RedisSrand48(int32_t seedval) {
  using namespace redis_rand_impl;
  SEED(X0, static_cast<uint32_t>(LOW(seedval)), static_cast<uint32_t>(HIGH(seedval)));
}
