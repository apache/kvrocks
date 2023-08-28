/* Copyright (c) 2014, Matt Stancliff <matt@genges.com>
 * Copyright (c) 2020, Amazon Web Services
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
 * POSSIBILITY OF SUCH DAMAGE. */

#include "endianconv.h"

// NOLINTBEGIN

/* Toggle the 16 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev16(void *p) {
  unsigned char *x = static_cast<unsigned char *>(p), t;

  t = x[0];
  x[0] = x[1];
  x[1] = t;
}

/* Toggle the 32 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev32(void *p) {
  unsigned char *x = static_cast<unsigned char *>(p), t;

  t = x[0];
  x[0] = x[3];
  x[3] = t;
  t = x[1];
  x[1] = x[2];
  x[2] = t;
}

/* Toggle the 64 bit unsigned integer pointed by *p from little endian to
 * big endian */
void memrev64(void *p) {
  unsigned char *x = static_cast<unsigned char *>(p), t;

  t = x[0];
  x[0] = x[7];
  x[7] = t;
  t = x[1];
  x[1] = x[6];
  x[6] = t;
  t = x[2];
  x[2] = x[5];
  x[5] = t;
  t = x[3];
  x[3] = x[4];
  x[4] = t;
}

uint16_t intrev16(uint16_t v) {
  memrev16(&v);
  return v;
}

uint32_t intrev32(uint32_t v) {
  memrev32(&v);
  return v;
}

uint64_t intrev64(uint64_t v) {
  memrev64(&v);
  return v;
}

// NOLINTEND
