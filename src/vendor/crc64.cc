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

#include "crc64.h"
// NOLINTBEGIN

static uint64_t crc64_table[8][256] = {{0}};

#define POLY UINT64_C(0xad93d23594c935a9)
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

/* Fill in a CRC constants table. */
void crcspeed64little_init(crcfn64 crcfn, uint64_t table[8][256]) {
  uint64_t crc;

  /* generate CRCs for all single byte sequences */
  for (int n = 0; n < 256; n++) {
    unsigned char v = n;
    table[0][n] = crcfn(0, &v, 1);
  }

  /* generate nested CRC table for future slice-by-8 lookup */
  for (int n = 0; n < 256; n++) {
    crc = table[0][n];
    for (int k = 1; k < 8; k++) {
      crc = table[0][crc & 0xff] ^ (crc >> 8);
      table[k][n] = crc;
    }
  }
}

void crcspeed16little_init(crcfn16 crcfn, uint16_t table[8][256]) {
  uint16_t crc;

  /* generate CRCs for all single byte sequences */
  for (int n = 0; n < 256; n++) {
    table[0][n] = crcfn(0, &n, 1);
  }

  /* generate nested CRC table for future slice-by-8 lookup */
  for (int n = 0; n < 256; n++) {
    crc = table[0][n];
    for (int k = 1; k < 8; k++) {
      crc = table[0][(crc >> 8) & 0xff] ^ (crc << 8);
      table[k][n] = crc;
    }
  }
}

/* Reverse the bytes in a 64-bit word. */
static inline uint64_t rev8(uint64_t a) {
#if defined(__GNUC__) || defined(__clang__)
  return __builtin_bswap64(a);
#else
  uint64_t m;

  m = UINT64_C(0xff00ff00ff00ff);
  a = ((a >> 8) & m) | (a & m) << 8;
  m = UINT64_C(0xffff0000ffff);
  a = ((a >> 16) & m) | (a & m) << 16;
  return a >> 32 | a << 32;
#endif
}

/* This function is called once to initialize the CRC table for use on a
   big-endian architecture. */
void crcspeed64big_init(crcfn64 fn, uint64_t big_table[8][256]) {
  /* Create the little endian table then reverse all the entries. */
  crcspeed64little_init(fn, big_table);
  for (int k = 0; k < 8; k++) {
    for (int n = 0; n < 256; n++) {
      big_table[k][n] = rev8(big_table[k][n]);
    }
  }
}

void crcspeed16big_init(crcfn16 fn, uint16_t big_table[8][256]) {
  /* Create the little endian table then reverse all the entries. */
  crcspeed16little_init(fn, big_table);
  for (int k = 0; k < 8; k++) {
    for (int n = 0; n < 256; n++) {
      big_table[k][n] = rev8(big_table[k][n]);
    }
  }
}

/* Calculate a non-inverted CRC multiple bytes at a time on a little-endian
 * architecture. If you need inverted CRC, invert *before* calling and invert
 * *after* calling.
 * 64 bit crc = process 8 bytes at once;
 */
uint64_t crcspeed64little(uint64_t little_table[8][256], uint64_t crc, void *buf, size_t len) {
  unsigned char *next = static_cast<unsigned char *>(buf);

  /* process individual bytes until we reach an 8-byte aligned pointer */
  while (len && ((uintptr_t)next & 7) != 0) {
    crc = little_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }

  /* fast middle processing, 8 bytes (aligned!) per loop */
  while (len >= 8) {
    crc ^= *(uint64_t *)next;
    crc = little_table[7][crc & 0xff] ^ little_table[6][(crc >> 8) & 0xff] ^ little_table[5][(crc >> 16) & 0xff] ^
          little_table[4][(crc >> 24) & 0xff] ^ little_table[3][(crc >> 32) & 0xff] ^
          little_table[2][(crc >> 40) & 0xff] ^ little_table[1][(crc >> 48) & 0xff] ^ little_table[0][crc >> 56];
    next += 8;
    len -= 8;
  }

  /* process remaining bytes (can't be larger than 8) */
  while (len) {
    crc = little_table[0][(crc ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }

  return crc;
}

uint16_t crcspeed16little(uint16_t little_table[8][256], uint16_t crc, void *buf, size_t len) {
  unsigned char *next = static_cast<unsigned char *>(buf);

  /* process individual bytes until we reach an 8-byte aligned pointer */
  while (len && ((uintptr_t)next & 7) != 0) {
    crc = little_table[0][((crc >> 8) ^ *next++) & 0xff] ^ (crc << 8);
    len--;
  }

  /* fast middle processing, 8 bytes (aligned!) per loop */
  while (len >= 8) {
    uint64_t n = *(uint64_t *)next;
    crc = little_table[7][(n & 0xff) ^ ((crc >> 8) & 0xff)] ^ little_table[6][((n >> 8) & 0xff) ^ (crc & 0xff)] ^
          little_table[5][(n >> 16) & 0xff] ^ little_table[4][(n >> 24) & 0xff] ^ little_table[3][(n >> 32) & 0xff] ^
          little_table[2][(n >> 40) & 0xff] ^ little_table[1][(n >> 48) & 0xff] ^ little_table[0][n >> 56];
    next += 8;
    len -= 8;
  }

  /* process remaining bytes (can't be larger than 8) */
  while (len) {
    crc = little_table[0][((crc >> 8) ^ *next++) & 0xff] ^ (crc << 8);
    len--;
  }

  return crc;
}

/* Calculate a non-inverted CRC eight bytes at a time on a big-endian
 * architecture.
 */
uint64_t crcspeed64big(uint64_t big_table[8][256], uint64_t crc, void *buf, size_t len) {
  unsigned char *next = static_cast<unsigned char *>(buf);

  crc = rev8(crc);
  while (len && ((uintptr_t)next & 7) != 0) {
    crc = big_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
    len--;
  }

  while (len >= 8) {
    crc ^= *(uint64_t *)next;
    crc = big_table[0][crc & 0xff] ^ big_table[1][(crc >> 8) & 0xff] ^ big_table[2][(crc >> 16) & 0xff] ^
          big_table[3][(crc >> 24) & 0xff] ^ big_table[4][(crc >> 32) & 0xff] ^ big_table[5][(crc >> 40) & 0xff] ^
          big_table[6][(crc >> 48) & 0xff] ^ big_table[7][crc >> 56];
    next += 8;
    len -= 8;
  }

  while (len) {
    crc = big_table[0][(crc >> 56) ^ *next++] ^ (crc << 8);
    len--;
  }

  return rev8(crc);
}

/* WARNING: Completely untested on big endian architecture.  Possibly broken. */
uint16_t crcspeed16big(uint16_t big_table[8][256], uint16_t crc_in, void *buf, size_t len) {
  unsigned char *next = static_cast<unsigned char *>(buf);
  uint64_t crc = crc_in;

  crc = rev8(crc);
  while (len && ((uintptr_t)next & 7) != 0) {
    crc = big_table[0][((crc >> (56 - 8)) ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }

  while (len >= 8) {
    uint64_t n = *(uint64_t *)next;
    crc = big_table[0][(n & 0xff) ^ ((crc >> (56 - 8)) & 0xff)] ^ big_table[1][((n >> 8) & 0xff) ^ (crc & 0xff)] ^
          big_table[2][(n >> 16) & 0xff] ^ big_table[3][(n >> 24) & 0xff] ^ big_table[4][(n >> 32) & 0xff] ^
          big_table[5][(n >> 40) & 0xff] ^ big_table[6][(n >> 48) & 0xff] ^ big_table[7][n >> 56];
    next += 8;
    len -= 8;
  }

  while (len) {
    crc = big_table[0][((crc >> (56 - 8)) ^ *next++) & 0xff] ^ (crc >> 8);
    len--;
  }

  return rev8(crc);
}

/* Return the CRC of buf[0..len-1] with initial crc, processing eight bytes
   at a time using passed-in lookup table.
   This selects one of two routines depending on the endianness of
   the architecture. */
uint64_t crcspeed64native(uint64_t table[8][256], uint64_t crc, void *buf, size_t len) {
  uint64_t n = 1;

  return *(char *)&n ? crcspeed64little(table, crc, buf, len) : crcspeed64big(table, crc, buf, len);
}

uint16_t crcspeed16native(uint16_t table[8][256], uint16_t crc, void *buf, size_t len) {
  uint64_t n = 1;

  return *(char *)&n ? crcspeed16little(table, crc, buf, len) : crcspeed16big(table, crc, buf, len);
}

/* Initialize CRC lookup table in architecture-dependent manner. */
void crcspeed64native_init(crcfn64 fn, uint64_t table[8][256]) {
  uint64_t n = 1;

  *(char *)&n ? crcspeed64little_init(fn, table) : crcspeed64big_init(fn, table);
}

void crcspeed16native_init(crcfn16 fn, uint16_t table[8][256]) {
  uint64_t n = 1;

  *(char *)&n ? crcspeed16little_init(fn, table) : crcspeed16big_init(fn, table);
}

/******************** BEGIN GENERATED PYCRC FUNCTIONS ********************/
/**
 * Generated on Sun Dec 21 14:14:07 2014,
 * by pycrc v0.8.2, https://www.tty1.net/pycrc/
 *
 * LICENSE ON GENERATED CODE:
 * ==========================
 * As of version 0.6, pycrc is released under the terms of the MIT licence.
 * The code generated by pycrc is not considered a substantial portion of the
 * software, therefore the author of pycrc will not claim any copyright on
 * the generated code.
 * ==========================
 *
 * CRC configuration:
 *    Width        = 64
 *    Poly         = 0xad93d23594c935a9
 *    XorIn        = 0xffffffffffffffff
 *    ReflectIn    = True
 *    XorOut       = 0x0000000000000000
 *    ReflectOut   = True
 *    Algorithm    = bit-by-bit-fast
 *
 * Modifications after generation (by matt):
 *   - included finalize step in-line with update for single-call generation
 *   - re-worked some inner variable architectures
 *   - adjusted function parameters to match expected prototypes.
 *****************************************************************************/

/**
 * Reflect all bits of a \a data word of \a data_len bytes.
 *
 * \param data         The data word to be reflected.
 * \param data_len     The width of \a data expressed in number of bits.
 * \return             The reflected data.
 *****************************************************************************/
static inline uint_fast64_t crc_reflect(uint_fast64_t data, size_t data_len) {
  uint_fast64_t ret = data & 0x01;

  for (size_t i = 1; i < data_len; i++) {
    data >>= 1;
    ret = (ret << 1) | (data & 0x01);
  }

  return ret;
}

/**
 *  Update the crc value with new data.
 *
 * \param crc      The current crc value.
 * \param data     Pointer to a buffer of \a data_len bytes.
 * \param data_len Number of bytes in the \a data buffer.
 * \return         The updated crc value.
 ******************************************************************************/
uint64_t _crc64(uint_fast64_t crc, const void *in_data, const uint64_t len) {
  const uint8_t *data = static_cast<const uint8_t *>(in_data);
  unsigned long long bit;

  for (uint64_t offset = 0; offset < len; offset++) {
    uint8_t c = data[offset];
    for (uint_fast8_t i = 0x01; i & 0xff; i <<= 1) {
      bit = crc & 0x8000000000000000;
      if (c & i) {
        bit = !bit;
      }

      crc <<= 1;
      if (bit) {
        crc ^= POLY;
      }
    }

    crc &= 0xffffffffffffffff;
  }

  crc = crc & 0xffffffffffffffff;
  return crc_reflect(crc, 64) ^ 0x0000000000000000;
}

/******************** END GENERATED PYCRC FUNCTIONS ********************/

/* Initializes the 16KB lookup tables. */
void crc64_init(void) { crcspeed64native_init(_crc64, crc64_table); }

/* Compute crc64 */
uint64_t crc64(uint64_t crc, const unsigned char *s, uint64_t l) {
  return crcspeed64native(crc64_table, crc, (void *)s, l);
}

/* Test main */
#ifdef REDIS_TEST
#include <stdio.h>

#define UNUSED(x) (void)(x)
int crc64Test(int argc, char *argv[], int flags) {
  UNUSED(argc);
  UNUSED(argv);
  UNUSED(flags);
  crc64_init();
  printf("[calcula]: e9c6d914c4b8d9ca == %016" PRIx64 "\n", (uint64_t)_crc64(0, "123456789", 9));
  printf("[64speed]: e9c6d914c4b8d9ca == %016" PRIx64 "\n", (uint64_t)crc64(0, (unsigned char *)"123456789", 9));
  char li[] =
      "Lorem ipsum dolor sit amet, consectetur adipiscing elit, sed "
      "do eiusmod tempor incididunt ut labore et dolore magna "
      "aliqua. Ut enim ad minim veniam, quis nostrud exercitation "
      "ullamco laboris nisi ut aliquip ex ea commodo consequat. Duis "
      "aute irure dolor in reprehenderit in voluptate velit esse "
      "cillum dolore eu fugiat nulla pariatur. Excepteur sint "
      "occaecat cupidatat non proident, sunt in culpa qui officia "
      "deserunt mollit anim id est laborum.";
  printf("[calcula]: c7794709e69683b3 == %016" PRIx64 "\n", (uint64_t)_crc64(0, li, sizeof(li)));
  printf("[64speed]: c7794709e69683b3 == %016" PRIx64 "\n", (uint64_t)crc64(0, (unsigned char *)li, sizeof(li)));
  return 0;
}

#endif

#ifdef REDIS_TEST_MAIN
int main(int argc, char *argv[]) { return crc64Test(argc, argv); }

#endif
// NOLINTEND
