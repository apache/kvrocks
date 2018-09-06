#include "util.h"

#include <limits.h>
#include <unistd.h>
#include <stdint.h>

/* Convert a string into a long long. Returns 1 if the string could be parsed
 * into a (non-overflowing) long long, 0 otherwise. The value will be set to
 * the parsed value when appropriate. */
int string2ll(const char *s, size_t slen, long long *value) {
  const char *p = s;
  size_t plen = 0;
  int negative = 0;
  unsigned long long v;

  if (plen == slen)
    return 0;

  /* Special case: first and only digit is 0. */
  if (slen == 1 && p[0] == '0') {
    if (value) *value = 0;
    return 1;
  }

  if (p[0] == '-') {
    negative = 1;
    p++;
    plen++;

    /* Abort on only a negative sign. */
    if (plen == slen)
      return 0;
  }

  /* First digit should be 1-9, otherwise the string should just be 0. */
  if (p[0] >= '1' && p[0] <= '9') {
    v = p[0] - '0';
    p++;
    plen++;
  } else if (p[0] == '0' && slen == 1) {
    *value = 0;
    return 1;
  } else {
    return 0;
  }

  while (plen < slen && p[0] >= '0' && p[0] <= '9') {
    if (v > (ULLONG_MAX / 10)) /* Overflow. */
      return 0;
    v *= 10;

    if (v > (ULLONG_MAX - (p[0] - '0'))) /* Overflow. */
      return 0;
    v += p[0] - '0';

    p++;
    plen++;
  }

  /* Return if not all bytes were used. */
  if (plen < slen)
    return 0;

  if (negative) {
    if (v > ((unsigned long long) (-(LLONG_MIN + 1)) + 1)) /* Overflow. */
      return 0;
    if (value) *value = -v;
  } else {
    if (v > LLONG_MAX) /* Overflow. */
      return 0;
    if (value) *value = v;
  }
  return 1;
}

/* Return the number of digits of 'v' when converted to string in radix 10.
 * See ll2string() for more information. */
uint32_t digits10(uint64_t v) {
  if (v < 10) return 1;
  if (v < 100) return 2;
  if (v < 1000) return 3;
  if (v < 1000000000000UL) {
    if (v < 100000000UL) {
      if (v < 1000000) {
        if (v < 10000) return 4;
        return 5 + (v >= 100000);
      }
      return 7 + (v >= 10000000UL);
    }
    if (v < 10000000000UL) {
      return 9 + (v >= 1000000000UL);
    }
    return 11 + (v >= 100000000000UL);
  }
  return 12 + digits10(v / 1000000000000UL);
}

/* Convert a long long into a string. Returns the number of
 * characters needed to represent the number.
 * If the buffer is not big enough to store the string, 0 is returned.
 *
 * Based on the following article (that apparently does not provide a
 * novel approach but only publicizes an already used technique):
 *
 * https://www.facebook.com/notes/facebook-engineering/three-optimization-tips-for-c/10151361643253920
 *
 * Modified in order to handle signed integers since the original code was
 * designed for unsigned integers. */
int ll2string(char* dst, size_t dstlen, long long svalue) {
  static const char digits[201] =
          "0001020304050607080910111213141516171819"
          "2021222324252627282930313233343536373839"
          "4041424344454647484950515253545556575859"
          "6061626364656667686970717273747576777879"
          "8081828384858687888990919293949596979899";
  int negative;
  unsigned long long value;

  /* The main loop works with 64bit unsigned integers for simplicity, so
   * we convert the number here and remember if it is negative. */
  if (svalue < 0) {
    if (svalue != LLONG_MIN) {
      value = -svalue;
    } else {
      value = ((unsigned long long) LLONG_MAX)+1;
    }
    negative = 1;
  } else {
    value = svalue;
    negative = 0;
  }

  /* Check length. */
  uint32_t const length = digits10(value)+negative;
  if (length >= dstlen) return 0;

  /* Null term. */
  uint32_t next = length;
  dst[next] = '\0';
  next--;
  while (value >= 100) {
    int const i = (value % 100) * 2;
    value /= 100;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
    next -= 2;
  }

  /* Handle last 1-2 digits. */
  if (value < 10) {
    dst[next] = '0' + (uint32_t) value;
  } else {
    int i = (uint32_t) value * 2;
    dst[next] = digits[i + 1];
    dst[next - 1] = digits[i];
  }

  /* Add sign. */
  if (negative) dst[0] = '-';
  return length;
}
