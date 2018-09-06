#ifndef KVROCKS_UTIL_H
#define KVROCKS_UTIL_H

#include <unistd.h>
int string2ll(const char *s, size_t slen, long long *value);
int ll2string(char* dst, size_t dstlen, long long svalue);
#endif //KVROCKS_UTIL_H
