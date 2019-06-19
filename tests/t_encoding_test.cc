#include <gtest/gtest.h>
#include "encoding.h"

#include <limits>
TEST(Util, EncodeAndDecodeDouble) {
  std::vector<double> values = {-1234, -100.1234, -1.2345, 0, 1.2345, 100.1234, 1234};
  std::string prev_bytes;
  for (auto value : values) {
    std::string bytes;
    PutDouble(&bytes, value);
    double got = DecodeDouble(bytes.data());
    if (!prev_bytes.empty()) {
      ASSERT_LT(prev_bytes, bytes);
    }
    prev_bytes.assign(bytes);
    ASSERT_EQ(value, got);
  }
}