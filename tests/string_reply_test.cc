#include <gtest/gtest.h>
#include "redis_reply.h"

class StringReplyTest : public testing::Test {
 protected:
    static void SetUpTestCase() {
        for (int i = 0; i < 100000; i++) {
            values.emplace_back("values" + std::to_string(i));
        }
    }
    static void TearDownTestCase() {
        values.clear();
    }
    static std::vector<std::string> values;
    virtual void SetUp() {}

    virtual void TearDown() {}
};

std::vector<std::string> StringReplyTest::values;

TEST_F(StringReplyTest, MultiBulkString) {
    std::string result = Redis::MultiBulkString(values);
    ASSERT_EQ(result.length(), 13*10+14*90+15*900+17*9000+18*90000+9);
}

TEST_F(StringReplyTest, BulkString) {
    std::string result = "*" + std::to_string(values.size()) + CRLF;
    for (const auto &v : values) {
      result += Redis::BulkString(v);
    }

    ASSERT_EQ(result.length(), 13*10+14*90+15*900+17*9000+18*90000+9);
}
