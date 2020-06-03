#include <gtest/gtest.h>

#include "test_base.h"
#include "redis_bitmap.h"

class RedisBitmapTest : public TestBase {
 protected:
  explicit RedisBitmapTest() : TestBase() {
    bitmap = new Redis::Bitmap(storage_, "bitmap_ns");
  }
  ~RedisBitmapTest() {
    delete bitmap;
  }
  void SetUp() override {
    key_ = "test_bitmap_key";
  }
  void TearDown() override {}

 protected:
  Redis::Bitmap *bitmap;
};

TEST_F(RedisBitmapTest, GetAndSetBit) {
  uint32_t offsets[] = {0, 123, 1024*8, 1024*8+1, 3*1024*8,  3*1024*8+1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap->GetBit(key_, offset, &bit);
    EXPECT_FALSE(bit);
    bitmap->SetBit(key_, offset, true, &bit);
    bitmap->GetBit(key_, offset, &bit);
    EXPECT_TRUE(bit);
  }
  bitmap->Del(key_);
}

TEST_F(RedisBitmapTest, BitCount) {
  uint32_t offsets[] = {0, 123, 1024*8, 1024*8+1, 3*1024*8,  3*1024*8+1};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap->SetBit(key_, offset, true, &bit);
  }
  uint32_t cnt;
  bitmap->BitCount(key_, 0, 4*1024, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap->BitCount(key_, 0, -1, &cnt);
  EXPECT_EQ(cnt, 6);
  bitmap->Del(key_);
}

TEST_F(RedisBitmapTest, BitPosClearBit) {
  int pos;
  bool old_bit;
  for (int i = 0; i < 1024+16;i ++) {
    bitmap->BitPos(key_, false, 0, -1, true, &pos);
    EXPECT_EQ(pos, i);
    bitmap->SetBit(key_, i, true, &old_bit);
    EXPECT_FALSE(old_bit);
  }
  bitmap->Del(key_);
}

TEST_F(RedisBitmapTest, BitPosSetBit) {
  uint32_t offsets[] = {0, 123, 1024*8, 1024*8+16, 3*1024*8,  3*1024*8+16};
  for (const auto &offset : offsets) {
    bool bit = false;
    bitmap->SetBit(key_, offset, true, &bit);
  }
  int pos;
  int start_indexes[] = {0, 1, 124, 1025, 1027, 3*1024+1};
  for (size_t i = 0; i < sizeof(start_indexes)/ sizeof(start_indexes[0]); i++) {
    bitmap->BitPos(key_, true, start_indexes[i], -1, true, &pos);
    EXPECT_EQ(pos, offsets[i]);
  }
  bitmap->Del(key_);
}
