#pragma once

#include <rocksdb/slice_transform.h>

class SubkeyPrefixTransform : public rocksdb::SliceTransform {
 private:
  bool cluster_enabled_;
  uint8_t prefix_base_len_;

 public:
  explicit SubkeyPrefixTransform(bool cluster_enabled) : cluster_enabled_(cluster_enabled) {
    // Subkey format:
    // 1(namespace_len) + N(namespace) + 2(slot_id) + 4(user_key_len) + N(user_key) + 8(version) + N(field) + ..
    // If cluster_enabled is true, length of Subkey is not smaller than 15
    // If cluster_enabled is false, length of Subkey is not smaller than 13
    prefix_base_len_ = cluster_enabled ? 15 : 13;
  }

  const char* Name() const override { return "Kvrocks.SubkeyPrefix"; }

  rocksdb::Slice Transform(const rocksdb::Slice& src) const override {
    assert(InDomain(src));
    size_t prefix_len = GetPrefixLen(src);
    return rocksdb::Slice(src.data(), prefix_len);
  }

  bool InDomain(const rocksdb::Slice& src) const override {
    return (src.size() >= prefix_base_len_);
  }

  size_t GetPrefixLen(const rocksdb::Slice& input) const;
};

extern const rocksdb::SliceTransform* NewSubkeyPrefixTransform(bool cluster_enabled);
