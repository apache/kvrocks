#include "prefix_extractor.h"
#include "encoding.h"

size_t SubkeyPrefixTransform::GetPrefixLen(const rocksdb::Slice& input) const {
    const char *data = input.data();
    uint8_t ns_size = static_cast<uint8_t>(data[0] & 0xff);

    size_t offset = 1 + ns_size;
    if (cluster_enabled_) {
        offset += 2;
    }
    uint32_t key_size = DecodeFixed32(data + offset);

    return (prefix_base_len_ + ns_size + key_size);
}

const rocksdb::SliceTransform* NewSubkeyPrefixTransform(bool cluster_enabled) {
    return new SubkeyPrefixTransform(cluster_enabled);
}
