/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

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
