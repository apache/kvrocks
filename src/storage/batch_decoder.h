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

#pragma once

#include <rocksdb/write_batch.h>

#include <charconv>
#include <string>
#include <string_view>
#include <utility>
#include <vector>

#include "encoding.h"
#include "redis_db.h"
#include "redis_metadata.h"
#include "storage.h"
#include "string_util.h"

namespace engine {

class WriteBatchDecoder : public rocksdb::WriteBatch::Handler {
 public:
  WriteBatchDecoder(bool detail, bool slot_id_encoded) : detail_(detail), slot_id_encoded_(slot_id_encoded) {}

  void LogData(const rocksdb::Slice &blob) override {
    std::string entry = "type=LOGDATA,raw_value_size=" + std::to_string(blob.size());
    if (detail_) {
      entry += ",raw_value=" + blob.ToString() + ",raw_value_hex=" + blob.ToString(true);
      redis::WriteBatchLogData data;
      if (!util::Split(blob.ToString(), " ").empty() && data.Decode(blob).IsOK()) {
        entry += ",redis_type=" + std::to_string(data.GetRedisType());
        for (const auto &arg : *data.GetArguments()) entry += ",arg=" + arg;
      }
    }
    entries_.push_back(std::move(entry));
  }

  rocksdb::Status PutCF(uint32_t cf, const Slice &key, const Slice &value) override {
    auto entry = makeEntry("PUT", cf, key);
    entry += ",raw_value_size=" + std::to_string(value.size());
    if (detail_) {
      entry += ",raw_value=" + value.ToString() + ",raw_value_hex=" + value.ToString(true);
      if (cf == static_cast<uint32_t>(ColumnFamilyID::Propagate)) {
        entry += decodePropagateValue(key, value);
      } else if (cf == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
        Metadata metadata(kRedisNone, false);
        auto s = metadata.Decode(value);
        if (!s.ok()) {
          entry += ",decode_error=" + s.ToString();
        } else {
          entry += ",type=" + (metadata.Type() < kRedisTypeMax ? std::string(metadata.TypeName()) : "unknown") +
                   ",expire_at_ms=" + std::to_string(metadata.expire);
          if (metadata.IsSingleKVType()) {
            entry += ",user_value=" + value.ToString().substr(Metadata::GetOffsetAfterExpire(metadata.flags));
          } else {
            entry += ",version=" + std::to_string(metadata.version) + ",size=" + std::to_string(metadata.size);
          }
        }
      }
    }
    entries_.push_back(std::move(entry));
    return rocksdb::Status::OK();
  }

  rocksdb::Status MergeCF(uint32_t cf, const Slice &key, const Slice &value) override {
    auto entry = makeEntry("MERGE", cf, key) + ",raw_value_size=" + std::to_string(value.size());
    if (detail_) entry += ",raw_value=" + value.ToString() + ",raw_value_hex=" + value.ToString(true);
    entries_.push_back(std::move(entry));
    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteCF(uint32_t cf, const Slice &key) override {
    entries_.push_back(makeEntry("DELETE", cf, key));
    return rocksdb::Status::OK();
  }

  rocksdb::Status SingleDeleteCF(uint32_t cf, const Slice &key) override {
    entries_.push_back(makeEntry("SINGLEDELETE", cf, key));
    return rocksdb::Status::OK();
  }

  rocksdb::Status DeleteRangeCF(uint32_t cf, const Slice &begin, const Slice &end) override {
    auto entry = makeEntry("DELETERANGE", cf, begin) + ",end_key_size=" + std::to_string(end.size());
    if (detail_) {
      entry += ",end_key=" + escapeBytes(end) + ",end_key_hex=" + end.ToString(true);
      entry += decodeKey(cf, end, "end_");
    }
    entries_.push_back(std::move(entry));
    return rocksdb::Status::OK();
  }

  const std::vector<std::string> &Get() const { return entries_; }

 private:
  std::string makeEntry(const char *operation, uint32_t cf, const Slice &key) const {
    std::string name = cf <= kMaxColumnFamilyID
                           ? std::string(ColumnFamilyConfigs::GetColumnFamily(static_cast<ColumnFamilyID>(cf)).Name())
                           : std::to_string(cf);
    auto entry = std::string("type=") + operation + ",cf=" + name + ",raw_key_size=" + std::to_string(key.size());
    if (!detail_) return entry;
    entry += ",raw_key=" + escapeBytes(key) + ",raw_key_hex=" + key.ToString(true);
    return entry + decodeKey(cf, key);
  }

  static std::string escapeBytes(const Slice &bytes) {
    constexpr char hex[] = "0123456789ABCDEF";
    std::string output;
    for (unsigned char c : bytes.ToStringView()) {
      if (c >= 0x20 && c <= 0x7e && c != '\\' && c != ',') {
        output.push_back(static_cast<char>(c));
      } else {
        output += "\\x";
        output.push_back(hex[c >> 4]);
        output.push_back(hex[c & 0xf]);
      }
    }
    return output;
  }

  struct PropagateKey {
    const char *kind;
    const char *field;
    Slice name;
  };

  static PropagateKey classifyPropagateKey(const Slice &key) {
    if (key == "replication_id_") return {"replication_id", nullptr, {}};
    if (key == kPropagateScriptCommand) return {"command", nullptr, {}};
    if (key.starts_with(kLuaFuncSHAPrefix)) {
      return {"lua_script", "sha",
              Slice(key.data() + std::string_view(kLuaFuncSHAPrefix).size(),
                    key.size() - std::string_view(kLuaFuncSHAPrefix).size())};
    }
    if (key.starts_with(kLuaFuncLibPrefix)) {
      return {"function_library", "function_name",
              Slice(key.data() + std::string_view(kLuaFuncLibPrefix).size(),
                    key.size() - std::string_view(kLuaFuncLibPrefix).size())};
    }
    if (key.starts_with(kLuaLibCodePrefix)) {
      return {"library_code", "library_name",
              Slice(key.data() + std::string_view(kLuaLibCodePrefix).size(),
                    key.size() - std::string_view(kLuaLibCodePrefix).size())};
    }
    return {"unknown", nullptr, {}};
  }

  static std::string byteField(const std::string &name, const Slice &value) {
    return "," + name + "=" + escapeBytes(value) + "," + name + "_hex=" + value.ToString(true);
  }

  static std::string decodePropagateValue(const Slice &key, const Slice &value) {
    std::string_view kind = classifyPropagateKey(key).kind;
    if (kind == "replication_id") return byteField("replication_id", value);
    if (kind == "lua_script" || kind == "library_code") return byteField("source", value);
    if (kind == "function_library") return byteField("library_name", value);
    if (kind != "command") return "";

    // Propagate writes exactly one RESP array of bulk strings. Parse lengths rather than delimiters in payloads.
    auto input = value.ToStringView();
    auto length = [&](char marker, uint64_t *result) {
      if (input.empty() || input.front() != marker) return false;
      auto end = input.find("\r\n");
      if (end == std::string_view::npos || end == 1) return false;
      auto digits = input.substr(1, end - 1);
      auto parsed = std::from_chars(digits.data(), digits.data() + digits.size(), *result);
      if (parsed.ec != std::errc{} || parsed.ptr != digits.data() + digits.size()) return false;
      input.remove_prefix(end + 2);
      return true;
    };
    constexpr const char *error = ",propagate_value_decode_error=invalid_RESP_command";
    uint64_t count = 0;
    if (!length('*', &count) || count == 0 || count > input.size() / 6) return error;
    std::string entry = ",command_argc=" + std::to_string(count);
    for (uint64_t i = 0; i < count; ++i) {
      uint64_t size = 0;
      if (!length('$', &size) || size > input.size()) return error;
      Slice arg(input.data(), static_cast<size_t>(size));
      input.remove_prefix(static_cast<size_t>(size));
      if (!input.starts_with("\r\n")) return error;
      input.remove_prefix(2);
      entry += byteField("command_arg_" + std::to_string(i), arg);
    }
    if (!input.empty()) return error;
    return entry;
  }

  std::string decodeKey(uint32_t cf, Slice input, const std::string &prefix = "") const {
    std::string entry;
    auto number = [&](const char *name, uint64_t value) { entry += "," + prefix + name + "=" + std::to_string(value); };
    auto bytes = [&](const char *name, const Slice &value) {
      entry += "," + prefix + name + "=" + escapeBytes(value);
      entry += "," + prefix + name + "_hex=" + value.ToString(true);
    };
    auto error = [&](const char *field) {
      return entry + "," + prefix + "key_decode_error=1," + prefix + "key_error_field=" + field;
    };
    if (cf == static_cast<uint32_t>(ColumnFamilyID::Propagate)) {
      auto decoded = classifyPropagateKey(input);
      entry += "," + prefix + "propagate_type=" + decoded.kind;
      if (decoded.field != nullptr) {
        number((std::string(decoded.field) + "_size").c_str(), decoded.name.size());
        bytes(decoded.field, decoded.name);
      }
      return entry;
    }
    if (cf != static_cast<uint32_t>(ColumnFamilyID::Metadata) &&
        cf != static_cast<uint32_t>(ColumnFamilyID::PrimarySubkey) &&
        cf != static_cast<uint32_t>(ColumnFamilyID::SecondarySubkey) &&
        cf != static_cast<uint32_t>(ColumnFamilyID::Stream))
      return entry;
    uint8_t ns_size = 0;
    if (!GetFixed8(&input, &ns_size)) return error("ns_size");
    number("ns_size", ns_size);
    if (input.size() < ns_size) return error("ns");
    bytes("ns", Slice(input.data(), ns_size));
    input.remove_prefix(ns_size);
    if (slot_id_encoded_) {
      uint16_t slot = 0;
      if (!GetFixed16(&input, &slot)) return error("slot");
      number("slot", slot);
    }
    if (cf == static_cast<uint32_t>(ColumnFamilyID::Metadata)) {
      // Metadata keys have no encoded key-length or version field.
      number("user_key_size", input.size());
      bytes("user_key", input);
      return entry;
    }
    uint32_t key_size = 0;
    if (!GetFixed32(&input, &key_size)) return error("user_key_size");
    number("user_key_size", key_size);
    if (input.size() < key_size) return error("user_key");
    bytes("user_key", Slice(input.data(), key_size));
    input.remove_prefix(key_size);
    uint64_t version = 0;
    if (!GetFixed64(&input, &version)) return error("version");
    number("version", version);
    number("sub_key_size", input.size());
    bytes("sub_key", input);
    if (cf == static_cast<uint32_t>(ColumnFamilyID::SecondarySubkey) && !input.empty()) {
      if (input.size() < sizeof(double)) return error("score");
      entry += "," + prefix + "score=" + std::to_string(DecodeDouble(input.data()));
      input.remove_prefix(sizeof(double));
      number("member_size", input.size());
      bytes("member", input);
    }
    return entry;
  }

  bool detail_;
  bool slot_id_encoded_;
  std::vector<std::string> entries_;
};

}  // namespace engine
