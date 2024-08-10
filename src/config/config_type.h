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

#include <fmt/format.h>

#include <climits>
#include <functional>
#include <limits>
#include <numeric>
#include <string>
#include <utility>

#include "bit_util.h"
#include "parse_util.h"
#include "status.h"
#include "string_util.h"

// forward declaration
class Server;

using ValidateFn = std::function<Status(const std::string &, const std::string &)>;
using CallbackFn = std::function<Status(Server *, const std::string &, const std::string &)>;

// forward declaration
template <typename>
class IntegerField;

using IntField = IntegerField<int>;
using UInt32Field = IntegerField<uint32_t>;
using Int64Field = IntegerField<int64_t>;
using UInt64Field = IntegerField<uint64_t>;

template <typename Enum>
struct ConfigEnum {
  std::string name;
  Enum val;
};

enum ConfigType { SingleConfig, MultiConfig };

class ConfigField {
 public:
  ConfigField() = default;
  virtual ~ConfigField() = default;
  virtual std::string Default() const = 0;
  virtual std::string ToString() const = 0;
  virtual std::string ToStringForRewrite() const { return ToString(); }
  virtual Status Set(const std::string &v) = 0;
  virtual Status ToNumber(int64_t *n) const { return {Status::NotOK, "not supported"}; }
  virtual Status ToBool(bool *b) const { return {Status::NotOK, "not supported"}; }

  ConfigType GetConfigType() const { return config_type; }
  bool IsMultiConfig() const { return config_type == ConfigType::MultiConfig; }
  bool IsSingleConfig() const { return config_type == ConfigType::SingleConfig; }

  int line_number = 0;
  bool readonly = true;
  ValidateFn validate = nullptr;
  CallbackFn callback = nullptr;
  ConfigType config_type = ConfigType::SingleConfig;
};

class StringField : public ConfigField {
 public:
  StringField(std::string *receiver, std::string s) : receiver_(receiver), default_(s) { *receiver_ = std::move(s); }
  ~StringField() override = default;
  std::string Default() const override { return default_; }
  std::string ToString() const override { return *receiver_; }
  Status Set(const std::string &v) override {
    *receiver_ = v;
    return Status::OK();
  }

 private:
  std::string *receiver_;
  std::string default_;
};

class MultiStringField : public ConfigField {
 public:
  MultiStringField(std::vector<std::string> *receiver, std::vector<std::string> input)
      : receiver_(receiver), default_(input) {
    *receiver_ = std::move(input);
    this->config_type = ConfigType::MultiConfig;
  }
  ~MultiStringField() override = default;

  std::string Default() const override { return format(default_); }
  std::string ToString() const override { return format(*receiver_); }
  Status Set(const std::string &v) override {
    receiver_->emplace_back(v);
    return Status::OK();
  }

 private:
  std::vector<std::string> *receiver_;
  std::vector<std::string> default_;

  static std::string format(const std::vector<std::string> &v) {
    std::string tmp;
    for (auto &p : v) {
      tmp += p + "\n";
    }
    return tmp;
  }
};

template <typename IntegerType>
class IntegerField : public ConfigField {
 public:
  IntegerField(IntegerType *receiver, IntegerType n, IntegerType min, IntegerType max)
      : receiver_(receiver), default_(n), min_(min), max_(max) {
    CHECK(min <= n && n <= max);
    *receiver_ = n;
  }
  ~IntegerField() override = default;
  std::string Default() const override { return std::to_string(default_); }
  std::string ToString() const override { return std::to_string(*receiver_); }
  Status ToNumber(int64_t *n) const override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    auto s = ParseInt<IntegerType>(v, {min_, max_});
    if (!s.IsOK()) return std::move(s);
    *receiver_ = s.GetValue();
    return Status::OK();
  }

 private:
  IntegerType *receiver_;
  IntegerType default_ = 0;
  IntegerType min_ = std::numeric_limits<IntegerType>::min();
  IntegerType max_ = std::numeric_limits<IntegerType>::max();
};

class OctalField : public ConfigField {
 public:
  OctalField(int *receiver, int n, int min, int max) : receiver_(receiver), default_(n), min_(min), max_(max) {
    *receiver_ = n;
  }
  ~OctalField() override = default;

  std::string Default() const override { return fmt::format("{:o}", default_); }
  std::string ToString() const override { return fmt::format("{:o}", *receiver_); }
  Status ToNumber(int64_t *n) const override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    auto s = ParseInt<int>(v, {min_, max_}, 8);
    if (!s.IsOK()) return std::move(s);
    *receiver_ = *s;
    return Status::OK();
  }

 private:
  int *receiver_;
  int default_ = 0;
  int min_ = INT_MIN;
  int max_ = INT_MAX;
};

class YesNoField : public ConfigField {
 public:
  YesNoField(bool *receiver, bool b) : receiver_(receiver), default_(b) { *receiver_ = b; }
  ~YesNoField() override = default;

  std::string Default() const override { return default_ ? "yes" : "no"; }
  std::string ToString() const override { return *receiver_ ? "yes" : "no"; }
  Status ToBool(bool *b) const override {
    *b = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    if (util::EqualICase(v, "yes")) {
      *receiver_ = true;
    } else if (util::EqualICase(v, "no")) {
      *receiver_ = false;
    } else {
      return {Status::NotOK, "argument must be 'yes' or 'no'"};
    }
    return Status::OK();
  }

 private:
  bool *receiver_;
  bool default_;
};

template <typename Enum>
class EnumField : public ConfigField {
 public:
  using EnumItem = ConfigEnum<Enum>;
  using EnumItems = std::vector<EnumItem>;

  EnumField(Enum *receiver, EnumItems enums, Enum e) : receiver_(receiver), default_(e), enums_(std::move(enums)) {
    *receiver_ = e;
  }
  ~EnumField() override = default;

  std::string Default() const override { return enumToString(default_); }
  std::string ToString() const override { return enumToString(*receiver_); }
  Status ToNumber(int64_t *n) const override {
    *n = static_cast<int64_t>(*receiver_);
    return Status::OK();
  }

  Status Set(const std::string &v) override {
    for (const auto &e : enums_) {
      if (util::EqualICase(e.name, v)) {
        *receiver_ = e.val;
        return Status::OK();
      }
    }

    auto acceptable_values = util::StringJoin(enums_, [](const auto &e) { return fmt::format("'{}'", e.name); });
    return {Status::NotOK, fmt::format("invalid enum option, acceptable values are {}", acceptable_values)};
  }

 private:
  Enum *receiver_;
  Enum default_;
  EnumItems enums_;

  std::string enumToString(const Enum v) const {
    for (const auto &e : enums_) {
      if (e.val == v) return e.name;
    }
    return {};
  }
};

enum class IntUnit { None = 0, K = 10, M = 20, G = 30, T = 40, P = 50 };

template <typename T>
class IntWithUnitField : public ConfigField {
 public:
  IntWithUnitField(T *receiver, std::string default_val, T min, T max)
      : receiver_(receiver), default_val_(std::move(default_val)), min_(min), max_(max) {
    CHECK(ReadFrom(default_val_));
  }

  std::string Default() const override { return default_val_; }
  std::string ToString() const override { return std::to_string(*receiver_); }
  std::string ToStringForRewrite() const override { return ToString(*receiver_, current_unit_); }

  Status ToNumber(int64_t *n) const override {
    *n = static_cast<int64_t>(*receiver_);
    return Status::OK();
  }

  Status Set(const std::string &v) override { return ReadFrom(v); }

  Status ReadFrom(const std::string &val) {
    auto [num, rest] = GET_OR_RET(TryParseInt<T>(val.c_str(), 10));

    if (*rest == 0) {
      *receiver_ = num;
      current_unit_ = IntUnit::None;
    } else if (util::EqualICase(rest, "k")) {
      *receiver_ = GET_OR_RET(util::CheckedShiftLeft(num, 10));
      current_unit_ = IntUnit::K;
    } else if (util::EqualICase(rest, "m")) {
      *receiver_ = GET_OR_RET(util::CheckedShiftLeft(num, 20));
      current_unit_ = IntUnit::M;
    } else if (util::EqualICase(rest, "g")) {
      *receiver_ = GET_OR_RET(util::CheckedShiftLeft(num, 30));
      current_unit_ = IntUnit::G;
    } else if (util::EqualICase(rest, "t")) {
      *receiver_ = GET_OR_RET(util::CheckedShiftLeft(num, 40));
      current_unit_ = IntUnit::T;
    } else if (util::EqualICase(rest, "p")) {
      *receiver_ = GET_OR_RET(util::CheckedShiftLeft(num, 50));
      current_unit_ = IntUnit::P;
    } else {
      return {Status::NotOK, fmt::format("encounter unexpected unit: `{}`", rest)};
    }

    if (min_ > *receiver_ || max_ < *receiver_) {
      return {Status::NotOK, fmt::format("this config value should be between {} and {}", min_, max_)};
    }

    return Status::OK();
  }

  static std::string ToString(T val, IntUnit current_unit) {
    std::string unit_str;

    switch (current_unit) {
      case IntUnit::None:
        unit_str = "";
        break;
      case IntUnit::K:
        unit_str = "K";
        break;
      case IntUnit::M:
        unit_str = "M";
        break;
      case IntUnit::G:
        unit_str = "G";
        break;
      case IntUnit::T:
        unit_str = "T";
        break;
      case IntUnit::P:
        unit_str = "P";
        break;
    }

    return std::to_string(val >> int(current_unit)) + unit_str;
  }

 private:
  // NOTE: the receiver here will get the converted integer (e.g. "10k" -> get 10240)
  T *receiver_;
  std::string default_val_;
  T min_;
  T max_;
  IntUnit current_unit_ = IntUnit::None;
};
