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

struct ConfigEnum {
  const std::string name;
  const int val;
};

enum ConfigType { SingleConfig, MultiConfig };

class ConfigField {
 public:
  ConfigField() = default;
  virtual ~ConfigField() = default;
  virtual std::string ToString() = 0;
  virtual Status Set(const std::string &v) = 0;
  virtual Status ToNumber(int64_t *n) { return {Status::NotOK, "not supported"}; }
  virtual Status ToBool(bool *b) { return {Status::NotOK, "not supported"}; }
  virtual ConfigType GetConfigType() { return config_type; }
  virtual bool IsMultiConfig() { return config_type == ConfigType::MultiConfig; }
  virtual bool IsSingleConfig() { return config_type == ConfigType::SingleConfig; }

  int line_number = 0;
  bool readonly = true;
  ValidateFn validate = nullptr;
  CallbackFn callback = nullptr;
  ConfigType config_type = ConfigType::SingleConfig;
};

class StringField : public ConfigField {
 public:
  StringField(std::string *receiver, std::string s) : receiver_(receiver) { *receiver_ = std::move(s); }
  ~StringField() override = default;
  std::string ToString() override { return *receiver_; }
  Status Set(const std::string &v) override {
    *receiver_ = v;
    return Status::OK();
  }

 private:
  std::string *receiver_;
};

class MultiStringField : public ConfigField {
 public:
  MultiStringField(std::vector<std::string> *receiver, std::vector<std::string> input) : receiver_(receiver) {
    *receiver_ = std::move(input);
    this->config_type = ConfigType::MultiConfig;
  }
  ~MultiStringField() override = default;
  std::string ToString() override {
    std::string tmp;
    for (auto &p : *receiver_) {
      tmp += p + "\n";
    }
    return tmp;
  }
  Status Set(const std::string &v) override {
    receiver_->emplace_back(v);
    return Status::OK();
  }

 private:
  std::vector<std::string> *receiver_;
};

template <typename IntegerType>
class IntegerField : public ConfigField {
 public:
  IntegerField(IntegerType *receiver, IntegerType n, IntegerType min, IntegerType max)
      : receiver_(receiver), min_(min), max_(max) {
    *receiver_ = n;
  }
  ~IntegerField() override = default;
  std::string ToString() override { return std::to_string(*receiver_); }
  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    auto s = ParseInt<IntegerType>(v, {min_, max_});
    if (!s.IsOK()) return s;
    *receiver_ = s.GetValue();
    return Status::OK();
  }

 private:
  IntegerType *receiver_;
  IntegerType min_ = std::numeric_limits<IntegerType>::min();
  IntegerType max_ = std::numeric_limits<IntegerType>::max();
};

class OctalField : public ConfigField {
 public:
  OctalField(int *receiver, int n, int min, int max) : receiver_(receiver), min_(min), max_(max) { *receiver_ = n; }
  ~OctalField() override = default;
  std::string ToString() override { return fmt::format("{:o}", *receiver_); }
  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    auto s = ParseInt<int>(v, {min_, max_}, 8);
    if (!s.IsOK()) return s;
    *receiver_ = *s;
    return Status::OK();
  }

 private:
  int *receiver_;
  int min_ = INT_MIN;
  int max_ = INT_MAX;
};

class YesNoField : public ConfigField {
 public:
  YesNoField(bool *receiver, bool b) : receiver_(receiver) { *receiver_ = b; }
  ~YesNoField() override = default;
  std::string ToString() override { return *receiver_ ? "yes" : "no"; }
  Status ToBool(bool *b) override {
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
};

class EnumField : public ConfigField {
 public:
  EnumField(int *receiver, std::vector<ConfigEnum> enums, int e) : receiver_(receiver), enums_(std::move(enums)) {
    *receiver_ = e;
  }
  ~EnumField() override = default;

  std::string ToString() override {
    for (const auto &e : enums_) {
      if (e.val == *receiver_) return e.name;
    }
    return {};
  }

  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }

  Status Set(const std::string &v) override {
    for (const auto &e : enums_) {
      if (util::EqualICase(e.name, v)) {
        *receiver_ = e.val;
        return Status::OK();
      }
    }
    return {Status::NotOK, fmt::format("invalid enum option, acceptable values are {}",
                                       std::accumulate(enums_.begin(), enums_.end(), std::string{},
                                                       [this](const std::string &res, const ConfigEnum &e) {
                                                         if (&e != &enums_.back()) return res + "'" + e.name + "', ";

                                                         return res + "'" + e.name + "'";
                                                       }))};
  }

 private:
  int *receiver_;
  std::vector<ConfigEnum> enums_;
};
