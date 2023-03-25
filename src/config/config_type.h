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
#include <string>
#include <utility>

#include "parse_util.h"
#include "status.h"
#include "string_util.h"

// forward declaration
class Server;

using validate_fn = std::function<Status(const std::string &, const std::string &)>;
using callback_fn = std::function<Status(Server *, const std::string &, const std::string &)>;

// forward declaration
template <typename>
class IntegerField;

using IntField = IntegerField<int>;
using UInt32Field = IntegerField<uint32_t>;
using Int64Field = IntegerField<int64_t>;

struct configEnum {
  const char *name;
  const int val;
};

enum configType { SingleConfig, MultiConfig };

int configEnumGetValue(configEnum *ce, const char *name);
const char *configEnumGetName(configEnum *ce, int val);

class ConfigField {
 public:
  ConfigField() = default;
  virtual ~ConfigField() = default;
  virtual std::string ToString() = 0;
  virtual Status Set(const std::string &v) = 0;
  virtual Status ToNumber(int64_t *n) { return {Status::NotOK, "not supported"}; }
  virtual Status ToBool(bool *b) { return {Status::NotOK, "not supported"}; }
  virtual configType GetConfigType() { return config_type; }
  virtual bool IsMultiConfig() { return config_type == configType::MultiConfig; }
  virtual bool IsSingleConfig() { return config_type == configType::SingleConfig; }

  int line_number = 0;
  bool readonly = true;
  validate_fn validate = nullptr;
  callback_fn callback = nullptr;
  configType config_type = configType::SingleConfig;
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
    this->config_type = configType::MultiConfig;
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
    if (strcasecmp(v.data(), "yes") == 0) {
      *receiver_ = true;
    } else if (strcasecmp(v.data(), "no") == 0) {
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
  EnumField(int *receiver, configEnum *enums, int e) : receiver_(receiver), enums_(enums) { *receiver_ = e; }
  ~EnumField() override = default;
  std::string ToString() override { return configEnumGetName(enums_, *receiver_); }
  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    int e = configEnumGetValue(enums_, v.c_str());
    if (e == INT_MIN) {
      return {Status::NotOK, "invalid enum option"};
    }
    *receiver_ = e;
    return Status::OK();
  }

 private:
  int *receiver_;
  configEnum *enums_ = nullptr;
};
