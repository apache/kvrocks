#pragma once

#include <string>
#include <utility>

#include "util.h"
#include "status.h"

// forward declaration
class Server;

typedef std::function<Status(const std::string&, const std::string&)> validate_fn;
typedef std::function<Status(Server *srv, const std::string&, const std::string&)> callback_fn;

typedef struct configEnum {
  const char *name;
  const int val;
} configEnum;
int configEnumGetValue(configEnum *ce, const char *name);
const char *configEnumGetName(configEnum *ce, int val);

class ConfigField {
 public:
  ConfigField() = default;
  virtual ~ConfigField() = 0;
  virtual std::string ToString() = 0;
  virtual Status Set(const std::string &v) = 0;
  virtual Status ToNumber(int64_t *n) { return Status(Status::NotOK, "not supported"); }
  virtual Status ToBool(bool *b) { return Status(Status::NotOK, "not supported"); }

 public:
  bool readonly = true;
  validate_fn validate = nullptr;
  callback_fn callback = nullptr;
};

class StringField: public ConfigField {
 public:
  StringField(std::string *receiver, std::string s): receiver_(receiver) {
    *receiver_ = std::move(s);
  }
  ~StringField() override = default;
  std::string ToString() override {
    return *receiver_;
  }
  Status Set(const std::string &v) override {
    *receiver_ = v;
    return Status::OK();
  }

 private:
  std::string *receiver_;
};

class IntField : public ConfigField {
 public:
  IntField(int *receiver, int n, int min, int max)
      : receiver_(receiver), min_(min), max_(max) {
    *receiver_ = n;
  }
  ~IntField() override = default;
  std::string ToString() override {
    return std::to_string(*receiver_);
  }
  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    int64_t n;
    auto s = Util::StringToNum(v, &n, min_, max_);
    if (!s.IsOK()) return s;
    *receiver_ = static_cast<int>(n);
    return Status::OK();
  }

 private:
  int *receiver_;
  int min_ = INT_MIN;
  int max_ = INT_MAX;
};

class Int64Field : public ConfigField {
 public:
  Int64Field(int64_t *receiver, int64_t n, int64_t min, int64_t max)
      : receiver_(receiver), min_(min), max_(max) {
    *receiver_ = n;
  }
  ~Int64Field() override = default;
  std::string ToString() override {
    return std::to_string(*receiver_);
  }
  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    int64_t n;
    auto s = Util::StringToNum(v, &n, min_, max_);
    if (!s.IsOK()) return s;
    *receiver_ = n;
    return Status::OK();
  }

 private:
  int64_t *receiver_;
  int64_t min_ = INT64_MIN;
  int64_t max_ = INT64_MAX;
};

class YesNoField : public ConfigField {
 public:
  YesNoField(bool *receiver, bool b) : receiver_(receiver) {
    *receiver_ = b;
  }
  ~YesNoField() override = default;
  std::string ToString() override {
    return *receiver_ ? "yes":"no";
  }
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
      return Status(Status::NotOK, "argument must be 'yes' or 'no'");
    }
    return Status::OK();
  }

 private:
  bool *receiver_;
};

class EnumField : public ConfigField {
 public:
  EnumField(int *receiver, configEnum *enums, int e) :
      receiver_(receiver), enums_(enums) {
    *receiver_ = e;
  }
  ~EnumField() override = default;
  std::string ToString() override {
    return configEnumGetName(enums_, *receiver_);
  }
  Status ToNumber(int64_t *n) override {
    *n = *receiver_;
    return Status::OK();
  }
  Status Set(const std::string &v) override {
    int e = configEnumGetValue(enums_, v.c_str());
    if (e == INT_MIN) {
      return Status(Status::NotOK, "invaild enum option");
    }
    *receiver_ = e;
    return Status::OK();
  }

 private:
  int *receiver_;
  configEnum *enums_ = nullptr;
};
