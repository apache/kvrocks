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
#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

class [[nodiscard]] Status {
 public:
  enum Code : unsigned char {
    NotOK = 1,
    NotFound,

    // DB
    DBOpenErr,
    DBBackupErr,
    DBGetWALErr,
    DBBackupFileErr,

    // Replication
    DBMismatched,

    // Redis
    RedisUnknownCmd,
    RedisInvalidCmd,
    RedisParseErr,
    RedisExecErr,
    RedisReplicationConflict,

    // Cluster
    ClusterDown,
    ClusterInvalidInfo,

    // Slot
    SlotImport,

    // Network
    NetSendErr,

    // Blocking
    BlockingCmd,
  };

  Status() : Status(cOK) {}

  Status(Code code, std::string msg = {}) : code_(code), msg_(std::move(msg)) {}  // NOLINT

  template <Code code>
  bool Is() const {
    return code_ == code;
  }

  bool IsOK() const { return Is<cOK>(); }
  explicit operator bool() const { return IsOK(); }

  Code GetCode() const { return code_; }

  std::string Msg() const& {
    if (*this) return ok_msg;
    return msg_;
  }

  std::string Msg() && {
    if (*this) return ok_msg;
    return std::move(msg_);
  }

  static Status OK() { return {}; }

  static Status FromErrno() { return {NotOK, strerror(errno)}; }
  static Status FromErrno(std::string_view prefix) { return {NotOK, fmt::format("{}: {}", prefix, strerror(errno))}; }

  Status Prefixed(std::string_view prefix) const {
    if (*this) {
      return {};
    }
    return {code_, fmt::format("{}: {}", prefix, msg_)};
  }

  void GetValue() {}

  static constexpr const char* ok_msg = "ok";

 private:
  Code code_;
  std::string msg_;

  static constexpr Code cOK = static_cast<Code>(0);

  template <typename>
  friend struct StatusOr;
};

namespace type_details {
template <typename... Ts>
using first_element_t = typename std::tuple_element_t<0, std::tuple<Ts...>>;

template <typename T>
using remove_cvref_t = typename std::remove_cv_t<typename std::remove_reference_t<T>>;
}  // namespace type_details

template <typename, typename = void>
struct StringInStatusOr : private std::string {
  using base_type = std::string;
  static constexpr bool inplace = true;

  explicit StringInStatusOr(std::string&& v) : base_type(std::move(v)) {}

  template <typename U>
  StringInStatusOr(StringInStatusOr<U>&& v) : base_type(*std::move(v)) {}  // NOLINT
  StringInStatusOr(const StringInStatusOr& v) = delete;

  StringInStatusOr& operator=(const StringInStatusOr&) = delete;

  std::string& operator*() & { return *this; }

  const std::string& operator*() const& { return *this; }

  std::string&& operator*() && { return std::move(*this); }

  ~StringInStatusOr() = default;
};

template <typename T>
struct StringInStatusOr<T, std::enable_if_t<sizeof(T) < sizeof(std::string)>> : private std::unique_ptr<std::string> {
  using base_type = std::unique_ptr<std::string>;
  static constexpr bool inplace = false;

  explicit StringInStatusOr(std::string&& v) : base_type(new std::string(std::move(v))) {}

  template <typename U, typename std::enable_if_t<StringInStatusOr<U>::inplace, int> = 0>
  StringInStatusOr(StringInStatusOr<U>&& v) : base_type(new std::string(*std::move(v))) {}  // NOLINT
  template <typename U, typename std::enable_if_t<!StringInStatusOr<U>::inplace, int> = 0>
  StringInStatusOr(StringInStatusOr<U>&& v)  // NOLINT
      : base_type((typename StringInStatusOr<U>::base_type &&)(std::move(v))) {}

  StringInStatusOr(const StringInStatusOr& v) = delete;

  StringInStatusOr& operator=(const StringInStatusOr&) = delete;

  std::string& operator*() & { return base_type::operator*(); }

  const std::string& operator*() const& { return base_type::operator*(); }

  std::string&& operator*() && { return std::move(base_type::operator*()); }

  ~StringInStatusOr() = default;
};

template <typename>
struct StatusOr;

template <typename>
struct IsStatusOr : std::integral_constant<bool, false> {};

template <typename T>
struct IsStatusOr<StatusOr<T>> : std::integral_constant<bool, true> {};

template <typename T>
struct [[nodiscard]] StatusOr {
  static_assert(!std::is_same_v<T, Status>, "value_type cannot be Status");
  static_assert(!std::is_same_v<T, Status::Code>, "value_type cannot be Status::Code");
  static_assert(!IsStatusOr<T>::value, "value_type cannot be StatusOr");
  static_assert(!std::is_reference_v<T>, "value_type cannot be reference");

  using value_type = T;

  // we use std::unique_ptr to make the error part as small as enough
  using error_type = StringInStatusOr<value_type>;

  using Code = Status::Code;

  StatusOr(Status s) : code_(s.code_) {  // NOLINT
    CHECK(!s);
    new (&error_) error_type(std::move(s.msg_));
  }

  StatusOr(Code code, std::string msg = {}) : code_(code) {  // NOLINT
    CHECK(code != Status::cOK);
    new (&error_) error_type(std::move(msg));
  }

  template <typename... Ts,
            typename std::enable_if<
                (sizeof...(Ts) > 0 &&
                 !std::is_same_v<Status, type_details::remove_cvref_t<type_details::first_element_t<Ts...>>> &&
                 !std::is_same_v<Code, type_details::remove_cvref_t<type_details::first_element_t<Ts...>>> &&
                 !std::is_same_v<StatusOr, type_details::remove_cvref_t<type_details::first_element_t<Ts...>>>),
                int>::type = 0>                  // NOLINT
  StatusOr(Ts&&... args) : code_(Status::cOK) {  // NOLINT
    new (&value_) value_type(std::forward<Ts>(args)...);
  }

  StatusOr(const StatusOr&) = delete;

  template <typename U, typename std::enable_if<std::is_convertible<U, T>::value, int>::type = 0>
  StatusOr(StatusOr<U>&& other) : code_(other.code_) {  // NOLINT
    if (code_ == Status::cOK) {
      new (&value_) value_type(std::move(other.value_));
    } else {
      new (&error_) error_type(std::move(other.error_));
    }
  }

  template <typename U, typename std::enable_if<!std::is_convertible<U, T>::value, int>::type = 0>
  StatusOr(StatusOr<U>&& other) : code_(other.code_) {  // NOLINT
    CHECK(code_ != Status::cOK);
    new (&error_) error_type(std::move(other.error_));
  }

  StatusOr& operator=(const StatusOr&) = delete;

  template <Code code>
  bool Is() const {
    return code_ == code;
  }

  bool IsOK() const { return Is<Status::cOK>(); }
  explicit operator bool() const { return IsOK(); }

  Status ToStatus() const& {
    if (*this) return Status::OK();
    return {code_, *error_};
  }

  Status ToStatus() && {
    if (*this) return Status::OK();
    return {code_, std::move(*error_)};
  }

  operator Status() const& { return ToStatus(); }  // NOLINT

  operator Status() && { return std::move(*this).ToStatus(); }  // NOLINT

  Code GetCode() const { return code_; }

  value_type& GetValue() & {
    CHECK(*this);
    return value_;
  }

  value_type&& GetValue() && {
    CHECK(*this);
    return std::move(value_);
  }

  const value_type& ValueOr(const value_type& v) const& {
    if (IsOK()) {
      return GetValue();
    } else {
      return v;
    }
  }

  value_type ValueOr(value_type&& v) const& {
    if (IsOK()) {
      return GetValue();
    } else {
      return std::move(v);
    }
  }

  value_type ValueOr(const T& v) && {
    if (IsOK()) {
      return std::move(*this).GetValue();
    } else {
      return v;
    }
  }

  value_type&& ValueOr(T&& v) && {
    if (IsOK()) {
      return std::move(*this).GetValue();
    } else {
      return std::move(v);
    }
  }

  const value_type& GetValue() const& {
    CHECK(*this);
    return value_;
  }

  value_type& operator*() & { return GetValue(); }

  value_type&& operator*() && { return std::move(GetValue()); }

  const value_type& operator*() const& { return GetValue(); }

  value_type* operator->() { return &GetValue(); }

  const value_type* operator->() const { return &GetValue(); }

  std::string Msg() const& {
    if (*this) return Status::ok_msg;
    return *error_;
  }

  std::string Msg() && {
    if (*this) return Status::ok_msg;
    return std::move(*error_);
  }

  StatusOr Prefixed(std::string_view prefix) && {
    if (*this) {
      return std::move(*this);
    }
    return {code_, fmt::format("{}: {}", prefix, std::move(*error_))};
  }

  ~StatusOr() {
    if (*this) {
      value_.~value_type();
    } else {
      error_.~error_type();
    }
  }

 private:
  Status::Code code_;
  union {
    value_type value_;
    error_type error_;
  };

  template <typename>
  friend struct StatusOr;
};

// NOLINTNEXTLINE
#define GET_OR_RET(...)                                         \
  ({                                                            \
    auto&& status = (__VA_ARGS__);                              \
    if (!status) return std::forward<decltype(status)>(status); \
    std::forward<decltype(status)>(status);                     \
  }).GetValue()
