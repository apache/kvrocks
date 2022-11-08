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

#include <glog/logging.h>

#include <algorithm>
#include <memory>
#include <string>
#include <tuple>
#include <type_traits>
#include <utility>

class Status {
 public:
  enum Code : unsigned char {
    kOK = 0,
    kNotOK,
    kNotFound,

    // DB
    kDBOpenErr,
    kDBBackupErr,
    kDBGetWALErr,
    kDBBackupFileErr,

    // Replication
    kDBMismatched,

    // Redis
    kRedisUnknownCmd,
    kRedisInvalidCmd,
    kRedisParseErr,
    kRedisExecErr,
    RedisReplicationConflict,

    // Cluster
    kClusterDown,
    kClusterInvalidInfo,

    // Slot
    kSlotImport,

    // Network
    kNetSendErr,

    // Blocking
    kBlockingCmd,
  };

  Status() : Status(kOK) {}
  Status(Code code, std::string msg = {}) : code_(code), msg_(std::move(msg)) {}  // NOLINT

  template <Code code>
  bool Is() const {
    return code_ == code;
  }

  bool IsOK() const { return Is<kOK>(); }
  bool IsNotFound() const { return Is<kNotFound>(); };
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
  static Status FromErrno() { return {kNotOK, strerror(errno)}; }
  static Status NotFound(std::string msg = {}) { return {kNotFound, std::move(msg)}; }
  static Status NotOK(std::string msg = {}) { return {kNotOK, std::move(msg)}; }
  static Status ParseError(std::string msg = {}) { return {kRedisParseErr, std::move(msg)}; }
  static Status ExecError(std::string msg = {}) { return {kRedisExecErr, std::move(msg)}; }

  void GetValue() {}

 private:
  Code code_;
  std::string msg_;

  static constexpr const char* ok_msg = "ok";

  template <typename>
  friend struct StatusOr;
};

template <typename... Ts>
using first_element = typename std::tuple_element<0, std::tuple<Ts...>>::type;

template <typename T>
using remove_cvref_t = typename std::remove_cv<typename std::remove_reference<T>::type>::type;

template <typename>
struct StatusOr;

template <typename>
struct IsStatusOr : std::integral_constant<bool, false> {};

template <typename T>
struct IsStatusOr<StatusOr<T>> : std::integral_constant<bool, true> {};

template <typename T>
struct StatusOr {
  static_assert(!std::is_same<T, Status>::value, "value_type cannot be Status");
  static_assert(!std::is_same<T, Status::Code>::value, "value_type cannot be Status::Code");
  static_assert(!IsStatusOr<T>::value, "value_type cannot be StatusOr");
  static_assert(!std::is_reference<T>::value, "value_type cannot be reference");

  using value_type = T;

  // we use std::unique_ptr to make the error part as small as enough
  using error_type = std::unique_ptr<std::string>;

  using Code = Status::Code;

  StatusOr(Status s) : code_(s.code_) {  // NOLINT
    CHECK(!s);
    new (&error_) error_type(new std::string(std::move(s.msg_)));
  }

  StatusOr(Code code, std::string msg = {}) : code_(code) {  // NOLINT
    CHECK(code != Code::kOK);
    new (&error_) error_type(new std::string(std::move(msg)));
  }

  template <typename... Ts,
            typename std::enable_if<(sizeof...(Ts) > 0 &&
                                     !std::is_same<Status, remove_cvref_t<first_element<Ts...>>>::value &&
                                     !std::is_same<Code, remove_cvref_t<first_element<Ts...>>>::value &&
                                     !std::is_same<StatusOr, remove_cvref_t<first_element<Ts...>>>::value),
                                    int>::type = 0>  // NOLINT
  StatusOr(Ts&&... args) : code_(Code::kOK) {
    new (&value_) value_type(std::forward<Ts>(args)...);
  }

  StatusOr(const StatusOr&) = delete;

  template <typename U, typename std::enable_if<std::is_convertible<U, T>::value, int>::type = 0>
  StatusOr(StatusOr<U>&& other) : code_(other.code_) {
    if (code_ == Code::kOK) {
      new (&value_) value_type(std::move(other.value_));
    } else {
      new (&error_) error_type(std::move(other.error_));
    }
  }

  template <typename U, typename std::enable_if<!std::is_convertible<U, T>::value, int>::type = 0>
  StatusOr(StatusOr<U>&& other) : code_(other.code_) {
    CHECK(code_ != Code::kOK);
    new (&error_) error_type(std::move(other.error_));
  }

  Status& operator=(const Status&) = delete;

  template <Code code>
  bool Is() const {
    return code_ == code;
  }

  bool IsOK() const { return Is<Code::kOK>(); }
  explicit operator bool() const { return IsOK(); }

  Status ToStatus() const& {
    if (*this) return Status::OK();
    return Status(code_, *error_);
  }

  Status ToStatus() && {
    if (*this) return Status::OK();
    return Status(code_, std::move(*error_));
  }

  operator Status() const& { return ToStatus(); }

  operator Status() && { return std::move(*this).ToStatus(); }

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

#define GET_OR_RET(...)                                         \
  ({                                                            \
    auto&& status = (__VA_ARGS__);                              \
    if (!status) return std::forward<decltype(status)>(status); \
    std::forward<decltype(status)>(status);                     \
  }).GetValue()
