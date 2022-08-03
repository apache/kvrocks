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

#include <string>
#include <type_traits>
#include <utility>
#include <memory>
#include <algorithm>
#include <tuple>
#include <glog/logging.h>

class Status {
 public:
  enum Code : unsigned char {
    cOK = 0,
    NotOK,
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
  explicit Status(Code code, std::string msg = {})
    : code_(code), msg_(std::move(msg)) {}

  template <Code code>
  bool Is() const { return code_ == code; }

  bool IsOK() const { return Is<cOK>(); }
  operator bool() const { return IsOK(); }

  Code GetCode() const {
    return code_;
  }

  std::string Msg() const& {
    if (*this) return ok_msg();
    return msg_;
  }

  std::string Msg() && {
    if (*this) return ok_msg();
    return std::move(msg_);
  }

  static Status OK() { return {}; }

 private:
  Code code_;
  std::string msg_;

  static constexpr const char* ok_msg() {
    return "ok";
  }

  template <typename T>
  friend struct StatusOr;
};

template <typename ...Ts>
using first_element = typename std::tuple_element<0, std::tuple<Ts...>>::type;

template <typename T>
using remove_cvref_t = typename std::remove_cv<typename std::remove_reference<T>::type>::type;

template <typename T>
struct StatusOr {
  static_assert(!std::is_same<T, Status>::value, "value_type cannot be Status");

  using value_type = T;

  // we use std::unique_ptr to make the error part as small as enough
  using error_type = std::unique_ptr<std::string>;

  using Code = Status::Code;

  explicit StatusOr(Status s) : code_(s.code_) {
    CHECK(!s);
    new(storage_) error_type(new std::string(std::move(s.msg_)));
  }

  StatusOr(Code code, std::string msg = {}) : code_(code) { // NOLINT
    CHECK(code != Code::cOK);
    new(storage_) error_type(new std::string(std::move(msg)));
  }

  template <typename ...Ts,
    typename std::enable_if<
      (sizeof...(Ts) > 0 &&
        !std::is_same<Status, remove_cvref_t<first_element<Ts...>>>::value &&
        !std::is_same<Code, remove_cvref_t<first_element<Ts...>>>::value &&
        !std::is_same<value_type, remove_cvref_t<first_element<Ts...>>>::value &&
        !std::is_same<StatusOr, remove_cvref_t<first_element<Ts...>>>::value
      ), int>::type = 0> // NOLINT
  explicit StatusOr(Ts && ... args) : code_(Code::cOK) {
    new(storage_) value_type(std::forward<Ts>(args)...);
  }

  StatusOr(T&& value) : code_(Code::cOK) { // NOLINT
    new(storage_) value_type(std::move(value));
  }

  StatusOr(const T& value) : code_(Code::cOK) { // NOLINT
    new(storage_) value_type(value);
  }

  StatusOr(const StatusOr&) = delete;
  StatusOr(StatusOr&& other) : code_(other.code_) {
    if (code_ == Code::cOK) {
      new(storage_) value_type(std::move(other.getValue()));
    } else {
      new(storage_) error_type(std::move(other.getError()));
    }
  }

  Status& operator=(const Status&) = delete;

  template <Code code>
  bool Is() const { return code_ == code; }

  bool IsOK() const { return Is<Code::cOK>(); }
  operator bool() const { return IsOK(); }

  Status ToStatus() const& {
    if (*this) return Status::OK();
    return Status(code_, *getError());
  }

  Status ToStatus() && {
    if (*this) return Status::OK();
    return Status(code_, std::move(*getError()));
  }

  Code GetCode() const {
    return code_;
  }

  value_type& GetValue() & {
    CHECK(*this);
    return getValue();
  }

  value_type&& GetValue() && {
    CHECK(*this);
    return std::move(getValue());
  }

  const value_type& GetValue() const& {
    CHECK(*this);
    return getValue();
  }

  value_type& operator*() & {
    return GetValue();
  }

  value_type&& operator*() && {
    return std::move(GetValue());
  }

  const value_type& operator*() const& {
    return GetValue();
  }

  value_type* operator->() {
    return &GetValue();
  }

  const value_type* operator->() const {
    return &GetValue();
  }

  std::string Msg() const& {
    if (*this) return Status::ok_msg();
    return *getError();
  }

  std::string Msg() && {
    if (*this) return Status::ok_msg();
    return std::move(*getError());
  }

  ~StatusOr() {
    if (*this) {
      getValue().~value_type();
    } else {
      getError().~error_type();
    }
  }

 private:
  Status::Code code_;
  alignas(value_type) alignas(error_type) unsigned char storage_[std::max(sizeof(value_type), sizeof(error_type))];

  value_type& getValue() {
    return *reinterpret_cast<value_type*>(storage_);
  }

  const value_type& getValue() const {
    return *reinterpret_cast<const value_type*>(storage_);
  }

  error_type& getError() {
    return *reinterpret_cast<error_type*>(storage_);
  }

  const error_type& getError() const {
    return *reinterpret_cast<const error_type*>(storage_);
  }
};
