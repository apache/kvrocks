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

#include <memory>

enum class ObserverOrUnique : char { Observer, Unique };

template <typename T, typename D = std::default_delete<T>>
struct ObserverOrUniquePtr : private D {
  explicit ObserverOrUniquePtr(T* ptr, ObserverOrUnique own) : ptr_(ptr), own_(own) {}

  ObserverOrUniquePtr(ObserverOrUniquePtr&& p) : ptr_(p.ptr_), own_(p.own_) { p.Release(); }
  ObserverOrUniquePtr& operator=(ObserverOrUniquePtr&& p) {
    Reset(p.ptr_);
    own_ = p.own_;
    p.Release();

    return *this;
  }

  ObserverOrUniquePtr(const ObserverOrUniquePtr&) = delete;
  ObserverOrUniquePtr& operator=(const ObserverOrUniquePtr&) = delete;

  ~ObserverOrUniquePtr() {
    if (own_ == ObserverOrUnique::Unique) {
      (*this)(ptr_);
    }
  }

  T* operator->() const { return ptr_; }
  T* Get() const { return ptr_; }
  T* Release() {
    own_ = ObserverOrUnique::Observer;
    return ptr_;
  }

  void Reset(T* ptr = nullptr) {
    if (own_ == ObserverOrUnique::Unique) {
      (*this)(ptr_);
    }

    ptr_ = ptr;
  }

 private:
  T* ptr_;
  ObserverOrUnique own_;
};
