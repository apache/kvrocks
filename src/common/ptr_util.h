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

#include <memory>

#pragma once

enum class ObserverOrUnique : char { Observer, Unique };

template <typename T, typename D = std::default_delete<T>>
struct ObserverOrUniquePtr : private D {
  template <typename U>
  explicit ObserverOrUniquePtr(U* ptr, ObserverOrUnique own) : ptr(static_cast<T*>(ptr)), own(own) {}

  ObserverOrUniquePtr(ObserverOrUniquePtr&& p) : ptr(static_cast<T*>(p.ptr)), own(p.own) {
    p.ptr = nullptr;
  }
  ObserverOrUniquePtr(const ObserverOrUniquePtr&) = delete;
  ObserverOrUniquePtr& operator=(const ObserverOrUniquePtr&) = delete;

  ~ObserverOrUniquePtr() {
    if (own == ObserverOrUnique::Unique) {
      (*this)(ptr);
    }
  }

  T* operator->() const { return ptr; }
  T* Get() const { return ptr; }
  T* Release() {
    own = ObserverOrUnique::Observer;
    return ptr;
  }

 private:
  T* ptr;
  ObserverOrUnique own;
};