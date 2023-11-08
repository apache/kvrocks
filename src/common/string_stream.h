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

#include "endianconv.h"
#include "status.h"

class InputStringStream {
 public:
  InputStringStream() = default;

  explicit InputStringStream(std::string_view input);

  size_t ReadableSize() const;
  const char *Data() const;
  void Consume(size_t size);

  template <typename T>
  T Read();

  std::string Read(size_t n);

 private:
  void peak(size_t n) const;

  std::string_view input_;
  size_t pos_{0};
};

template <typename T>
T InputStringStream::Read() {
  static_assert(std::is_integral<T>::value);
  static_assert(sizeof(T) <= 8);

  size_t len = sizeof(T);
  Consume(len);

  T v;
  memcpy(&v, Data() - len, len);

  if (sizeof(T) == 2) {
    return intrev16ifbe(v);
  } else if (sizeof(T) == 4) {
    return intrev32ifbe(v);
  } else if (sizeof(T) == 8) {
    return intrev64ifbe(v);
  }
  return v;
}