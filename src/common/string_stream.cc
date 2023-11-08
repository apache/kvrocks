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

#include "common/string_stream.h"

InputStringStream::InputStringStream(std::string_view input) : input_(input) {}

const char* InputStringStream::Data() const { return input_.data() + pos_; }

size_t InputStringStream::ReadableSize() const { return input_.size() - pos_; }

void InputStringStream::Consume(size_t size) {
  check(size);
  pos_ += size;
}

std::string InputStringStream::Read(size_t size) {
  check(size);
  std::string str{Data(), size};
  Consume(size);
  return str;
}

void InputStringStream::check(size_t n) const {
  if (ReadableSize() < n) {
    throw std::out_of_range{"unexpected end of stream"};
  }
}
