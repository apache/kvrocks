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
#include <cstdint>
#include <string>

// crc16
constexpr const uint16_t HASH_SLOTS_MASK = 0x3fff;
constexpr const uint16_t HASH_SLOTS_SIZE = HASH_SLOTS_MASK + 1;  // 16384
constexpr const uint16_t HASH_SLOTS_MAX_ITERATIONS = 50;

uint16_t Crc16(const char *buf, size_t len);
uint16_t GetSlotIdFromKey(std::string_view key);
std::string_view GetTagFromKey(std::string_view key);
