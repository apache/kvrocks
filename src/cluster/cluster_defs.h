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

#include "cluster/redis_slot.h"

enum {
  kClusterMaster = 1,
  kClusterSlave = 2,
  kClusterNodeIdLen = 40,
  kClusterPortIncr = 10000,
  kClusterSlots = HASH_SLOTS_SIZE,
};

inline constexpr const char *errInvalidNodeID = "Invalid cluster node id";
inline constexpr const char *errInvalidSlotID = "Invalid slot id";
inline constexpr const char *errSlotOutOfRange = "Slot is out of range";
inline constexpr const char *errInvalidClusterVersion = "Invalid cluster version";
inline constexpr const char *errSlotOverlapped = "Slot distribution is overlapped";
inline constexpr const char *errNoMasterNode = "The node isn't a master";
inline constexpr const char *errClusterNoInitialized = "The cluster is not initialized";
inline constexpr const char *errInvalidClusterNodeInfo = "Invalid cluster nodes info";
inline constexpr const char *errInvalidImportState = "Invalid import state";

using SlotRange = std::pair<int, int>;
