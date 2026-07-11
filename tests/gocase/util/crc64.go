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
 */

package util

import "hash/crc64"

// redisCRC64Table is the reflected CRC-64 (Jones) table Redis uses for the
// DUMP/RESTORE payload footer.
var redisCRC64Table = crc64.MakeTable(0x95ac9329ac4bc9b5)

// RedisCRC64 computes the CRC-64 checksum that Redis appends to a DUMP/RESTORE
// payload, matching the checksum kvrocks verifies when loading the payload.
func RedisCRC64(data []byte) uint64 {
	return ^crc64.Update(^uint64(0), redisCRC64Table, data)
}
