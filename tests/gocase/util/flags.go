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

import "flag"

var binPath = flag.String("binPath", "", "directory including kvrocks build files")
var workspace = flag.String("workspace", "", "directory of cases workspace")
var deleteOnExit = flag.Bool("deleteOnExit", false, "whether to delete workspace on exit")
var cliPath = flag.String("cliPath", "redis-cli", "path to redis-cli")
var tlsEnable = flag.Bool("tlsEnable", false, "enable TLS-related test cases")

func CLIPath() string {
	return *cliPath
}

func TLSEnable() bool {
	return *tlsEnable
}
