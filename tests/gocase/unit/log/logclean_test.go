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

package log

import (
	"os"
	"testing"
	"time"

	"github.com/apache/kvrocks/tests/gocase/util"
	"github.com/stretchr/testify/require"
)

func TestLogClean(t *testing.T) {
	err := os.Mkdir("/tmp/kvrocks/logfile", os.ModePerm)
	if err != nil {
		return
	}
	srv1 := util.StartServer(t, map[string]string{
		"log-dir":            "/tmp/kvrocks/logfile",
		"log-retention-days": "0",
	})
	srv1.Close()
	files1, _ := os.ReadDir("/tmp/kvrocks/logfile")
	time.Sleep(2000 * time.Millisecond)
	srv2 := util.StartServer(t, map[string]string{
		"log-dir":            "/tmp/kvrocks/logfile",
		"log-retention-days": "0",
	})
	srv2.Close()
	files2, _ := os.ReadDir("/tmp/kvrocks/logfile")
	islogclear := false
	for _, f1 := range files1 {
		ishave := false
		for _, f2 := range files2 {
			if f1.Name() == f2.Name() {
				ishave = true
				break
			}
		}
		if !ishave {
			islogclear = true
			break
		}
	}
	require.Equal(t, true, islogclear)
	err = os.RemoveAll("/tmp/kvrocks/logfile")
	if err != nil {
		return
	}
}
