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

import (
	"fmt"
	"math/rand"
	"strings"
)

func RandPath[T any](funcs ...func() T) T {
	index := rand.Int31n(int32(len(funcs)))
	return funcs[index]()
}

func RandPathNoResult(funcs ...func()) {
	index := rand.Int31n(int32(len(funcs)))
	funcs[index]()
}

// Random signed integer in (-max, max)
func randomSignedInt(max int32) int64 {
	return rand.Int63n(int64(max)*2-1) - int64(max) + 1
}

// Random integer in [0, max)
func RandomInt(max int64) int64 {
	return rand.Int63() % int64(max)
}

type RandStringType int

const (
	Alpha RandStringType = iota
	Binary
	Compr
)

func RandString(min, max int, typ RandStringType) string {
	length := min + rand.Intn(max-min+1)

	var minVal, maxVal int
	switch typ {
	case Binary:
		minVal, maxVal = 0, 255
	case Alpha:
		minVal, maxVal = 48, 122
	case Compr:
		minVal, maxVal = 48, 52
	}

	var sb strings.Builder
	for ; length > 0; length-- {
		s := fmt.Sprintf("%c", minVal+rand.Intn(maxVal-minVal+1))
		sb.WriteString(s)
	}
	return sb.String()
}

func RandomValue() string {
	return RandPath(
		// Small enough to likely collide
		func() string {
			return fmt.Sprintf("%d", randomSignedInt(1000))
		},
		// 32 bit compressible signed/unsigned
		func() string {
			return RandPath(
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(2000000000))
				},
				func() string {
					return fmt.Sprintf("%d", rand.Int63n(4000000000))
				},
			)
		},
		// 64 bit
		func() string {
			return fmt.Sprintf("%d", rand.Int63n(1000000000000))
		},
		// Random string
		func() string {
			return RandPath(
				func() string {
					return RandString(0, 256, Alpha)
				},
				func() string {
					return RandString(0, 256, Compr)
				},
				func() string {
					return RandString(0, 256, Binary)
				},
			)
		},
	)
}
