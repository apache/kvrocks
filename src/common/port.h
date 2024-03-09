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

#if defined(__sparc__) || defined(__arm__)
#define USE_ALIGNED_ACCESS
#endif

#ifndef CACHE_LINE_SIZE
// To test behavior with non-native cache line size, e.g. for
// Bloom filters, set TEST_CACHE_LINE_SIZE to the desired test size.
// This disables ALIGN_AS to keep it from failing compilation.
#ifdef TEST_CACHE_LINE_SIZE
#define CACHE_LINE_SIZE TEST_CACHE_LINE_SIZE
#define ALIGN_AS(n) /*empty*/
#else
#if defined(__s390__)
#if defined(__GNUC__) && __GNUC__ < 7
#define CACHE_LINE_SIZE 64U
#else
#define CACHE_LINE_SIZE 256U
#endif
#elif defined(__powerpc__) || defined(__aarch64__)
#define CACHE_LINE_SIZE 128U
#else
#define CACHE_LINE_SIZE 64U
#endif
#define ALIGN_AS(n) alignas(n)
#endif
#endif
