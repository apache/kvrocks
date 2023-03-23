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

#ifdef ENABLE_OPENSSL

#include <openssl/ssl.h>

#include <memory>

#include "config/config.h"
#include "event_util.h"

void InitSSL();

using StaticSSLCTXFree = StaticFunction<decltype(SSL_CTX_free), SSL_CTX_free>;

struct UniqueSSLContext : std::unique_ptr<SSL_CTX, StaticSSLCTXFree> {
  using base_type = std::unique_ptr<SSL_CTX, StaticSSLCTXFree>;

  using base_type::base_type;

  explicit UniqueSSLContext(const SSL_METHOD *method = SSLv23_method()) : base_type(SSL_CTX_new(method)) {}
};

UniqueSSLContext CreateSSLContext(const Config *config, const SSL_METHOD *method = SSLv23_method());

using StaticSSLFree = StaticFunction<decltype(SSL_free), SSL_free>;

struct UniqueSSL : std::unique_ptr<SSL, StaticSSLFree> {
  using base_type = std::unique_ptr<SSL, StaticSSLFree>;

  using base_type::base_type;

  UniqueSSL() = default;

  explicit UniqueSSL(SSL_CTX *ctx) : base_type(SSL_new(ctx)) {}
};

struct SSLErrors {
  friend std::ostream &operator<<(std::ostream &, SSLErrors);
};

struct SSLError {
  explicit SSLError(unsigned long err) : err(err) {}  // NOLINT

  friend std::ostream &operator<<(std::ostream &, SSLError);

  unsigned long err;  // NOLINT
};

#endif
