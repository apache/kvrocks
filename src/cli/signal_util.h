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

#include <execinfo.h>
#include <glog/logging.h>
#include <signal.h>

#include <cstddef>
#include <iomanip>

#include "version_util.h"

namespace google {
bool Symbolize(void *pc, char *out, size_t out_size);
}  // namespace google

extern "C" inline void SegvHandler(int sig, siginfo_t *info, void *secret) {
  void *trace[100];

  LOG(ERROR) << "======= Ooops! kvrocks " << PrintVersion << " got signal: " << strsignal(sig) << " (" << sig
             << ") =======";
  int trace_size = backtrace(trace, sizeof(trace) / sizeof(void *));
  char **messages = backtrace_symbols(trace, trace_size);

  size_t max_msg_len = 0;
  for (int i = 1; i < trace_size; ++i) {
    auto msg_len = strlen(messages[i]);
    if (msg_len > max_msg_len) {
      max_msg_len = msg_len;
    }
  }

  for (int i = 1; i < trace_size; ++i) {
    char func_info[1024] = {};
    if (google::Symbolize(trace[i], func_info, sizeof(func_info) - 1)) {
      LOG(ERROR) << std::left << std::setw(static_cast<int>(max_msg_len)) << messages[i] << "  " << func_info;
    } else {
      LOG(ERROR) << messages[i];
    }
  }

  struct sigaction act;
  /* Make sure we exit with the right signal at the end. So for instance
   * the core will be dumped if enabled.
   */
  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used
   */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = SIG_DFL;
  sigaction(sig, &act, nullptr);
  kill(getpid(), sig);
}

inline void SetupSigSegvAction(void (*handler)(int)) {
  struct sigaction act;

  sigemptyset(&act.sa_mask);
  /* When the SA_SIGINFO flag is set in sa_flags then sa_sigaction
   * is used. Otherwise, sa_handler is used */
  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND | SA_SIGINFO;
  act.sa_sigaction = SegvHandler;
  sigaction(SIGSEGV, &act, nullptr);
  sigaction(SIGBUS, &act, nullptr);
  sigaction(SIGFPE, &act, nullptr);
  sigaction(SIGILL, &act, nullptr);
  sigaction(SIGABRT, &act, nullptr);

  act.sa_flags = SA_NODEFER | SA_ONSTACK | SA_RESETHAND;
  act.sa_handler = handler;
  sigaction(SIGTERM, &act, nullptr);
  sigaction(SIGINT, &act, nullptr);
}
