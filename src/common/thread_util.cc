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

#include "thread_util.h"

#include <fmt/std.h>
#include <pthread.h>

#ifdef __APPLE__
#include <mach/mach.h>
#include <mach/thread_act.h>
#endif

namespace util {

void ThreadSetName(const char *name) {
#ifdef __APPLE__
  pthread_setname_np(name);
#else
  pthread_setname_np(pthread_self(), name);
#endif
}

#ifdef __APPLE__
double ThreadGetCPUTime(std::thread::native_handle_type thread_id) {
  if (!thread_id) {
    return 0.0;
  }

  mach_port_t mach_thread = pthread_mach_thread_np(thread_id);

  thread_basic_info_data_t info;
  mach_msg_type_number_t count = THREAD_BASIC_INFO_COUNT;

  if (thread_info(mach_thread, THREAD_BASIC_INFO, (thread_info_t)&info, &count) != KERN_SUCCESS) {
    return 0.0;
  }

  return (static_cast<double>(info.user_time.seconds) + static_cast<double>(info.user_time.microseconds) / 1e6) +
         (static_cast<double>(info.system_time.seconds) + static_cast<double>(info.system_time.microseconds) / 1e6);
}
#else
double ThreadGetCPUTime(std::thread::native_handle_type thread_id) {
  if (!thread_id) {
    return 0.0;
  }

  clockid_t clock_id = 0;
  if (pthread_getcpuclockid(thread_id, &clock_id) != 0) {
    return 0.0;
  }

  timespec ts;
  if (clock_gettime(clock_id, &ts) != 0) {
    return 0.0;
  }
  return static_cast<double>(ts.tv_sec) + static_cast<double>(ts.tv_nsec) / 1e9;
}
#endif

template <void (std::thread::*F)(), typename... Args>
Status ThreadOperationImpl(std::thread &t, const char *op, Args &&...args) {
  try {
    (t.*F)(std::forward<Args>(args)...);
  } catch (const std::system_error &e) {
    return {Status::NotOK, fmt::format("thread #{} cannot be `{}`ed: {}", t.get_id(), op, e.what())};
  }

  return Status::OK();
}

Status ThreadJoin(std::thread &t) { return ThreadOperationImpl<&std::thread::join>(t, "join"); }

Status ThreadDetach(std::thread &t) { return ThreadOperationImpl<&std::thread::detach>(t, "detach"); }

}  // namespace util
