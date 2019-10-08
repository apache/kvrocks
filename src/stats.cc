#include "stats.h"

#if defined(__APPLE__)
#include <mach/task.h>
#include <mach/mach_init.h>

int64_t Stats::GetMemoryRSS() {
  task_t task = MACH_PORT_NULL;
  struct task_basic_info t_info;
  mach_msg_type_number_t t_info_count = TASK_BASIC_INFO_COUNT;
  if (task_for_pid(current_task(), getpid(), &task) != KERN_SUCCESS) return 0;
  task_info(task, TASK_BASIC_INFO, (task_info_t)&t_info, &t_info_count);
  return static_cast<int64_t>(t_info.resident_size);
}
#else
#include <fcntl.h>

#include <string>
#include <cstdio>
#include <cstring>

int64_t Stats::GetMemoryRSS() {
  int fd, count;
  char buf[4096], filename[256];
  snprintf(filename, sizeof(filename), "/proc/%d/stat", getpid());
  if ((fd = open(filename, O_RDONLY)) == -1) return 0;
  if (read(fd, buf, sizeof(buf)) <= 0) {
    close(fd);
    return 0;
  }
  close(fd);

  char *start = buf;
  count = 23; /* RSS is the 24th field in /proc/<pid>/stat */
  while (start && count--) {
    start = strchr(start, ' ');
    if (start) start++;
  }
  if (!start) return 0;
  char *stop = strchr(start, ' ');
  if (!stop) return 0;
  *stop = '\0';
  int rss = std::atoi(start);
  return static_cast<int64_t>(rss * sysconf(_SC_PAGESIZE));
}
#endif

void Stats::IncrCalls(const std::string &command_name) {
  total_calls.fetch_add(1, std::memory_order_relaxed);
  commands_stats[command_name].calls.fetch_add(1, std::memory_order_relaxed);
}

void Stats::IncrLatency(uint64_t latency, const std::string &command_name) {
  commands_stats[command_name].latency.fetch_add(latency, std::memory_order_relaxed);
}
