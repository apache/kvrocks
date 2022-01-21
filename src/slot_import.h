#pragma once

#include <string>
#include <vector>
#include <mutex>
#include <glog/logging.h>

#include "redis_db.h"
#include "config.h"
#include "server.h"

enum ImportStatus{
  kImportStart,
  kImportSuccess,
  kImportFailed,
  kImportNone,
};

class SlotImport : public Redis::Database {
 public:
  explicit SlotImport(Server *svr);
  ~SlotImport() {}
  bool Start(int fd, int slot);
  bool Success(int slot);
  bool Fail(int slot);
  void StopForLinkError(int fd);
  int GetSlot();
  int GetStatus();
  Status GetImportInfo(std::vector<std::string> *info, int slot);

 private:
  Server *svr_ = nullptr;
  std::mutex mutex_;
  int import_slot_;
  int import_status_;
  int import_fd_;
};
