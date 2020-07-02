#pragma once

#include <string>
#include <utility>
#include <vector>
#include "storage.h"

class CompactionChecker {
 public:
  explicit CompactionChecker(Engine::Storage *storage):storage_(storage) {}
  ~CompactionChecker() {}
  void PickCompactionFiles(const std::string &cf_name);
  void CompactPubsubAndSlotFiles();
 private:
  Engine::Storage *storage_ = nullptr;
};
