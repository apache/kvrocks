#ifndef KVROCKS_TEST_BASE_H
#define KVROCKS_TEST_BASE_H

#include <gtest/gtest.h>
#include "redis_db.h"
#include "redis_hash.h"

class TestBase : public testing::Test {
protected:
  explicit TestBase() {
    config_ = new Config();
    config_->db_dir = "testsdb";
    config_->backup_dir = "testsdb/backup";
    storage_ = new Engine::Storage(config_);
    Status s = storage_->Open();
    if (!s.IsOK()) {
      std::cout << "Failed to open the storage, encounter error: " << s.Msg() << std::endl;
      assert(s.IsOK());
    }
  }
  ~TestBase() override {
    rmdir("testsdb");
    delete storage_;
    delete config_;
  }

protected:
  Engine::Storage *storage_;
  Config *config_ = nullptr;
  std::string key_;
  std::vector<Slice> fields_;
  std::vector<Slice> values_;
};
#endif //KVROCKS_TEST_BASE_H
