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

#include <glog/logging.h>
#include <rocksdb/listener.h>

#include "storage.h"

class EventListener : public rocksdb::EventListener {
 public:
  explicit EventListener(engine::Storage *storage) : storage_(storage) {}
  ~EventListener() override = default;
  void OnFlushBegin(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) override;
  void OnFlushCompleted(rocksdb::DB *db, const rocksdb::FlushJobInfo &fi) override;
  void OnCompactionBegin(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) override;
  void OnCompactionCompleted(rocksdb::DB *db, const rocksdb::CompactionJobInfo &ci) override;
  void OnSubcompactionBegin(const rocksdb::SubcompactionJobInfo &si) override;
  void OnSubcompactionCompleted(const rocksdb::SubcompactionJobInfo &si) override;

  void OnBackgroundError(rocksdb::BackgroundErrorReason reason, rocksdb::Status *status) override;
  void OnTableFileDeleted(const rocksdb::TableFileDeletionInfo &info) override;
  void OnStallConditionsChanged(const rocksdb::WriteStallInfo &info) override;
  void OnTableFileCreated(const rocksdb::TableFileCreationInfo &info) override;

 private:
  engine::Storage *storage_ = nullptr;
};
