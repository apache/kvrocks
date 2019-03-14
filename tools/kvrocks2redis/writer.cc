#include "writer.h"
#include <cstring>

Writer::~Writer() {
  for (auto iter = aof_file_streams_.begin(); iter != aof_file_streams_.end(); iter++) {
    iter->second.close();
  }
}

Status Writer::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  auto s = GetAofFileStream(ns);
  if (!s.IsOK()) {
    return Status(Status::NotOK, s.Msg());
  }
  for (size_t i = 0; i < aofs.size(); i++) {
    aof_file_streams_[ns] << aofs[i];
    aof_file_streams_[ns].flush();
  }

  return Status::OK();
}

Status Writer::FlushAll(const std::string &ns) {
  auto s = GetAofFileStream(ns, true);
  if (!s.IsOK()) {
    return Status(Status::NotOK, s.Msg());
  }

  return Status::OK();
}

Status Writer::GetAofFileStream(const std::string &ns, bool truncate) {
  auto aof_fs = aof_file_streams_.find(ns);
  if (aof_fs == aof_file_streams_.end()) {
    return OpenAofFile(ns, truncate);
  } else if (truncate) {
    aof_file_streams_[ns].close();
    return OpenAofFile(ns, truncate);
  }
  return Status::OK();
}

Status Writer::OpenAofFile(const std::string &ns, bool truncate) {
  std::ios_base::openmode openmode = std::ofstream::out | std::ofstream::app;
  if (truncate) {
    openmode = std::ofstream::out | std::ofstream::trunc;
  }
  aof_file_streams_[ns].open(GetAofFilePath(ns), openmode);
  if (!aof_file_streams_[ns].is_open()) {
    return Status(Status::NotOK, std::string("Failed to open aof file :") + strerror(errno));
  }

  return Status::OK();
}

std::string Writer::GetAofFilePath(const std::string &ns) {
  return config_->dir + "/" + ns + "_" + config_->aof_file_name;
}

