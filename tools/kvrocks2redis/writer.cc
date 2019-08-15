#include "writer.h"
#include <fcntl.h>
#include <unistd.h>
#include <cstring>

Writer::~Writer() {
  for (const auto &iter : aof_fds_) {
    close(iter.second);
  }
}

Status Writer::Write(const std::string &ns, const std::vector<std::string> &aofs) {
  auto s = GetAofFd(ns);
  if (!s.IsOK()) {
    return Status(Status::NotOK, s.Msg());
  }
  for (size_t i = 0; i < aofs.size(); i++) {
    write(aof_fds_[ns], aofs[i].data(), aofs[i].size());
  }

  return Status::OK();
}

Status Writer::FlushAll(const std::string &ns) {
  auto s = GetAofFd(ns, true);
  if (!s.IsOK()) {
    return Status(Status::NotOK, s.Msg());
  }

  return Status::OK();
}

Status Writer::GetAofFd(const std::string &ns, bool truncate) {
  auto aof_fd = aof_fds_.find(ns);
  if (aof_fd == aof_fds_.end()) {
    return OpenAofFile(ns, truncate);
  } else if (truncate) {
    close(aof_fds_[ns]);
    return OpenAofFile(ns, truncate);
  }
  if (aof_fds_[ns] < 0) {
    return Status(Status::NotOK, std::string("Failed to open aof file :") + strerror(errno));
  }
  return Status::OK();
}

Status Writer::OpenAofFile(const std::string &ns, bool truncate) {
  int openmode = O_RDWR | O_CREAT | O_APPEND;
  if (truncate) {
    openmode |= O_TRUNC;
  }
  aof_fds_[ns] = open(GetAofFilePath(ns).data(), openmode, 0666);
  if (aof_fds_[ns] < 0) {
    return Status(Status::NotOK, std::string("Failed to open aof file :") + strerror(errno));
  }

  return Status::OK();
}

std::string Writer::GetAofFilePath(const std::string &ns) {
  return config_->dir + "/" + ns + "_" + config_->aof_file_name;
}

