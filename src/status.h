#pragma once

#include <string>

class Status {
 public:
  enum Code {
    cOK,

    // DB
    DBOpenErr,
    DBBackupErr,
    DBGetWALErr,

    // Redis
    RedisUnknownCmd,
    RedisInvalidCmd,
    RedisParseErr,
    RedisExecErr,
    RedisReplicationConflict,

    // Network
    NetSendErr,
  };
  Status() : Status(cOK, "ok") {}
  Status(Code code, std::string msg = "") : code_(code), msg_(std::move(msg)) {}

  bool IsOK() { return code_ == cOK; }

  std::string msg() { return msg_; }

  static Status OK() { return Status(cOK, "ok"); }

 private:
  Code code_;
  std::string msg_;
};
