#pragma once

#include <event2/bufferevent.h>
#include <unistd.h>
#include <fstream>

#include "../../src/status.h"
#include "../../src/storage.h"
#include "../../src/replication.h"
#include "../../src/server.h"

#include "config.h"
#include "writer.h"
#include "parser.h"

class Sync {
 public:
  explicit Sync(Engine::Storage *storage, Writer *writer, Parser *parser, Kvrocks2redis::Config *config);
  ~Sync();
  void Start();
  void Stop();
  bool IsStopped() { return stop_flag_; }

 private:
  int sock_fd_;
  bool stop_flag_ = false;
  Engine::Storage *storage_ = nullptr;
  Writer *writer_ = nullptr;
  Parser *parser_ = nullptr;
  Kvrocks2redis::Config *config_ = nullptr;
  int next_seq_fd_;
  rocksdb::SequenceNumber next_seq_ = static_cast<rocksdb::SequenceNumber>(0);

  // Internal states managed by IncrementBatchLoop procedure
  enum IncrementBatchLoopState {
    Incr_batch_size,
    Incr_batch_data,
  } incr_state_ = Incr_batch_size;

  size_t incr_bulk_len_ = 0;

  Status auth();
  Status tryPSync();
  Status incrementBatchLoop();

  void parseKVFromLocalStorage();

  Status updateNextSeq(rocksdb::SequenceNumber seq);
  Status readNextSeqFromFile(rocksdb::SequenceNumber *seq);
  Status writeNextSeqToFile(rocksdb::SequenceNumber seq);
};
