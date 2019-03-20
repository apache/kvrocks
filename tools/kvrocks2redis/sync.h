#pragma once

#include <event2/bufferevent.h>
#include <unistd.h>
#include <fstream>

#include "../../src/status.h"
#include "../../src/storage.h"
#include "../../src/replication.h"

#include "config.h"
#include "writer.h"
#include "parser.h"

class Sync : public ReplicationThread {
 public:
  explicit Sync(Engine::Storage *storage, Writer *writer, Parser *parser, Kvrocks2redis::Config *config);
  ~Sync() {
    if (next_seq_fd_) close(next_seq_fd_);
  }
  void Start();
  void Stop();
  bool IsStopped() { return stop_flag_; }

 private:
  bool stop_flag_ = false;
  Engine::Storage *storage_ = nullptr;
  Writer *writer_ = nullptr;
  Parser *parser_ = nullptr;
  Kvrocks2redis::Config *config_ = nullptr;
  ReplState sync_state_;
  int next_seq_fd_;
  rocksdb::SequenceNumber next_seq_ = static_cast<rocksdb::SequenceNumber>(0);

  using CBState = CallbacksStateMachine::State;
  CallbacksStateMachine psync_steps_;

  // Internal states managed by IncrementBatchLoop procedure
  enum IncrementBatchLoopState {
    Incr_batch_size,
    Incr_batch_data,
  } incr_state_ = Incr_batch_size;

  size_t incr_bulk_len_ = 0;

  static CBState tryPSyncWriteCB(bufferevent *bev, void *ctx);
  static CBState tryPSyncReadCB(bufferevent *bev, void *ctx);
  static CBState incrementBatchLoopCB(bufferevent *bev, void *ctx);

  static void EventTimerCB(int, int16_t, void *ctx);

  void parseKVFromLocalStorage();

  Status updateNextSeq(rocksdb::SequenceNumber seq);
  Status readNextSeqFromFile(rocksdb::SequenceNumber *seq);
  Status writeNextSeqToFile(rocksdb::SequenceNumber seq);
};
