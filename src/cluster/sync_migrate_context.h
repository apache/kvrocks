#include "server/server.h"

class SyncMigrateContext {
 public:
  SyncMigrateContext(Server *svr, redis::Connection *conn, int timeout) : svr_(svr), conn_(conn), timeout_(timeout){};
  SyncMigrateContext(SyncMigrateContext &&) = delete;
  SyncMigrateContext(const SyncMigrateContext &) = delete;

  SyncMigrateContext &operator=(SyncMigrateContext &&) = delete;
  SyncMigrateContext &operator=(const SyncMigrateContext &) = delete;

  ~SyncMigrateContext();

  void StartBlock();
  void Wakeup(const Status &migrate_result);
  static void WriteCB(bufferevent *bev, void *ctx);
  static void EventCB(bufferevent *bev, int16_t events, void *ctx);
  static void TimerCB(int, int16_t events, void *ctx);

 private:
  Server *svr_;
  redis::Connection *conn_;
  int timeout_ = 0;
  event *timer_ = nullptr;

  Status migrate_result_;
};
