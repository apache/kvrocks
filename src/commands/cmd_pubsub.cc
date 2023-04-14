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

#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "storage/redis_pubsub.h"

namespace Redis {

class CommandPublish : public Commander {
 public:
  // mark is_write as false here because slave should be able to execute publish command
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (!svr->IsSlave()) {
      // Compromise: can't replicate message to sub-replicas in a cascading-like structure.
      // Replication relies on WAL seq, increase the seq on slave will break the replication, hence the compromise
      Redis::PubSub pubsub_db(svr->storage);
      auto s = pubsub_db.Publish(args_[1], args_[2]);
      if (!s.ok()) {
        return {Status::RedisExecErr, s.ToString()};
      }
    }

    int receivers = svr->PublishMessage(args_[1], args_[2]);
    *output = Redis::Integer(receivers);
    return Status::OK();
  }
};

void SubscribeCommandReply(std::string *output, const std::string &name, const std::string &sub_name, int num) {
  output->append(Redis::MultiLen(3));
  output->append(Redis::BulkString(name));
  output->append(sub_name.empty() ? Redis::NilString() : Redis::BulkString(sub_name));
  output->append(Redis::Integer(num));
}

class CommandSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (unsigned i = 1; i < args_.size(); i++) {
      conn->SubscribeChannel(args_[i]);
      SubscribeCommandReply(output, "subscribe", args_[i], conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandUnSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->UnsubscribeAll([output](const std::string &sub_name, int num) {
        SubscribeCommandReply(output, "unsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->UnsubscribeChannel(args_[i]);
        SubscribeCommandReply(output, "unsubscribe", args_[i],
                              conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    for (size_t i = 1; i < args_.size(); i++) {
      conn->PSubscribeChannel(args_[i]);
      SubscribeCommandReply(output, "psubscribe", args_[i], conn->SubscriptionsCount() + conn->PSubscriptionsCount());
    }
    return Status::OK();
  }
};

class CommandPUnSubscribe : public Commander {
 public:
  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    if (args_.size() == 1) {
      conn->PUnsubscribeAll([output](const std::string &sub_name, int num) {
        SubscribeCommandReply(output, "punsubscribe", sub_name, num);
      });
    } else {
      for (size_t i = 1; i < args_.size(); i++) {
        conn->PUnsubscribeChannel(args_[i]);
        SubscribeCommandReply(output, "punsubscribe", args_[i],
                              conn->SubscriptionsCount() + conn->PSubscriptionsCount());
      }
    }
    return Status::OK();
  }
};

class CommandPubSub : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    subcommand_ = Util::ToLower(args[1]);
    if (subcommand_ == "numpat" && args.size() == 2) {
      return Status::OK();
    }

    if ((subcommand_ == "numsub") && args.size() >= 2) {
      if (args.size() > 2) {
        channels_ = std::vector<std::string>(args.begin() + 2, args.end());
      }
      return Status::OK();
    }

    if ((subcommand_ == "channels") && args.size() <= 3) {
      if (args.size() == 3) {
        pattern_ = args[2];
      }
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Unknown subcommand or wrong number of arguments"};
  }

  Status Execute(Server *srv, Connection *conn, std::string *output) override {
    if (subcommand_ == "numpat") {
      *output = Redis::Integer(srv->GetPubSubPatternSize());
      return Status::OK();
    }

    if (subcommand_ == "numsub") {
      std::vector<ChannelSubscribeNum> channel_subscribe_nums;
      srv->ListChannelSubscribeNum(channels_, &channel_subscribe_nums);

      output->append(Redis::MultiLen(channel_subscribe_nums.size() * 2));
      for (const auto &chan_subscribe_num : channel_subscribe_nums) {
        output->append(Redis::BulkString(chan_subscribe_num.channel));
        output->append(Redis::Integer(chan_subscribe_num.subscribe_num));
      }

      return Status::OK();
    }

    if (subcommand_ == "channels") {
      std::vector<std::string> channels;
      srv->GetChannelsByPattern(pattern_, &channels);
      *output = Redis::MultiBulkString(channels);
      return Status::OK();
    }

    return {Status::RedisInvalidCmd, "Unknown subcommand or wrong number of arguments"};
  }

 private:
  std::string pattern_;
  std::vector<std::string> channels_;
  std::string subcommand_;
};

REDIS_REGISTER_COMMANDS(
    MakeCmdAttr<CommandPublish>("publish", 3, "read-only pub-sub", 0, 0, 0),
    MakeCmdAttr<CommandSubscribe>("subscribe", -2, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandUnSubscribe>("unsubscribe", -1, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandPSubscribe>("psubscribe", -2, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandPUnSubscribe>("punsubscribe", -1, "read-only pub-sub no-multi no-script", 0, 0, 0),
    MakeCmdAttr<CommandPubSub>("pubsub", -2, "read-only pub-sub no-script", 0, 0, 0), )

}  // namespace Redis
