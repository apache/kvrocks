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

namespace Redis {

RegisterToCommandTable::RegisterToCommandTable(std::initializer_list<CommandAttributes> list) {
  for (const auto &attr : list) {
    command_details::redis_command_table.emplace_back(attr);
    command_details::original_commands[attr.name_] = &command_details::redis_command_table.back();
    command_details::commands[attr.name_] = &command_details::redis_command_table.back();
  }
}

int GetCommandNum() { return (int)command_details::redis_command_table.size(); }

const CommandMap *GetOriginalCommands() { return &command_details::original_commands; }

CommandMap *GetCommands() { return &command_details::commands; }

void ResetCommands() { command_details::commands = command_details::original_commands; }

std::string GetCommandInfo(const CommandAttributes *command_attributes) {
  std::string command, command_flags;
  command.append(Redis::MultiLen(6));
  command.append(Redis::BulkString(command_attributes->name_));
  command.append(Redis::Integer(command_attributes->arity_));
  command_flags.append(Redis::MultiLen(1));
  command_flags.append(Redis::BulkString(command_attributes->is_write() ? "write" : "readonly"));
  command.append(command_flags);
  command.append(Redis::Integer(command_attributes->key_range_.first_key_));
  command.append(Redis::Integer(command_attributes->key_range_.last_key_));
  command.append(Redis::Integer(command_attributes->key_range_.key_step_));
  return command;
}

void GetAllCommandsInfo(std::string *info) {
  info->append(Redis::MultiLen(command_details::original_commands.size()));
  for (const auto &iter : command_details::original_commands) {
    auto command_attribute = iter.second;
    auto command_info = GetCommandInfo(command_attribute);
    info->append(command_info);
  }
}

void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names) {
  info->append(Redis::MultiLen(cmd_names.size()));
  for (const auto &cmd_name : cmd_names) {
    auto cmd_iter = command_details::original_commands.find(Util::ToLower(cmd_name));
    if (cmd_iter == command_details::original_commands.end()) {
      info->append(Redis::NilString());
    } else {
      auto command_attribute = cmd_iter->second;
      auto command_info = GetCommandInfo(command_attribute);
      info->append(command_info);
    }
  }
}

Status GetKeysFromCommand(const std::string &cmd_name, int argc, std::vector<int> *keys_indexes) {
  auto cmd_iter = command_details::original_commands.find(Util::ToLower(cmd_name));
  if (cmd_iter == command_details::original_commands.end()) {
    return {Status::RedisUnknownCmd, "Invalid command specified"};
  }

  auto command_attribute = cmd_iter->second;
  if (command_attribute->key_range_.first_key_ == 0) {
    return {Status::NotOK, "The command has no key arguments"};
  }

  if (command_attribute->key_range_.first_key_ < 0) {
    return {Status::NotOK, "The command has dynamic positions of key arguments"};
  }

  if ((command_attribute->arity_ > 0 && command_attribute->arity_ != argc) || argc < -command_attribute->arity_) {
    return {Status::NotOK, "Invalid number of arguments specified for command"};
  }

  auto last = command_attribute->key_range_.last_key_;
  if (last < 0) last = argc + last;

  for (int j = command_attribute->key_range_.first_key_; j <= last; j += command_attribute->key_range_.key_step_) {
    keys_indexes->emplace_back(j);
  }

  return Status::OK();
}

bool IsCommandExists(const std::string &name) {
  return command_details::original_commands.find(Util::ToLower(name)) != command_details::original_commands.end();
}

}  // namespace Redis
