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

#include "cluster/cluster_defs.h"

namespace redis {

RegisterToCommandTable::RegisterToCommandTable(std::initializer_list<CommandAttributes> list) {
  for (const auto &attr : list) {
    command_details::redis_command_table.emplace_back(attr);
    command_details::original_commands[attr.name] = &command_details::redis_command_table.back();
    command_details::commands[attr.name] = &command_details::redis_command_table.back();
  }
}

int GetCommandNum() { return (int)command_details::redis_command_table.size(); }

const CommandMap *GetOriginalCommands() { return &command_details::original_commands; }

CommandMap *GetCommands() { return &command_details::commands; }

void ResetCommands() { command_details::commands = command_details::original_commands; }

std::string GetCommandInfo(const CommandAttributes *command_attributes) {
  std::string command, command_flags;
  command.append(redis::MultiLen(6));
  command.append(redis::BulkString(command_attributes->name));
  command.append(redis::Integer(command_attributes->arity));
  command_flags.append(redis::MultiLen(1));
  command_flags.append(redis::BulkString(command_attributes->IsWrite() ? "write" : "readonly"));
  command.append(command_flags);
  command.append(redis::Integer(command_attributes->key_range.first_key));
  command.append(redis::Integer(command_attributes->key_range.last_key));
  command.append(redis::Integer(command_attributes->key_range.key_step));
  return command;
}

void GetAllCommandsInfo(std::string *info) {
  info->append(redis::MultiLen(command_details::original_commands.size()));
  for (const auto &iter : command_details::original_commands) {
    auto command_attribute = iter.second;
    auto command_info = GetCommandInfo(command_attribute);
    info->append(command_info);
  }
}

void GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names) {
  info->append(redis::MultiLen(cmd_names.size()));
  for (const auto &cmd_name : cmd_names) {
    auto cmd_iter = command_details::original_commands.find(util::ToLower(cmd_name));
    if (cmd_iter == command_details::original_commands.end()) {
      info->append(redis::NilString());
    } else {
      auto command_attribute = cmd_iter->second;
      auto command_info = GetCommandInfo(command_attribute);
      info->append(command_info);
    }
  }
}

Status GetKeysFromCommand(const std::string &cmd_name, int argc, std::vector<int> *keys_indexes) {
  auto cmd_iter = command_details::original_commands.find(util::ToLower(cmd_name));
  if (cmd_iter == command_details::original_commands.end()) {
    return {Status::RedisUnknownCmd, "Invalid command specified"};
  }

  auto command_attribute = cmd_iter->second;
  if (command_attribute->key_range.first_key == 0) {
    return {Status::NotOK, "The command has no key arguments"};
  }

  if (command_attribute->key_range.first_key < 0) {
    return {Status::NotOK, "The command has dynamic positions of key arguments"};
  }

  if ((command_attribute->arity > 0 && command_attribute->arity != argc) || argc < -command_attribute->arity) {
    return {Status::NotOK, "Invalid number of arguments specified for command"};
  }

  auto last = command_attribute->key_range.last_key;
  if (last < 0) last = argc + last;

  for (int j = command_attribute->key_range.first_key; j <= last; j += command_attribute->key_range.key_step) {
    keys_indexes->emplace_back(j);
  }

  return Status::OK();
}

bool IsCommandExists(const std::string &name) {
  return command_details::original_commands.find(util::ToLower(name)) != command_details::original_commands.end();
}

Status CommanderHelper::ParseSlotRanges(const std::string &slots_str, std::vector<SlotRange> &slots) {
  if (slots_str.empty()) {
    return {Status::NotOK, "No slots to parse."};
  }

  std::vector<std::string> slot_ranges = util::Split(slots_str, " ");
  if (slot_ranges.empty()) {
    return {Status::NotOK,
            fmt::format("Invalid slots: `{}`. No slots to parse. Please use spaces to separate slots.", slots_str)};
  }

  auto valid_range = NumericRange<int>{0, kClusterSlots - 1};
  // Parse all slots (include slot ranges)
  for (auto &slot_range : slot_ranges) {
    if (slot_range.find('-') == std::string::npos) {
      auto parse_result = ParseInt<int>(slot_range, valid_range, 10);
      if (!parse_result) {
        return std::move(parse_result).Prefixed(errInvalidSlotID);
      }
      slots.emplace_back(*parse_result, *parse_result);
      continue;
    }

    // parse slot range: "int1-int2" (satisfy: int1 <= int2 )
    if (slot_range.front() == '-' || slot_range.back() == '-') {
      return {Status::NotOK,
              fmt::format("Invalid slot range: `{}`. The character '-' can't appear in the first or last position.",
                          slot_range)};
    }
    std::vector<std::string> fields = util::Split(slot_range, "-");
    if (fields.size() != 2) {
      return {Status::NotOK,
              fmt::format("Invalid slot range: `{}`. The slot range should be of the form `int1-int2`.", slot_range)};
    }
    auto parse_start = ParseInt<int>(fields[0], valid_range, 10);
    auto parse_end = ParseInt<int>(fields[1], valid_range, 10);
    if (!parse_start || !parse_end || *parse_start > *parse_end) {
      return {Status::NotOK,
              fmt::format(
                  "Invalid slot range: `{}`. The slot range `int1-int2` needs to satisfy the condition (int1 <= int2).",
                  slot_range)};
    }
    slots.emplace_back(*parse_start, *parse_end);
  }

  return Status::OK();
}

}  // namespace redis
