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
    CommandTable::redis_command_table.emplace_back(attr);
    CommandTable::original_commands[attr.name] = &CommandTable::redis_command_table.back();
    CommandTable::commands[attr.name] = &CommandTable::redis_command_table.back();
  }
}

size_t CommandTable::Size() { return redis_command_table.size(); }

const CommandMap *CommandTable::GetOriginal() { return &original_commands; }

CommandMap *CommandTable::Get() { return &commands; }

void CommandTable::Reset() { commands = original_commands; }

std::string CommandTable::GetCommandInfo(const CommandAttributes *command_attributes) {
  std::string command, command_flags;
  command.append(redis::MultiLen(6));
  command.append(redis::BulkString(command_attributes->name));
  command.append(redis::Integer(command_attributes->arity));
  command_flags.append(redis::MultiLen(1));
  command_flags.append(redis::BulkString(command_attributes->flags & kCmdWrite ? "write" : "readonly"));
  command.append(command_flags);
  command.append(redis::Integer(command_attributes->key_range.first_key));
  command.append(redis::Integer(command_attributes->key_range.last_key));
  command.append(redis::Integer(command_attributes->key_range.key_step));
  return command;
}

void CommandTable::GetAllCommandsInfo(std::string *info) {
  info->append(redis::MultiLen(original_commands.size()));
  for (const auto &iter : original_commands) {
    auto command_attribute = iter.second;
    auto command_info = GetCommandInfo(command_attribute);
    info->append(command_info);
  }
}

void CommandTable::GetCommandsInfo(std::string *info, const std::vector<std::string> &cmd_names) {
  info->append(redis::MultiLen(cmd_names.size()));
  for (const auto &cmd_name : cmd_names) {
    auto cmd_iter = original_commands.find(util::ToLower(cmd_name));
    if (cmd_iter == original_commands.end()) {
      info->append(NilString(RESP::v2));
    } else {
      auto command_attribute = cmd_iter->second;
      auto command_info = GetCommandInfo(command_attribute);
      info->append(command_info);
    }
  }
}

void DumpKeyRange(std::vector<int> &keys_index, int argc, const CommandKeyRange &range) {
  auto last = range.last_key;
  if (last < 0) last = argc + last;

  for (int i = range.first_key; i <= last; i += range.key_step) {
    keys_index.emplace_back(i);
  }
}

Status CommandTable::GetKeysFromCommand(const CommandAttributes *attributes, const std::vector<std::string> &cmd_tokens,
                                        std::vector<int> *keys_index) {
  if (attributes->key_range.first_key == 0) {
    return {Status::NotOK, "The command has no key arguments"};
  }

  int argc = static_cast<int>(cmd_tokens.size());

  if ((attributes->arity > 0 && attributes->arity != argc) || argc < -attributes->arity) {
    return {Status::NotOK, "Invalid number of arguments specified for command"};
  }

  if (attributes->key_range.first_key > 0) {
    DumpKeyRange(*keys_index, argc, attributes->key_range);
  } else if (attributes->key_range.first_key == -1) {
    DumpKeyRange(*keys_index, argc, attributes->key_range_gen(cmd_tokens));
  } else if (attributes->key_range.first_key == -2) {
    for (const auto &range : attributes->key_range_vec_gen(cmd_tokens)) {
      DumpKeyRange(*keys_index, argc, range);
    }
  } else {
    return {Status::NotOK, "The key range specification is invalid"};
  }

  return Status::OK();
}

bool CommandTable::IsExists(const std::string &name) {
  return original_commands.find(util::ToLower(name)) != original_commands.end();
}

Status CommandTable::ParseSlotRanges(const std::string &slots_str, std::vector<SlotRange> &slots) {
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
