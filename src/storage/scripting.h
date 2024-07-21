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

#pragma once

#include <string>
#include <vector>

#include "lua.hpp"
#include "server/redis_connection.h"
#include "status.h"

inline constexpr const char REDIS_LUA_FUNC_SHA_PREFIX[] = "f_";
inline constexpr const char REDIS_LUA_REGISTER_FUNC_PREFIX[] = "__redis_registered_";
inline constexpr const char REDIS_LUA_SERVER_PTR[] = "__server_ptr";
inline constexpr const char REDIS_FUNCTION_LIBNAME[] = "REDIS_FUNCTION_LIBNAME";
inline constexpr const char REDIS_FUNCTION_NEEDSTORE[] = "REDIS_FUNCTION_NEEDSTORE";
inline constexpr const char REDIS_FUNCTION_LIBRARIES[] = "REDIS_FUNCTION_LIBRARIES";

namespace lua {

lua_State *CreateState(Server *srv);
void DestroyState(lua_State *lua);
Server *GetServer(lua_State *lua);

void LoadFuncs(lua_State *lua);
void LoadLibraries(lua_State *lua);
void RemoveUnsupportedFunctions(lua_State *lua);
void EnableGlobalsProtection(lua_State *lua);

int RedisCallCommand(lua_State *lua);
int RedisPCallCommand(lua_State *lua);
int RedisGenericCommand(lua_State *lua, int raise_error);
int RedisSha1hexCommand(lua_State *lua);
int RedisStatusReplyCommand(lua_State *lua);
int RedisErrorReplyCommand(lua_State *lua);
int RedisLogCommand(lua_State *lua);
int RedisRegisterFunction(lua_State *lua);
int RedisSetResp(lua_State *lua);

Status CreateFunction(Server *srv, const std::string &body, std::string *sha, lua_State *lua, bool need_to_store);

Status EvalGenericCommand(redis::Connection *conn, const std::string &body_or_sha, const std::vector<std::string> &keys,
                          const std::vector<std::string> &argv, bool evalsha, std::string *output,
                          bool read_only = false);

bool ScriptExists(lua_State *lua, const std::string &sha);

Status FunctionLoad(redis::Connection *conn, const std::string &script, bool need_to_store, bool replace,
                    std::string *lib_name, bool read_only = false);
Status FunctionCall(redis::Connection *conn, const std::string &name, const std::vector<std::string> &keys,
                    const std::vector<std::string> &argv, std::string *output, bool read_only = false);
Status FunctionList(Server *srv, const redis::Connection *conn, const std::string &libname, bool with_code,
                    std::string *output);
Status FunctionListFunc(Server *srv, const redis::Connection *conn, const std::string &funcname, std::string *output);
Status FunctionListLib(Server *srv, const redis::Connection *conn, const std::string &libname, std::string *output);
Status FunctionDelete(Server *srv, const std::string &name);
bool FunctionIsLibExist(redis::Connection *conn, const std::string &libname, bool need_check_storage = true,
                        bool read_only = false);

const char *RedisProtocolToLuaType(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeInt(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBulk(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeStatus(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeError(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeAggregate(lua_State *lua, const char *reply, int atype);
const char *RedisProtocolToLuaTypeNull(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBool(lua_State *lua, const char *reply, int tf);
const char *RedisProtocolToLuaTypeDouble(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeBigNumber(lua_State *lua, const char *reply);
const char *RedisProtocolToLuaTypeVerbatimString(lua_State *lua, const char *reply);

std::string ReplyToRedisReply(redis::Connection *conn, lua_State *lua);

void PushError(lua_State *lua, const char *err);
[[noreturn]] int RaiseError(lua_State *lua);

void SortArray(lua_State *lua);
void SetGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems);
void PushArray(lua_State *lua, const std::vector<std::string> &elems);

void SHA1Hex(char *digest, const char *script, size_t len);

int RedisMathRandom(lua_State *l);
int RedisMathRandomSeed(lua_State *l);

// TODO 注释，默认值，uint=>int
enum ScriptFlags : uint64_t {
  kScriptNoWrites = 1ULL << 0,            // "no-writes" flag
  kScriptAllowOom = 1ULL << 1,            // "allow-oom" flag
  kScriptAllowStale = 1ULL << 2,          // "allow-stale" flag
  kScriptNoCluster = 1ULL << 3,           // "no-cluster" flag
  kScriptAllowCrossSlotKeys = 1ULL << 4,  // "allow-cross-slot-keys" flag
};

class ShebangParser {
 public:
  ShebangParser(const std::string &shebang) : shebang_(shebang) {}

  [[nodiscard]] Status Parse() {
    std::cout << "Start Parse\n";
    static constexpr const char *shebang_prefix = "#!lua";
    static constexpr const char *shebang_libname_prefix = "name=";
    static constexpr const char *shebang_flags_prefix = "flags=";

    if (!util::HasPrefix(shebang_, shebang_prefix)) {
      return {Status::NotOK, "Expect shebang prefix \"#!lua\" at the beginning of the first line"};
    }
    auto shebang_content = shebang_.substr(strlen(shebang_prefix));
    for (const auto &shebang_split : util::Split(shebang_content, " ")) {
      std::cout << shebang_split << std::endl;
      if (util::HasPrefix(shebang_split, shebang_libname_prefix)) {
        if (!libname_.empty()) {
          // TODO 已经有了
          return {Status::NotOK, "Expect a valid library name in the Shebang statement"};
        }
        libname_ = shebang_split.substr(strlen(shebang_libname_prefix));
        if (libname_.empty() ||
            std::any_of(libname_.begin(), libname_.end(), [](char v) { return !std::isalnum(v) && v != '_'; })) {
          return {Status::NotOK, "Expect a valid library name in the Shebang statement"};
        }
      } else if (util::HasPrefix(shebang_split, shebang_flags_prefix)) {
        auto flags = shebang_split.substr(strlen(shebang_flags_prefix));
        for (const auto &flag : util::Split(flags, ",")) {
          if (flag == "no-writes") {
            flags_ |= kScriptNoWrites;
          } else if (flag == "allow-oom") {
            return {Status::NotSupported, "allow-oom is not supported yet"};
          } else if (flag == "allow-stale") {
            return {Status::NotSupported, "allow-stale is not supported yet"};
          } else if (flag == "no-cluster") {
            flags_ |= kScriptNoCluster;
          } else if (flag == "allow-cross-slot-keys") {
            flags_ |= kScriptAllowCrossSlotKeys;
          } else {
            return {Status::NotOK, "Unexpected flag in script shebang: " + flag};
          }
        }
      } else {
        return {Status::NotOK, "Expect a valid Shebang statement"};
      }
    }
    return Status::OK();
  }

  [[nodiscard]] uint64_t GetFlags() const { return flags_; }
  [[nodiscard]] std::string GetLibName() const { return libname_; }

 private:
  uint64_t flags_ = 0;
  std::string libname_;
  std::string shebang_;
};

inline constexpr const char *REGISTRY_SCRIPT_RUN_CTX_NAME = "SCRIPT_RUN_CTX";
// TODO 注释
struct ScriptRunCtx {
  uint64_t flags = 0;
  int current_slot = -1;
};

static void stackDump(lua_State *L) {
  int top = lua_gettop(L);
  for (auto i = top; i >= 1; i--) { /* repeat for each level */
    int t = lua_type(L, i);
    printf("%d: ", i);
    switch (t) {
      case LUA_TSTRING: /* strings */
        printf("`%s'", lua_tostring(L, i));
        break;

      case LUA_TBOOLEAN: /* booleans */
        printf(lua_toboolean(L, i) ? "true" : "false");
        break;

      case LUA_TNUMBER: /* numbers */
        printf("%g", lua_tonumber(L, i));
        break;
      default: /* other values */
        printf("%s", lua_typename(L, t));
        break;
    }
    printf("\n"); /* put a separator */
  }
  printf("\n"); /* end the listing */
}

template <typename T>
void SaveOnRegistry(lua_State *lua, const char *name, T *ptr) {
  lua_pushstring(lua, name);
  if (ptr) {
    lua_pushlightuserdata(lua, ptr);
  } else {
    lua_pushnil(lua);
  }
  lua_settable(lua, LUA_REGISTRYINDEX);
}

template <typename T>
T *GetFromRegistry(lua_State *lua, const char *name) {
  lua_pushstring(lua, name);
  lua_gettable(lua, LUA_REGISTRYINDEX);

  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1); /* pops the value */
    return nullptr;
  }

  /* must be light user data */
  CHECK(lua_islightuserdata(lua, -1));
  auto *ptr = static_cast<T*>(lua_touserdata(lua, -1));
  
  CHECK_NOTNULL(ptr);

  /* pops the value */
  lua_pop(lua, 1);

  return ptr;
}

}  // namespace lua
