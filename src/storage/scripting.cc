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

// This file is modified from several source code files about lua scripting of Redis.
// See the original code at https://github.com/redis/redis.

#include "scripting.h"

#include <math.h>

#include <cctype>
#include <string>

#include "commands/commander.h"
#include "fmt/format.h"
#include "parse_util.h"
#include "rand.h"
#include "server/redis_connection.h"
#include "server/server.h"
#include "sha1.h"

/* The maximum number of characters needed to represent a long double
 * as a string (long double has a huge range).
 * This should be the size of the buffer given to doule to string */
constexpr size_t MAX_LONG_DOUBLE_CHARS = 5 * 1024;

enum {
  LL_DEBUG = 0,
  LL_VERBOSE,
  LL_NOTICE,
  LL_WARNING,
};

namespace Lua {

lua_State *CreateState(bool read_only) {
  lua_State *lua = lua_open();
  loadLibraries(lua);
  removeUnsupportedFunctions(lua);
  loadFuncs(lua, read_only);
  enableGlobalsProtection(lua);
  return lua;
}

void DestroyState(lua_State *lua) {
  lua_gc(lua, LUA_GCCOLLECT, 0);
  lua_close(lua);
}

void loadFuncs(lua_State *lua, bool read_only) {
  lua_newtable(lua);

  /* redis.call */
  lua_pushstring(lua, "call");
  lua_pushcfunction(lua, redisCallCommand);
  lua_settable(lua, -3);

  /* redis.pcall */
  lua_pushstring(lua, "pcall");
  lua_pushcfunction(lua, redisPCallCommand);
  lua_settable(lua, -3);

  /* redis.log and log levels. */
  lua_pushstring(lua, "log");
  lua_pushcfunction(lua, redisLogCommand);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_DEBUG");
  lua_pushnumber(lua, LL_DEBUG);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_VERBOSE");
  lua_pushnumber(lua, LL_VERBOSE);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_NOTICE");
  lua_pushnumber(lua, LL_NOTICE);
  lua_settable(lua, -3);

  lua_pushstring(lua, "LOG_WARNING");
  lua_pushnumber(lua, LL_WARNING);
  lua_settable(lua, -3);

  /* redis.sha1hex */
  lua_pushstring(lua, "sha1hex");
  lua_pushcfunction(lua, redisSha1hexCommand);
  lua_settable(lua, -3);

  /* redis.error_reply and redis.status_reply */
  lua_pushstring(lua, "error_reply");
  lua_pushcfunction(lua, redisErrorReplyCommand);
  lua_settable(lua, -3);
  lua_pushstring(lua, "status_reply");
  lua_pushcfunction(lua, redisStatusReplyCommand);
  lua_settable(lua, -3);

  /* redis.read_only */
  lua_pushstring(lua, "read_only");
  lua_pushboolean(lua, read_only);
  lua_settable(lua, -3);

  lua_setglobal(lua, "redis");

  /* Replace math.random and math.randomseed with our implementations. */
  lua_getglobal(lua, "math");

  lua_pushstring(lua, "random");
  lua_pushcfunction(lua, redisMathRandom);
  lua_settable(lua, -3);

  lua_pushstring(lua, "randomseed");
  lua_pushcfunction(lua, redisMathRandomSeed);
  lua_settable(lua, -3);

  lua_setglobal(lua, "math");

  /* Add a helper function we use for pcall error reporting.
   * Note that when the error is in the C function we want to report the
   * information about the caller, that's what makes sense from the point
   * of view of the user debugging a script. */
  const char *err_func =
      "local dbg = debug\n"
      "function __redis__err__handler(err)\n"
      "  local i = dbg.getinfo(2,'nSl')\n"
      "  if i and i.what == 'C' then\n"
      "    i = dbg.getinfo(3,'nSl')\n"
      "  end\n"
      "  if i then\n"
      "    return i.source .. ':' .. i.currentline .. ': ' .. err\n"
      "  else\n"
      "    return err\n"
      "  end\n"
      "end\n";
  luaL_loadbuffer(lua, err_func, strlen(err_func), "@err_handler_def");
  lua_pcall(lua, 0, 0, 0);

  const char *compare_func =
      "function __redis__compare_helper(a,b)\n"
      "  if a == false then a = '' end\n"
      "  if b == false then b = '' end\n"
      "  return a<b\n"
      "end\n";
  luaL_loadbuffer(lua, compare_func, strlen(compare_func), "@cmp_func_def");
  lua_pcall(lua, 0, 0, 0);
}

int redisLogCommand(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc < 2) {
    lua_pushstring(lua, "redis.log() requires two arguments or more.");
    return lua_error(lua);
  }
  if (!lua_isnumber(lua, -argc)) {
    lua_pushstring(lua, "First argument must be a number (log level).");
    return lua_error(lua);
  }
  int level = static_cast<int>(lua_tonumber(lua, -argc));
  if (level < LL_DEBUG || level > LL_WARNING) {
    lua_pushstring(lua, "Invalid debug level.");
    return lua_error(lua);
  }

  std::string log_message;
  for (int j = 1; j < argc; j++) {
    size_t len = 0;
    if (const char *s = lua_tolstring(lua, j - argc, &len)) {
      if (j != 1) {
        log_message += " ";
      }
      log_message += std::string(s, len);
    }
  }

  // The min log level was INFO, DEBUG would never take effect
  switch (level) {
    case LL_VERBOSE:  // also regard VERBOSE as INFO here since no VERBOSE level
    case LL_NOTICE:
      LOG(INFO) << "[Lua] " << log_message;
      break;
    case LL_WARNING:
      LOG(WARNING) << "[Lua] " << log_message;
      break;
  }
  return 0;
}

Status evalGenericCommand(Redis::Connection *conn, const std::string &body_or_sha, const std::vector<std::string> &keys,
                          const std::vector<std::string> &argv, bool evalsha, std::string *output, bool read_only) {
  Server *srv = conn->GetServer();

  // Use the worker's private Lua VM when entering the read-only mode
  lua_State *lua = read_only ? conn->Owner()->Lua() : srv->Lua();

  /* We obtain the script SHA1, then check if this function is already
   * defined into the Lua state */
  char funcname[2 + 40 + 1] = REDIS_LUA_FUNC_SHA_PREFIX;

  if (!evalsha) {
    SHA1Hex(funcname + 2, body_or_sha.c_str(), body_or_sha.size());
  } else {
    for (int j = 0; j < 40; j++) {
      funcname[j + 2] = static_cast<char>(tolower(body_or_sha[j]));
    }
  }

  /* Push the pcall error handler function on the stack. */
  lua_getglobal(lua, "__redis__err__handler");

  /* Try to lookup the Lua function */
  lua_getglobal(lua, funcname);
  if (lua_isnil(lua, -1)) {
    lua_pop(lua, 1); /* remove the nil from the stack */
    std::string body;
    if (evalsha) {
      auto s = srv->ScriptGet(funcname + 2, &body);
      if (!s.IsOK()) {
        lua_pop(lua, 1); /* remove the error handler from the stack. */
        return {Status::NotOK, "NOSCRIPT No matching script. Please use EVAL"};
      }
    } else {
      body = body_or_sha;
    }

    std::string sha = funcname + 2;
    auto s = createFunction(srv, body, &sha, lua, false);
    if (!s.IsOK()) {
      lua_pop(lua, 1); /* remove the error handler from the stack. */
      return s;
    }
    /* Now the following is guaranteed to return non nil */
    lua_getglobal(lua, funcname);
  }

  /* Populate the argv and keys table accordingly to the arguments that
   * EVAL received. */
  setGlobalArray(lua, "KEYS", keys);
  setGlobalArray(lua, "ARGV", argv);

  if (lua_pcall(lua, 0, 1, -2)) {
    auto msg = fmt::format("ERR running script (call to {}): {}", funcname, lua_tostring(lua, -1));
    *output = Redis::Error(msg);
    lua_pop(lua, 2);
  } else {
    *output = replyToRedisReply(lua);
    lua_pop(lua, 1);
  }

  // clean global variables to prevent information leak in function commands
  lua_pushnil(lua);
  lua_setglobal(lua, "KEYS");
  lua_pushnil(lua);
  lua_setglobal(lua, "ARGV");

  /* Call the Lua garbage collector from time to time to avoid a
   * full cycle performed by Lua, which adds too latency.
   *
   * The call is performed every LUA_GC_CYCLE_PERIOD executed commands
   * (and for LUA_GC_CYCLE_PERIOD collection steps) because calling it
   * for every command uses too much CPU. */
  constexpr int64_t LUA_GC_CYCLE_PERIOD = 50;
  static int64_t gc_count = 0;

  gc_count++;
  if (gc_count == LUA_GC_CYCLE_PERIOD) {
    lua_gc(lua, LUA_GCSTEP, LUA_GC_CYCLE_PERIOD);
    gc_count = 0;
  }

  return Status::OK();
}

int redisCallCommand(lua_State *lua) { return redisGenericCommand(lua, 1); }

int redisPCallCommand(lua_State *lua) { return redisGenericCommand(lua, 0); }

// TODO: we do not want to repeat same logic as Connection::ExecuteCommands,
// so the function need to be refactored
int redisGenericCommand(lua_State *lua, int raise_error) {
  lua_getglobal(lua, "redis");
  lua_getfield(lua, -1, "read_only");
  int read_only = lua_toboolean(lua, -1);
  lua_pop(lua, 2);

  int argc = lua_gettop(lua);
  if (argc == 0) {
    pushError(lua, "Please specify at least one argument for redis.call()");
    return raise_error ? raiseError(lua) : 1;
  }

  std::vector<std::string> args;
  for (int j = 1; j <= argc; j++) {
    if (lua_type(lua, j) == LUA_TNUMBER) {
      lua_Number num = lua_tonumber(lua, j);
      args.emplace_back(fmt::format("{:.17g}", static_cast<double>(num)));
    } else {
      size_t obj_len = 0;
      const char *obj_s = lua_tolstring(lua, j, &obj_len);
      if (obj_s == nullptr) {
        pushError(lua, "Lua redis() command arguments must be strings or integers");
        return raise_error ? raiseError(lua) : 1;
      }
      args.emplace_back(obj_s, obj_len);
    }
  }

  auto commands = Redis::GetCommands();
  auto cmd_iter = commands->find(Util::ToLower(args[0]));
  if (cmd_iter == commands->end()) {
    pushError(lua, "Unknown Redis command called from Lua script");
    return raise_error ? raiseError(lua) : 1;
  }

  auto redisCmd = cmd_iter->second;
  if (read_only && !(redisCmd->flags & Redis::kCmdReadOnly)) {
    pushError(lua, "Write commands are not allowed from read-only scripts");
    return raise_error ? raiseError(lua) : 1;
  }

  auto cmd = redisCmd->factory();
  cmd->SetAttributes(redisCmd);
  cmd->SetArgs(args);

  int arity = cmd->GetAttributes()->arity;
  if (((arity > 0 && argc != arity) || (arity < 0 && argc < -arity))) {
    pushError(lua, "Wrong number of args calling Redis command From Lua script");
    return raise_error ? raiseError(lua) : 1;
  }
  auto attributes = cmd->GetAttributes();
  if (attributes->flags & Redis::kCmdNoScript) {
    pushError(lua, "This Redis command is not allowed from scripts");
    return raise_error ? raiseError(lua) : 1;
  }

  std::string cmd_name = Util::ToLower(args[0]);
  Server *srv = GetServer();
  Config *config = srv->GetConfig();

  Redis::Connection *conn = srv->GetCurrentConnection();
  if (config->cluster_enabled) {
    auto s = srv->cluster_->CanExecByMySelf(attributes, args, conn);
    if (!s.IsOK()) {
      pushError(lua, s.Msg().c_str());
      return raise_error ? raiseError(lua) : 1;
    }
  }

  if (config->slave_readonly && srv->IsSlave() && attributes->is_write()) {
    pushError(lua, "READONLY You can't write against a read only slave.");
    return raise_error ? raiseError(lua) : 1;
  }

  if (!config->slave_serve_stale_data && srv->IsSlave() && cmd_name != "info" && cmd_name != "slaveof" &&
      srv->GetReplicationState() != kReplConnected) {
    pushError(lua,
              "MASTERDOWN Link with MASTER is down "
              "and slave-serve-stale-data is set to 'no'.");
    return raise_error ? raiseError(lua) : 1;
  }

  auto s = cmd->Parse(args);
  if (!s) {
    pushError(lua, s.Msg().data());
    return raise_error ? raiseError(lua) : 1;
  }

  srv->stats_.IncrCalls(cmd_name);
  auto start = std::chrono::high_resolution_clock::now();
  bool is_profiling = conn->isProfilingEnabled(cmd_name);
  std::string output;
  s = cmd->Execute(GetServer(), srv->GetCurrentConnection(), &output);
  auto end = std::chrono::high_resolution_clock::now();
  uint64_t duration = std::chrono::duration_cast<std::chrono::microseconds>(end - start).count();
  if (is_profiling) conn->recordProfilingSampleIfNeed(cmd_name, duration);
  srv->SlowlogPushEntryIfNeeded(&args, duration);
  srv->stats_.IncrLatency(static_cast<uint64_t>(duration), cmd_name);
  srv->FeedMonitorConns(conn, args);
  if (!s) {
    pushError(lua, s.Msg().data());
    return raise_error ? raiseError(lua) : 1;
  }

  redisProtocolToLuaType(lua, output.data());
  return 1;
}

void removeUnsupportedFunctions(lua_State *lua) {
  lua_pushnil(lua);
  lua_setglobal(lua, "loadfile");
  lua_pushnil(lua);
  lua_setglobal(lua, "dofile");
}

void enableGlobalsProtection(lua_State *lua) {
  const char *code =
      "local dbg=debug\n"
      "local mt = {}\n"
      "setmetatable(_G, mt)\n"
      "mt.__newindex = function (t, n, v)\n"
      "  if dbg.getinfo(2) then\n"
      "    local w = dbg.getinfo(2, \"S\").what\n"
      "    if w ~= \"main\" and w ~= \"C\" then\n"
      "      error(\"Script attempted to create global variable '\"..tostring(n)..\"'\", 2)\n"
      "    end\n"
      "  end\n"
      "  rawset(t, n, v)\n"
      "end\n"
      "mt.__index = function (t, n)\n"
      "  if dbg.getinfo(2) and dbg.getinfo(2, \"S\").what ~= \"C\" then\n"
      "    error(\"Script attempted to access nonexistent global variable '\"..tostring(n)..\"'\", 2)\n"
      "  end\n"
      "  return rawget(t, n)\n"
      "end\n"
      "debug = nil\n";

  luaL_loadbuffer(lua, code, strlen(code), "@enable_strict_lua");
  lua_pcall(lua, 0, 0, 0);
}

void loadLibraries(lua_State *lua) {
  auto loadLib = [](lua_State *lua, const char *libname, lua_CFunction func) {
    lua_pushcfunction(lua, func);
    lua_pushstring(lua, libname);
    lua_call(lua, 1, 0);
  };

  loadLib(lua, "", luaopen_base);
  loadLib(lua, LUA_TABLIBNAME, luaopen_table);
  loadLib(lua, LUA_STRLIBNAME, luaopen_string);
  loadLib(lua, LUA_MATHLIBNAME, luaopen_math);
  loadLib(lua, LUA_DBLIBNAME, luaopen_debug);
  loadLib(lua, "cjson", luaopen_cjson);
  loadLib(lua, "struct", luaopen_struct);
  loadLib(lua, "cmsgpack", luaopen_cmsgpack);
  loadLib(lua, "bit", luaopen_bit);
}

/* Returns a table with a single field 'field' set to the string value
 * passed as argument. This helper function is handy when returning
 * a Redis Protocol error or status reply from Lua:
 *
 * return redis.error_reply("ERR Some Error")
 * return redis.status_reply("ERR Some Error")
 */
int redisReturnSingleFieldTable(lua_State *lua, const char *field) {
  if (lua_gettop(lua) != 1 || lua_type(lua, -1) != LUA_TSTRING) {
    pushError(lua, "wrong number or type of arguments");
    return 1;
  }

  lua_newtable(lua);
  lua_pushstring(lua, field);
  lua_pushvalue(lua, -3);
  lua_settable(lua, -3);
  return 1;
}

/* redis.error_reply() */
int redisErrorReplyCommand(lua_State *lua) { return redisReturnSingleFieldTable(lua, "err"); }

/* redis.status_reply() */
int redisStatusReplyCommand(lua_State *lua) { return redisReturnSingleFieldTable(lua, "ok"); }

/* This adds redis.sha1hex(string) to Lua scripts using the same hashing
 * function used for sha1ing lua scripts. */
int redisSha1hexCommand(lua_State *lua) {
  int argc = lua_gettop(lua);

  if (argc != 1) {
    lua_pushstring(lua, "wrong number of arguments");
    return lua_error(lua);
  }

  size_t len = 0;
  const char *s = static_cast<const char *>(lua_tolstring(lua, 1, &len));

  char digest[41];
  SHA1Hex(digest, s, len);
  lua_pushstring(lua, digest);
  return 1;
}

/* ---------------------------------------------------------------------------
 * Utility functions.
 * ------------------------------------------------------------------------- */

/* Perform the SHA1 of the input string. We use this both for hashing script
 * bodies in order to obtain the Lua function name, and in the implementation
 * of redis.sha1().
 *
 * 'digest' should point to a 41 bytes buffer: 40 for SHA1 converted into an
 * hexadecimal number, plus 1 byte for null term. */
void SHA1Hex(char *digest, const char *script, size_t len) {
  SHA1_CTX ctx;
  unsigned char hash[20];
  const char *cset = "0123456789abcdef";

  SHA1Init(&ctx);
  SHA1Update(&ctx, (const unsigned char *)script, len);
  SHA1Final(hash, &ctx);

  for (int j = 0; j < 20; j++) {
    digest[j * 2] = cset[((hash[j] & 0xF0) >> 4)];
    digest[j * 2 + 1] = cset[(hash[j] & 0xF)];
  }
  digest[40] = '\0';
}

/*
 * ---------------------------------------------------------------------------
 * Redis reply to Lua type conversion functions.
 * ------------------------------------------------------------------------- */

/* Take a Redis reply in the Redis protocol format and convert it into a
 * Lua type. Thanks to this function, and the introduction of not connected
 * clients, it is trivial to implement the redis() lua function.
 *
 * Basically we take the arguments, execute the Redis command in the context
 * of a non connected client, then take the generated reply and convert it
 * into a suitable Lua type. With this trick the scripting feature does not
 * need the introduction of a full Redis internals API. The script
 * is like a normal client that bypasses all the slow I/O paths.
 *
 * Note: in this function we do not do any sanity check as the reply is
 * generated by Redis directly. This allows us to go faster.
 *
 * Errors are returned as a table with a single 'err' field set to the
 * error string.
 */

const char *redisProtocolToLuaType(lua_State *lua, const char *reply) {
  const char *p = reply;

  switch (*p) {
    case ':':
      p = redisProtocolToLuaType_Int(lua, reply);
      break;
    case '$':
      p = redisProtocolToLuaType_Bulk(lua, reply);
      break;
    case '+':
      p = redisProtocolToLuaType_Status(lua, reply);
      break;
    case '-':
      p = redisProtocolToLuaType_Error(lua, reply);
      break;
    case '*':
      p = redisProtocolToLuaType_Aggregate(lua, reply, *p);
      break;
    case '%':
      p = redisProtocolToLuaType_Aggregate(lua, reply, *p);
      break;
    case '~':
      p = redisProtocolToLuaType_Aggregate(lua, reply, *p);
      break;
    case '_':
      p = redisProtocolToLuaType_Null(lua, reply);
      break;
    case '#':
      p = redisProtocolToLuaType_Bool(lua, reply, p[1]);
      break;
    case ',':
      p = redisProtocolToLuaType_Double(lua, reply);
      break;
  }
  return p;
}

const char *redisProtocolToLuaType_Int(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  auto value = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);
  lua_pushnumber(lua, static_cast<lua_Number>(value));
  return p + 2;
}

const char *redisProtocolToLuaType_Bulk(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  auto bulklen = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);

  if (bulklen == -1) {
    lua_pushboolean(lua, 0);
    return p + 2;
  } else {
    lua_pushlstring(lua, p + 2, bulklen);
    return p + 2 + bulklen + 2;
  }
}

const char *redisProtocolToLuaType_Status(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');

  lua_newtable(lua);
  lua_pushstring(lua, "ok");
  lua_pushlstring(lua, reply + 1, p - reply - 1);
  lua_settable(lua, -3);
  return p + 2;
}

const char *redisProtocolToLuaType_Error(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');

  lua_newtable(lua);
  lua_pushstring(lua, "err");
  lua_pushlstring(lua, reply + 1, p - reply - 1);
  lua_settable(lua, -3);
  return p + 2;
}

const char *redisProtocolToLuaType_Aggregate(lua_State *lua, const char *reply, int atype) {
  const char *p = strchr(reply + 1, '\r');
  int64_t mbulklen = ParseInt<int64_t>(std::string(reply + 1, p - reply - 1), 10).ValueOr(0);
  int j = 0;

  p += 2;
  if (mbulklen == -1) {
    lua_pushboolean(lua, 0);
    return p;
  }
  lua_newtable(lua);
  for (j = 0; j < mbulklen; j++) {
    lua_pushnumber(lua, j + 1);
    p = redisProtocolToLuaType(lua, p);
    lua_settable(lua, -3);
  }
  return p;
}

const char *redisProtocolToLuaType_Null(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  lua_pushnil(lua);
  return p + 2;
}

const char *redisProtocolToLuaType_Bool(lua_State *lua, const char *reply, int tf) {
  const char *p = strchr(reply + 1, '\r');
  lua_pushboolean(lua, tf == 't');
  return p + 2;
}

const char *redisProtocolToLuaType_Double(lua_State *lua, const char *reply) {
  const char *p = strchr(reply + 1, '\r');
  char buf[MAX_LONG_DOUBLE_CHARS + 1];
  size_t len = p - reply - 1;
  double d = NAN;

  if (len <= MAX_LONG_DOUBLE_CHARS) {
    memcpy(buf, reply + 1, len);
    buf[len] = '\0';
    d = strtod(buf, nullptr); /* We expect a valid representation. */
  } else {
    d = 0;
  }

  lua_newtable(lua);
  lua_pushstring(lua, "double");
  lua_pushnumber(lua, d);
  lua_settable(lua, -3);
  return p + 2;
}

/* This function is used in order to push an error on the Lua stack in the
 * format used by redis.pcall to return errors, which is a lua table
 * with a single "err" field set to the error string. Note that this
 * table is never a valid reply by proper commands, since the returned
 * tables are otherwise always indexed by integers, never by strings. */
void pushError(lua_State *lua, const char *err) {
  lua_newtable(lua);
  lua_pushstring(lua, "err");
  lua_pushstring(lua, err);
  lua_settable(lua, -3);
}

std::string replyToRedisReply(lua_State *lua) {
  std::string output;
  const char *obj_s = nullptr;
  size_t obj_len = 0;

  int t = lua_type(lua, -1);
  switch (t) {
    case LUA_TSTRING:
      obj_s = lua_tolstring(lua, -1, &obj_len);
      output = Redis::BulkString(std::string(obj_s, obj_len));
      break;
    case LUA_TBOOLEAN:
      output = lua_toboolean(lua, -1) ? Redis::Integer(1) : Redis::NilString();
      break;
    case LUA_TNUMBER:
      output = Redis::Integer((int64_t)(lua_tonumber(lua, -1)));
      break;
    case LUA_TTABLE:
      /* We need to check if it is an array, an error, or a status reply.
       * Error are returned as a single element table with 'err' field.
       * Status replies are returned as single element table with 'ok'
       * field. */

      /* Handle error reply. */
      lua_pushstring(lua, "err");
      lua_gettable(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        output = Redis::Error(lua_tostring(lua, -1));
        lua_pop(lua, 2);
        return output;
      }
      lua_pop(lua, 1); /* Discard field name pushed before. */
      /* Handle status reply. */
      lua_pushstring(lua, "ok");
      lua_gettable(lua, -2);
      t = lua_type(lua, -1);
      if (t == LUA_TSTRING) {
        obj_s = lua_tolstring(lua, -1, &obj_len);
        output = Redis::BulkString(std::string(obj_s, obj_len));
        lua_pop(lua, 1);
        return output;
      } else {
        int j = 1, mbulklen = 0;
        lua_pop(lua, 1); /* Discard the 'ok' field value we popped */
        while (true) {
          lua_pushnumber(lua, j++);
          lua_gettable(lua, -2);
          t = lua_type(lua, -1);
          if (t == LUA_TNIL) {
            lua_pop(lua, 1);
            break;
          }
          mbulklen++;
          output += replyToRedisReply(lua);
        }
        output = Redis::MultiLen(mbulklen) + output;
      }
      break;
    default:
      output = Redis::NilString();
  }
  lua_pop(lua, 1);
  return output;
}

/* In case the error set into the Lua stack by pushError() was generated
 * by the non-error-trapping version of redis.pcall(), which is redis.call(),
 * this function will raise the Lua error so that the execution of the
 * script will be halted. */
[[noreturn]] int raiseError(lua_State *lua) {
  lua_pushstring(lua, "err");
  lua_gettable(lua, -2);
  lua_error(lua);
  __builtin_unreachable();
}

/* Sort the array currently in the stack. We do this to make the output
 * of commands like KEYS or SMEMBERS something deterministic when called
 * from Lua (to play well with AOf/replication).
 *
 * The array is sorted using table.sort itself, and assuming all the
 * list elements are strings. */
void sortArray(lua_State *lua) {
  /* Initial Stack: array */
  lua_getglobal(lua, "table");
  lua_pushstring(lua, "sort");
  lua_gettable(lua, -2);  /* Stack: array, table, table.sort */
  lua_pushvalue(lua, -3); /* Stack: array, table, table.sort, array */
  if (lua_pcall(lua, 1, 0, 0)) {
    /* Stack: array, table, error */

    /* We are not interested in the error, we assume that the problem is
     * that there are 'false' elements inside the array, so we try
     * again with a slower function but able to handle this case, that
     * is: table.sort(table, __redis__compare_helper) */
    lua_pop(lua, 1);             /* Stack: array, table */
    lua_pushstring(lua, "sort"); /* Stack: array, table, sort */
    lua_gettable(lua, -2);       /* Stack: array, table, table.sort */
    lua_pushvalue(lua, -3);      /* Stack: array, table, table.sort, array */
    lua_getglobal(lua, "__redis__compare_helper");
    /* Stack: array, table, table.sort, array, __redis__compare_helper */
    lua_call(lua, 2, 0);
  }
  /* Stack: array (sorted), table */
  lua_pop(lua, 1); /* Stack: array (sorted) */
}

void setGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems) {
  lua_newtable(lua);
  for (size_t i = 0; i < elems.size(); i++) {
    lua_pushlstring(lua, elems[i].c_str(), elems[i].size());
    lua_rawseti(lua, -2, static_cast<int>(i) + 1);
  }
  lua_setglobal(lua, var.c_str());
}

/* ---------------------------------------------------------------------------
 * Redis provided math.random
 * ------------------------------------------------------------------------- */

/* We replace math.random() with our implementation that is not affected
 * by specific libc random() implementations and will output the same sequence
 * (for the same seed) in every arch. */

/* The following implementation is the one shipped with Lua itself but with
 * rand() replaced by redisLrand48(). */
int redisMathRandom(lua_State *L) {
  /* the `%' avoids the (rare) case of r==1, and is needed also because on
     some systems (SunOS!) `rand()' may return a value larger than RAND_MAX */
  lua_Number r = (lua_Number)(redisLrand48() % REDIS_LRAND48_MAX) / (lua_Number)REDIS_LRAND48_MAX;
  switch (lua_gettop(L)) {  /* check number of arguments */
    case 0: {               /* no arguments */
      lua_pushnumber(L, r); /* Number between 0 and 1 */
      break;
    }
    case 1: { /* only upper limit */
      int u = luaL_checkint(L, 1);
      luaL_argcheck(L, 1 <= u, 1, "interval is empty");
      lua_pushnumber(L, floor(r * u) + 1); /* int between 1 and `u' */
      break;
    }
    case 2: { /* lower and upper limits */
      int l = luaL_checkint(L, 1);
      int u = luaL_checkint(L, 2);
      luaL_argcheck(L, l <= u, 2, "interval is empty");
      lua_pushnumber(L, floor(r * (u - l + 1)) + l); /* int between `l' and `u' */
      break;
    }
    default:
      return luaL_error(L, "wrong number of arguments");
  }
  return 1;
}

int redisMathRandomSeed(lua_State *L) {
  redisSrand48(luaL_checkint(L, 1));
  return 0;
}

/* ---------------------------------------------------------------------------
 * EVAL and SCRIPT commands implementation
 * ------------------------------------------------------------------------- */

/* Define a Lua function with the specified body.
 * The function name will be generated in the following form:
 *
 *   f_<hex sha1 sum>
 *
 * The function increments the reference count of the 'body' object as a
 * side effect of a successful call.
 *
 * On success a pointer to an SDS string representing the function SHA1 of the
 * just added function is returned (and will be valid until the next call
 * to scriptingReset() function), otherwise NULL is returned.
 *
 * The function handles the fact of being called with a script that already
 * exists, and in such a case, it behaves like in the success case.
 *
 * If 'c' is not NULL, on error the client is informed with an appropriate
 * error describing the nature of the problem and the Lua interpreter error. */
Status createFunction(Server *srv, const std::string &body, std::string *sha, lua_State *lua, bool need_to_store) {
  char funcname[2 + 40 + 1] = REDIS_LUA_FUNC_SHA_PREFIX;

  if (sha->empty()) {
    SHA1Hex(funcname + 2, body.c_str(), body.size());
    *sha = funcname + 2;
  } else {
    std::copy(sha->begin(), sha->end(), funcname + 2);
  }

  auto funcdef = fmt::format("function {}() {}\nend", funcname, body);

  if (luaL_loadbuffer(lua, funcdef.c_str(), funcdef.size(), "@user_script")) {
    std::string errMsg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return {Status::NotOK, "Error compiling script (new function): " + errMsg + "\n"};
  }
  if (lua_pcall(lua, 0, 0, 0)) {
    std::string errMsg = lua_tostring(lua, -1);
    lua_pop(lua, 1);
    return {Status::NotOK, "Error running script (new function): " + errMsg + "\n"};
  }
  // would store lua function into propagate column family and propagate those scripts to slaves
  return need_to_store ? srv->ScriptSet(*sha, body) : Status::OK();
}

}  // namespace Lua
