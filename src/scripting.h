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
#include "status.h"
#include "redis_connection.h"

namespace Lua {

lua_State* CreateState(bool read_only = false);
void DestroyState(lua_State *lua);

void loadFuncs(lua_State *lua, bool read_only = false);
void loadLibraries(lua_State *lua);
void removeUnsupportedFunctions(lua_State *lua);
void enableGlobalsProtection(lua_State *lua);
int redisCallCommand(lua_State *lua);
int redisPCallCommand(lua_State *lua);
int redisGenericCommand(lua_State *lua, int raise_error);
int redisSha1hexCommand(lua_State *lua);
int redisStatusReplyCommand(lua_State *lua);
int redisErrorReplyCommand(lua_State *lua);
Status createFunction(Server *srv, const std::string &body, std::string *sha,
                      lua_State *lua);

int redisLogCommand(lua_State *lua);
Status evalGenericCommand(Redis::Connection *conn,
                          const std::vector<std::string> &args, bool evalsha,
                          std::string *output, bool read_only = false);

const char *redisProtocolToLuaType(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Int(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Bulk(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Status(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Error(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Aggregate(lua_State *lua, const char *reply, int atype);
const char *redisProtocolToLuaType_Null(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Bool(lua_State *lua, const char *reply, int tf);
const char *redisProtocolToLuaType_Double(lua_State *lua, const char *reply);
std::string replyToRedisReply(lua_State *lua);
void pushError(lua_State *lua, const char *err);
[[noreturn]] int raiseError(lua_State *lua);
void sortArray(lua_State *lua);
void setGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems);

void SHA1Hex(char *digest, const char *script, size_t len);

int redisMathRandom(lua_State *L);
int redisMathRandomSeed(lua_State *L);
}  // namespace Lua
