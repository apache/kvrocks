#pragma once

#include <string>
#include <vector>

extern "C" {
#include <lua.h>
#include <lauxlib.h>
#include <lualib.h>
}

#include "status.h"
#include "redis_connection.h"


namespace Lua {

lua_State* CreateState();

void loadFuncs(lua_State *lua);
void loadLibraries(lua_State *lua);
int redisCallCommand(lua_State *lua);
int redisGenericCommand(lua_State *lua, int raise_error);
Status createFunction(lua_State *lua, const std::string &body, std::string *sha);
Status evalGenericCommand(Redis::Connection *conn,
                          const std::vector<std::string> &args,
                          bool evalsha,
                          std::string *output);

const char * redisProtocolToLuaType(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Int(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Bulk(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Status(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Error(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Aggregate(lua_State *lua, const char *reply, int atype);
const char *redisProtocolToLuaType_Null(lua_State *lua, const char *reply);
const char *redisProtocolToLuaType_Bool(lua_State *lua, const char *reply, int tf);
const char *redisProtocolToLuaType_Double(lua_State *lua, const char *reply);
std::string luaReplyToRedisReply(lua_State *lua);
void pushError(lua_State *lua, const char *err);
int raiseError(lua_State *lua);
void sortArray(lua_State *lua);
void setGlobalArray(lua_State *lua, const std::string &var, const std::vector<std::string> &elems);

void SHA1Hex(char *digest, const char *script, size_t len);
}  // namespace Lua
