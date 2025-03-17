#include "luaconf.h"

typedef struct lua_State lua_State;

LUALIB_API int(luaopen_cjson)(lua_State *L);
LUALIB_API int(luaopen_struct)(lua_State *L);
LUALIB_API int(luaopen_cmsgpack)(lua_State *L);
LUALIB_API int(luaopen_bit)(lua_State *L);
