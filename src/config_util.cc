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

#include "config_util.h"
#include "util.h"


StatusOr<ConfigKV> ParseConfigLine(const std::string& line) {
  enum {
    KEY,  // in (unquoted) key string
    NORMAL,  // in unquoted value string
    QUOTED,  // in quoted value string
    PRE_KEY_SPACE,  // in whitespace characters before key
    AFTER_KEY_SPACE,  // in whitespace characters after key and before value
    AFTER_VAL_SPACE,  // in whitespace characters after value
    ESCAPE,  // in escape character of quoted string
    ERROR  // error state, e.g. encounter more than one value
  } state = PRE_KEY_SPACE;

  char quote;  // single or double quote
  std::string current_str;
  ConfigKV res;

  for (auto i = line.begin(); i != line.end();) {
    switch (state) {
      case PRE_KEY_SPACE:
        if (!std::isspace(*i)) {
          if (*i == '#') {
            i = line.end();
          } else {
            state = KEY;
          }
        } else {
          i++;
        }
        break;
      case KEY:
        if (std::isspace(*i)) {
          res.first = current_str;
          current_str = "";
          state = AFTER_KEY_SPACE;
        } else if (*i == '#') {
          res.first = current_str;
          i = line.end();
        } else {
          current_str.push_back(*i);
          i++;
        }
        break;
      case AFTER_KEY_SPACE:
        if (!std::isspace(*i)) {
          if (*i == '"' || *i == '\'') {
            state = QUOTED;
            quote = *i;
            i++;
          } else if (*i == '#') {
            i = line.end();
          } else {
            state = NORMAL;
          }
        } else {
          i++;
        }
        break;
      case NORMAL:
        if (*i == '#') {
          res.second = current_str;
          i = line.end();
        } else {
          current_str.push_back(*i);
          i++;
        }
        break;
      case QUOTED:
        if (*i == '\\') {
          state = ESCAPE;
        } else if (*i == quote) {
          res.second = current_str;
          state = AFTER_VAL_SPACE;
        } else {
          current_str.push_back(*i);
        }
        i++;
        break;
      case ESCAPE:
        if (*i == '\'' || *i == '"' || *i == '\\') {
          current_str.push_back(*i);
        } else if (*i == 't') {
          current_str.push_back('\t');
        } else if (*i == 'r') {
          current_str.push_back('\r');
        } else if (*i == 'n') {
          current_str.push_back('\n');
        } else if (*i == 'v') {
          current_str.push_back('\v');
        } else if (*i == 'f') {
          current_str.push_back('\f');
        } else if (*i == 'b') {
          current_str.push_back('\b');
        }
        state = QUOTED;
        i++;
        break;
      case AFTER_VAL_SPACE:
        if (!std::isspace(*i)) {
          if (*i == '#') {
            i = line.end();
          } else {
            state = ERROR;
          }
        } else {
          i++;
        }
        break;
      case ERROR:
        i = line.end();
        break;
    }
  }


  if (state == KEY) {
    res.first = current_str;
    state = AFTER_KEY_SPACE;
  } else if (state == NORMAL) {
    res.second = Util::Trim(current_str, " \t\r\n\v\f\b");
    state = AFTER_VAL_SPACE;
  } else if (state == QUOTED || state == ESCAPE) {
    return {Status::NotOK, "config line ends unexpectedly in quoted string"};
  } else if (state == ERROR) {
    return {Status::NotOK, "more than 2 item in config line"};
  }

  return res;
}

std::string DumpConfigLine(const ConfigKV &config) {
  std::string res;

  res += config.first;
  res += " ";

  if (std::any_of(config.second.begin(), config.second.end(), [](char c) {
    return std::isspace(c) || c == '"' || c == '\'' || c == '#';
  })) {
    res += '"';
    for (char c : config.second) {
      if (c == '\\') {
        res += "\\\\";
      } else if (c == '\'') {
        res += "\\'";
      } else if (c == '"') {
        res += "\\\"";
      } else if (c == '\t') {
        res += "\\t";
      } else if (c == '\r') {
        res += "\\r";
      } else if (c == '\n') {
        res += "\\n";
      } else if (c == '\v') {
        res += "\\v";
      } else if (c == '\f') {
        res += "\\f";
      } else if (c == '\b') {
        res += "\\b";
      } else {
        res += c;
      }
    }
    res += '"';
  } else if (config.second.empty()) {
    res += "\"\"";
  } else {
    res += config.second;
  }

  return res;
}
