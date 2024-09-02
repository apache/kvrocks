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

#include "string_util.h"

#include <fmt/format.h>

#include <regex>
#include <string>

#include "parse_util.h"

namespace util {

std::string Float2String(double d) {
  if (std::isinf(d)) {
    return d > 0 ? "inf" : "-inf";
  }

  return fmt::format("{:.17g}", d);
}

std::string ToLower(std::string in) {
  std::transform(in.begin(), in.end(), in.begin(), [](char c) -> char { return static_cast<char>(std::tolower(c)); });
  return in;
}

bool EqualICase(std::string_view lhs, std::string_view rhs) {
  return lhs.size() == rhs.size() && std::equal(lhs.begin(), lhs.end(), rhs.begin(),
                                                [](char l, char r) { return std::tolower(l) == std::tolower(r); });
}

std::string Trim(std::string in, std::string_view chars) {
  if (in.empty()) return in;

  in.erase(0, in.find_first_not_of(chars));
  in.erase(in.find_last_not_of(chars) + 1);

  return in;
}

std::vector<std::string> Split(std::string_view in, std::string_view delim) {
  std::vector<std::string> out;

  if (in.empty()) {
    return out;
  }

  if (delim.empty()) {
    out.resize(in.size());
    std::transform(in.begin(), in.end(), out.begin(), [](char c) -> std::string { return {c}; });
    return out;
  }

  size_t begin = 0, end = in.find_first_of(delim);
  do {
    std::string_view elem = in.substr(begin, end - begin);
    if (!elem.empty()) out.emplace_back(elem.begin(), elem.end());
    if (end == std::string::npos) break;
    begin = end + 1;
    end = in.find_first_of(delim, begin);
  } while (true);

  return out;
}

std::vector<std::string> Split2KV(const std::string &in, const std::string &delim) {
  std::vector<std::string> out;

  std::string::size_type pos = in.find_first_of(delim);
  if (pos != std::string::npos) {
    std::string key = in.substr(0, pos);
    if (!key.empty()) out.push_back(std::move(key));

    std::string value = Trim(in.substr(pos + 1), delim);
    if (!value.empty()) out.push_back(std::move(value));
  }

  return out;
}

bool HasPrefix(const std::string &str, const std::string &prefix) {
  if (str.empty() || prefix.empty()) return false;
  return !strncasecmp(str.data(), prefix.data(), prefix.size());
}

int StringMatch(const std::string &pattern, const std::string &in, int nocase) {
  return StringMatchLen(pattern.c_str(), pattern.length(), in.c_str(), in.length(), nocase);
}

// Glob-style pattern matching.
int StringMatchLen(const char *pattern, size_t pattern_len, const char *string, size_t string_len, int nocase) {
  while (pattern_len && string_len) {
    switch (pattern[0]) {
      case '*':
        while (pattern[1] == '*') {
          pattern++;
          pattern_len--;
        }

        if (pattern_len == 1) return 1; /* match */

        while (string_len) {
          if (StringMatchLen(pattern + 1, pattern_len - 1, string, string_len, nocase)) return 1; /* match */
          string++;
          string_len--;
        }
        return 0; /* no match */
      case '?':
        string++;
        string_len--;
        break;
      case '[': {
        pattern++;
        pattern_len--;
        int not_symbol = pattern[0] == '^';
        if (not_symbol) {
          pattern++;
          pattern_len--;
        }

        int match = 0;
        while (true) {
          if (pattern[0] == '\\' && pattern_len >= 2) {
            pattern++;
            pattern_len--;
            if (pattern[0] == string[0]) match = 1;
          } else if (pattern[0] == ']') {
            break;
          } else if (pattern_len == 0) {
            pattern--;
            pattern_len++;
            break;
          } else if (pattern[1] == '-' && pattern_len >= 3) {
            int start = pattern[0];
            int end = pattern[2];
            int c = string[0];
            if (start > end) {
              int t = start;
              start = end;
              end = t;
            }
            if (nocase) {
              start = tolower(start);
              end = tolower(end);
              c = tolower(c);
            }
            pattern += 2;
            pattern_len -= 2;
            if (c >= start && c <= end) match = 1;
          } else {
            if (!nocase) {
              if (pattern[0] == string[0]) match = 1;
            } else {
              if (tolower(static_cast<int>(pattern[0])) == tolower(static_cast<int>(string[0]))) match = 1;
            }
          }
          pattern++;
          pattern_len--;
        }

        if (not_symbol) match = !match;

        if (!match) return 0; /* no match */

        string++;
        string_len--;
        break;
      }
      case '\\':
        if (pattern_len >= 2) {
          pattern++;
          pattern_len--;
        }
        /* fall through */
      default:
        if (!nocase) {
          if (pattern[0] != string[0]) return 0; /* no match */
        } else {
          if (tolower(static_cast<int>(pattern[0])) != tolower(static_cast<int>(string[0]))) return 0; /* no match */
        }
        string++;
        string_len--;
        break;
    }
    pattern++;
    pattern_len--;
    if (string_len == 0) {
      while (*pattern == '*') {
        pattern++;
        pattern_len--;
      }
      break;
    }
  }

  if (pattern_len == 0 && string_len == 0) return 1;
  return 0;
}

std::vector<std::string> RegexMatch(const std::string &str, const std::string &regex) {
  std::regex base_regex(regex);
  std::smatch pieces_match;
  std::vector<std::string> out;

  if (std::regex_match(str, pieces_match, base_regex)) {
    for (const auto &piece : pieces_match) {
      out.emplace_back(piece.str());
    }
  }
  return out;
}

std::string StringToHex(std::string_view input) {
  static const char hex_digits[] = "0123456789ABCDEF";
  std::string output;
  output.reserve(input.length() * 2);
  for (unsigned char c : input) {
    output.push_back(hex_digits[c >> 4]);
    output.push_back(hex_digits[c & 15]);
  }
  return output;
}

constexpr unsigned long long ExpTo1024(unsigned n) { return 1ULL << (n * 10); }

std::string BytesToHuman(uint64_t n) {
  if (n < ExpTo1024(1)) {
    return fmt::format("{}B", n);
  } else if (n < ExpTo1024(2)) {
    return fmt::format("{:.2f}K", static_cast<double>(n) / ExpTo1024(1));
  } else if (n < ExpTo1024(3)) {
    return fmt::format("{:.2f}M", static_cast<double>(n) / ExpTo1024(2));
  } else if (n < ExpTo1024(4)) {
    return fmt::format("{:.2f}G", static_cast<double>(n) / ExpTo1024(3));
  } else if (n < ExpTo1024(5)) {
    return fmt::format("{:.2f}T", static_cast<double>(n) / ExpTo1024(4));
  } else if (n < ExpTo1024(6)) {
    return fmt::format("{:.2f}P", static_cast<double>(n) / ExpTo1024(5));
  } else {
    return fmt::format("{}B", n);
  }
}

std::vector<std::string> TokenizeRedisProtocol(const std::string &value) {
  std::vector<std::string> tokens;

  if (value.empty()) {
    return tokens;
  }

  enum ParserState { stateArrayLen, stateBulkLen, stateBulkData };
  uint64_t array_len = 0, bulk_len = 0;
  int state = stateArrayLen;
  const char *start = value.data(), *end = start + value.size(), *p = nullptr;

  while (start != end) {
    switch (state) {
      case stateArrayLen: {
        if (start[0] != '*') {
          return tokens;
        }

        p = strchr(start, '\r');
        if (!p || (p == end) || p[1] != '\n') {
          tokens.clear();
          return tokens;
        }

        // parse_result expects to must be parsed successfully here
        array_len = *ParseInt<uint64_t>(std::string(start + 1, p), 10);
        start = p + 2;
        state = stateBulkLen;
        break;
      }
      case stateBulkLen: {
        if (start[0] != '$') {
          return tokens;
        }

        p = strchr(start, '\r');
        if (!p || (p == end) || p[1] != '\n') {
          tokens.clear();
          return tokens;
        }

        // parse_result expects to must be parsed successfully here
        bulk_len = *ParseInt<uint64_t>(std::string(start + 1, p), 10);
        start = p + 2;
        state = stateBulkData;
        break;
      }
      case stateBulkData: {
        if (bulk_len + 2 > static_cast<uint64_t>(end - start)) {
          tokens.clear();
          return tokens;
        }

        tokens.emplace_back(start, start + bulk_len);
        start += bulk_len + 2;
        state = stateBulkLen;
        break;
      }
    }
  }

  if (array_len != tokens.size()) {
    tokens.clear();
  }

  return tokens;
}

/* escape string where all the non-printable characters
 * (tested with isprint()) are turned into escapes in
 * the form "\n\r\a...." or "\x<hex-number>". */
std::string EscapeString(std::string_view s) {
  std::string str;
  str.reserve(s.size());

  for (auto ch : s) {
    switch (ch) {
      case '\\':
      case '"':
        str += "\\";
        str += ch;
        break;
      case '\n':
        str += "\\n";
        break;
      case '\r':
        str += "\\r";
        break;
      case '\t':
        str += "\\t";
        break;
      case '\a':
        str += "\\a";
        break;
      case '\b':
        str += "\\b";
        break;
      case '\v':
        str += "\\v";
        break;
      case '\f':
        str += "\\f";
        break;
      default:
        if (isprint(ch)) {
          str += ch;
        } else {
          str += fmt::format("\\x{:02x}", (unsigned char)ch);
        }
    }
  }

  return str;
}

std::string StringNext(std::string s) {
  for (auto iter = s.rbegin(); iter != s.rend(); ++iter) {
    if (*iter != char(0xff)) {
      (*iter)++;
      break;
    }
  }
  return s;
}

}  // namespace util
