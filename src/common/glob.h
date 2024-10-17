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

#include <algorithm>
#include <cctype>
#include <string>
#include <string_view>

namespace util {

constexpr inline bool GlobMatchImpl(std::string_view pattern, std::string_view string, bool ignore_case,
                                    bool *skip_longer_matches, size_t recursion_depth = 0) {
  // If we want to ignore case, this is equivalent to converting both the pattern and the string to lowercase
  const auto canonicalize = [ignore_case](unsigned char c) -> unsigned char {
    return ignore_case ? static_cast<unsigned char>(std::tolower(c)) : c;
  };

  if (recursion_depth > 1000) return false;

  while (!pattern.empty() && !string.empty()) {
    switch (pattern[0]) {
      case '*':
        // Optimization: collapse multiple * into one
        while (pattern.size() >= 2 && pattern[1] == '*') {
          pattern.remove_prefix(1);
        }
        // Optimization: If the '*' is the last character in the pattern, it can match anything
        if (pattern.length() == 1) return true;
        while (!string.empty()) {
          if (GlobMatchImpl(pattern.substr(1), string, ignore_case, skip_longer_matches, recursion_depth + 1))
            return true;
          if (*skip_longer_matches) return false;
          string.remove_prefix(1);
        }
        // There was no match for the rest of the pattern starting
        // from anywhere in the rest of the string. If there were
        // any '*' earlier in the pattern, we can terminate the
        // search early without trying to match them to longer
        // substrings. This is because a longer match for the
        // earlier part of the pattern would require the rest of the
        // pattern to match starting later in the string, and we
        // have just determined that there is no match for the rest
        // of the pattern starting from anywhere in the current
        // string.
        *skip_longer_matches = true;
        return false;
      case '?':
        if (string.empty()) return false;
        string.remove_prefix(1);
        break;
      case '[': {
        pattern.remove_prefix(1);
        const bool invert = pattern[0] == '^';
        if (invert) pattern.remove_prefix(1);

        bool match = false;
        while (true) {
          if (pattern.empty()) {
            // unterminated [ group: reject invalid pattern
            return false;
          } else if (pattern[0] == ']') {
            break;
          } else if (pattern.length() >= 2 && pattern[0] == '\\') {
            pattern.remove_prefix(1);
            if (pattern[0] == string[0]) match = true;
          } else if (pattern.length() >= 3 && pattern[1] == '-') {
            unsigned char start = canonicalize(pattern[0]);
            unsigned char end = canonicalize(pattern[2]);
            if (start > end) std::swap(start, end);
            const int c = canonicalize(string[0]);
            pattern.remove_prefix(2);

            if (c >= start && c <= end) match = true;
          } else if (canonicalize(pattern[0]) == canonicalize(string[0])) {
            match = true;
          }
          pattern.remove_prefix(1);
        }
        if (invert) match = !match;
        if (!match) return false;
        string.remove_prefix(1);
        break;
      }
      case '\\':
        if (pattern.length() >= 2) {
          pattern.remove_prefix(1);
        }
        [[fallthrough]];
      default:
        // Just a normal character
        if (!ignore_case) {
          if (pattern[0] != string[0]) return false;
        } else {
          if (std::tolower((int)pattern[0]) != std::tolower((int)string[0])) return false;
        }
        string.remove_prefix(1);
        break;
    }
    pattern.remove_prefix(1);
  }

  // Now that either the pattern is empty or the string is empty, this is a match iff
  // the pattern consists only of '*', and the string is empty.
  return string.empty() && std::all_of(pattern.begin(), pattern.end(), [](char c) { return c == '*'; });
}

// Given a glob [pattern] and a string [string], return true iff the string matches the glob.
// If [ignore_case] is true, the match is case-insensitive.
constexpr inline bool GlobMatches(std::string_view glob, std::string_view str, bool ignore_case = false) {
  bool skip_longer_matches = false;
  return GlobMatchImpl(glob, str, ignore_case, &skip_longer_matches);
}

// Split a glob pattern into a literal prefix and a suffix containing wildcards.
// For example, if the user calls [KEYS bla*bla], this function will return {"bla", "*bla"}.
// This allows the caller of this function to optimize this call by performing a
// prefix-scan on "bla" and then filtering the results using the GlobMatches function.
inline std::pair<std::string, std::string> SplitGlob(std::string_view glob) {
  // Stores the prefix of the glob pattern, with backslashes removed
  std::string prefix;
  // Find the first un-escaped '*', '?' or '[' character in [glob]
  for (size_t idx = 0; idx < glob.size(); ++idx) {
    if (glob[idx] == '*' || glob[idx] == '?' || glob[idx] == '[') {
      // Return a pair of views: the part of the glob before the wildcard, and the part after
      return {prefix, std::string(glob.substr(idx))};
    } else if (glob[idx] == '\\') {
      // Skip checking whether the next character is a special character
      ++idx;
      // Append the escaped special character to the prefix
      prefix.push_back(glob[idx]);
    } else {
      prefix.push_back(glob[idx]);
    }
  }
  // No wildcard found, return the entire string (without the backslashes) as the prefix
  return {prefix, ""};
}

}  // namespace util