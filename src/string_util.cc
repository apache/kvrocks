#include "string_util.h"
#include <vector>

namespace Util {
std::string ToLower(std::string in) {
  std::transform(in.begin(), in.end(), in.begin(),
                 [](char c) -> char { return static_cast<char>(std::tolower(c)); });
  return in;
}

std::string& Trim(std::string &in, std::string chars) {
  if (in.empty()) return in;
  // left trim
  in.erase(0, in.find_first_not_of(chars));
  // right trim
  in.erase(in.find_last_not_of(chars)+1);
  return in;
}

void Split(std::string in, std::string delim, std::vector<std::string> *out) {
  if (in.empty() || !out) return;
  out->clear();
  std::string::size_type pos = 0;
  std::string elem;
  do {
    pos = in.find_first_of(delim);
    elem = in.substr(0, pos);
    elem = Trim(elem, delim);
    if (!elem.empty()) out->push_back(elem);
    in = in.substr(pos+1);
  } while(pos != std::string::npos);
}
}
