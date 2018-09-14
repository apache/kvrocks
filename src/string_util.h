#pragma once
#include <algorithm>
#include <cctype>
#include <string>

namespace Util {
std::string ToLower(std::string in);
std::string& Trim(std::string &in, std::string chars);
void Split(std::string in, std::string delim, std::vector<std::string> *out);
}
