#pragma once

#include <algorithm>
#include <cctype>
#include <string>
#include <vector>

namespace Util {
std::string ToLower(std::string in);
void BytesToHuman(char *s, unsigned long long n);
std::string& Trim(std::string &in, std::string chars);
void Split(std::string in, std::string delim, std::vector<std::string> *out);
}
