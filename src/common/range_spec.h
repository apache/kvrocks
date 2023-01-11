#pragma once

#include <string>

struct CommonRangeLexSpec {
  std::string min, max;
  bool minex, maxex; /* are min or max exclusive */
  bool max_infinite; /* are max infinite */
  int64_t offset, count;
  bool removed, reversed;
  CommonRangeLexSpec()
      : minex(false), maxex(false), max_infinite(false), offset(-1), count(-1), removed(false), reversed(false) {}
};
