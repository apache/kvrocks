#ifndef JSONCONS_TESTS_TEST_UTILITIES_HPP
#define JSONCONS_TESTS_TEST_UTILITIES_HPP

#include <random>
#include <string>
#include <cstddef>

template <typename Generator>
std::string random_binary_string(Generator& gen, std::size_t n)
{
  std::string s;
  s.reserve(n);
  for (std::size_t i = 0; i < n; ++i)
  {
      auto c = static_cast<char>(std::uniform_int_distribution<int>('0', '1')(gen));
      s.push_back(c);
  }
  return s;
}

#endif
