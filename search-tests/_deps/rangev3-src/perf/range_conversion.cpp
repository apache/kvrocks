// Range v3 library
//
//  Copyright 2019-present Christopher Di Bella
//  Copyright 2019-present Eric Niebler
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//

// Benchmark for https://github.com/ericniebler/range-v3/issues/1337

#include <string>
#include <vector>

#include <benchmark/benchmark.h>

#include <range/v3/algorithm/equal.hpp>
#include <range/v3/range/conversion.hpp>
#include <range/v3/range/primitives.hpp>
#include <range/v3/view/common.hpp>
#include <range/v3/view/reverse.hpp>
#include <range/v3/view/transform.hpp>

using namespace ranges;

namespace
{
    auto palindrome_range_common(std::vector<std::string> const & words)
    {
        auto is_palindrome = [](auto const & word) {
            return !ranges::empty(word) && ranges::equal(word, word | views::reverse);
        };

        auto palindrome_excalim = [&is_palindrome](auto const & word) {
            return is_palindrome(word) ? word + '!' : word;
        };

        auto result = words | views::transform(palindrome_excalim) | views::common;

        return std::vector<std::string>{ranges::begin(result), ranges::end(result)};
    }

    auto palindrome_range_to(std::vector<std::string> const & words)
    {
        auto is_palindrome = [](auto const & word) {
            return !ranges::empty(word) && ranges::equal(word, word | views::reverse);
        };

        auto palindrome_excalim = [&is_palindrome](auto const & word) {
            return is_palindrome(word) ? word + '!' : word;
        };

        return words | views::transform(palindrome_excalim) | ranges::to<std::vector>;
    }
} // namespace

class Words : public ::benchmark::Fixture
{
protected:
    std::vector<std::string> words_;

public:
    void SetUp(const ::benchmark::State &)
    {
        auto magic = 476'000u;
        words_.reserve(magic);
        for(auto i = 0u; i < magic; ++i)
        {
            words_.push_back("this");
            words_.push_back("is");
            words_.push_back("his");
            words_.push_back("face");
            words_.push_back("abba");
            words_.push_back("toot");
        }
    }
};

BENCHMARK_F(Words, RangeCommon)(benchmark::State & st)
{
    for(auto _ : st)
    {
        auto result = ::palindrome_range_common(words_);
        benchmark::DoNotOptimize(result.data());
        benchmark::ClobberMemory();
    }
}

BENCHMARK_F(Words, RangeTo)(benchmark::State & st)
{
    for(auto _ : st)
    {
        auto result = ::palindrome_range_to(words_);
        benchmark::DoNotOptimize(result.data());
        benchmark::ClobberMemory();
    }
}
