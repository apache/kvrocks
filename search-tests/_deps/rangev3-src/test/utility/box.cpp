// Range v3 library
//
//  Copyright Andrey Diduh 2019
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3

#include <range/v3/utility/compressed_pair.hpp>
#include "../simple_test.hpp"

using namespace ranges;

// #https://github.com/ericniebler/range-v3/issues/1093
void test_1093()
{
    struct Op {};
    struct Op2 {};

    struct payload { void* v; };
    struct base_adaptor {};

    struct RANGES_EMPTY_BASES A : base_adaptor, private box<Op, A> {};
    struct RANGES_EMPTY_BASES B : base_adaptor, private box<Op2, B> {};

    using P  = compressed_pair<A, payload>;
    using P2 = compressed_pair<B, P>;

    CHECK(sizeof(P) == sizeof(payload));
    CHECK(sizeof(P2) == sizeof(P));
}

int main()
{
    test_1093();

    return ::test_result();
}
