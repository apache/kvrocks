// Range v3 library
//
//  Copyright Eric Niebler 2014-present
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
//  Copyright 2005 - 2007 Adobe Systems Incorporated
//  Distributed under the MIT License(see accompanying file LICENSE_1_0_0.txt
//  or a copy at http://stlab.adobe.com/licenses.html)

//===----------------------------------------------------------------------===//
//
//                     The LLVM Compiler Infrastructure
//
// This file is dual licensed under the MIT and the University of Illinois Open
// Source Licenses. See LICENSE.TXT for details.
//
//===----------------------------------------------------------------------===//

#include <utility>
#include <vector>
#include <range/v3/core.hpp>
#include <range/v3/algorithm/rotate.hpp>
#include "../simple_test.hpp"
#include "../test_iterators.hpp"

template<class Iter, class Sent = Iter>
void test()
{
    using namespace ranges;
    using Res = subrange<Iter>;

    int ia[] = {0};
    const unsigned sa = sizeof(ia)/sizeof(ia[0]);
    Res r = rotate(Iter(ia), Iter(ia), Sent(ia));
    CHECK(base(r.begin()) == ia);
    CHECK(base(r.end()) == ia);
    CHECK(ia[0] == 0);
    r = rotate(Iter(ia), Iter(ia), Sent(ia+sa));
    CHECK(base(r.begin()) == ia+sa);
    CHECK(base(r.end()) == ia+sa);
    CHECK(ia[0] == 0);
    r = rotate(Iter(ia), Iter(ia+sa), Sent(ia+sa));
    CHECK(base(r.begin()) == ia);
    CHECK(base(r.end()) == ia+sa);
    CHECK(ia[0] == 0);

    int ib[] = {0, 1};
    const unsigned sb = sizeof(ib)/sizeof(ib[0]);
    r = rotate(Iter(ib), Iter(ib), Sent(ib+sb));
    CHECK(base(r.begin()) == ib+sb);
    CHECK(base(r.end()) == ib+sb);
    CHECK(ib[0] == 0);
    CHECK(ib[1] == 1);
    r = rotate(Iter(ib), Iter(ib+1), Sent(ib+sb));
    CHECK(base(r.end()) == ib+sb);
    CHECK(base(r.begin()) == ib+1);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 0);
    r = rotate(Iter(ib), Iter(ib+sb), Sent(ib+sb));
    CHECK(base(r.end()) == ib+sb);
    CHECK(base(r.begin()) == ib);
    CHECK(ib[0] == 1);
    CHECK(ib[1] == 0);

    int ic[] = {0, 1, 2};
    const unsigned sc = sizeof(ic)/sizeof(ic[0]);
    r = rotate(Iter(ic), Iter(ic), Sent(ic+sc));
    CHECK(base(r.begin()) == ic+sc);
    CHECK(base(r.end()) == ic+sc);
    CHECK(ic[0] == 0);
    CHECK(ic[1] == 1);
    CHECK(ic[2] == 2);
    r = rotate(Iter(ic), Iter(ic+1), Sent(ic+sc));
    CHECK(base(r.begin()) == ic+2);
    CHECK(base(r.end()) == ic+sc);
    CHECK(ic[0] == 1);
    CHECK(ic[1] == 2);
    CHECK(ic[2] == 0);
    r = rotate(Iter(ic), Iter(ic+2), Sent(ic+sc));
    CHECK(base(r.begin()) == ic+1);
    CHECK(base(r.end()) == ic+sc);
    CHECK(ic[0] == 0);
    CHECK(ic[1] == 1);
    CHECK(ic[2] == 2);
    r = rotate(Iter(ic), Iter(ic+sc), Sent(ic+sc));
    CHECK(base(r.begin()) == ic);
    CHECK(base(r.end()) == ic+sc);
    CHECK(ic[0] == 0);
    CHECK(ic[1] == 1);
    CHECK(ic[2] == 2);

    int id[] = {0, 1, 2, 3};
    const unsigned sd = sizeof(id)/sizeof(id[0]);
    r = rotate(Iter(id), Iter(id), Sent(id+sd));
    CHECK(base(r.begin()) == id+sd);
    CHECK(base(r.end()) == id+sd);
    CHECK(id[0] == 0);
    CHECK(id[1] == 1);
    CHECK(id[2] == 2);
    CHECK(id[3] == 3);
    r = rotate(Iter(id), Iter(id+1), Sent(id+sd));
    CHECK(base(r.begin()) == id+3);
    CHECK(base(r.end()) == id+sd);
    CHECK(id[0] == 1);
    CHECK(id[1] == 2);
    CHECK(id[2] == 3);
    CHECK(id[3] == 0);
    r = rotate(Iter(id), Iter(id+2), Sent(id+sd));
    CHECK(base(r.begin()) == id+2);
    CHECK(base(r.end()) == id+sd);
    CHECK(id[0] == 3);
    CHECK(id[1] == 0);
    CHECK(id[2] == 1);
    CHECK(id[3] == 2);
    r = rotate(Iter(id), Iter(id+3), Sent(id+sd));
    CHECK(base(r.begin()) == id+1);
    CHECK(base(r.end()) == id+sd);
    CHECK(id[0] == 2);
    CHECK(id[1] == 3);
    CHECK(id[2] == 0);
    CHECK(id[3] == 1);
    r = rotate(Iter(id), Iter(id+sd), Sent(id+sd));
    CHECK(base(r.begin()) == id);
    CHECK(base(r.end()) == id+sd);
    CHECK(id[0] == 2);
    CHECK(id[1] == 3);
    CHECK(id[2] == 0);
    CHECK(id[3] == 1);

    int ie[] = {0, 1, 2, 3, 4};
    const unsigned se = sizeof(ie)/sizeof(ie[0]);
    r = rotate(Iter(ie), Iter(ie), Sent(ie+se));
    CHECK(base(r.begin()) == ie+se);
    CHECK(base(r.end()) == ie+se);
    CHECK(ie[0] == 0);
    CHECK(ie[1] == 1);
    CHECK(ie[2] == 2);
    CHECK(ie[3] == 3);
    CHECK(ie[4] == 4);
    r = rotate(Iter(ie), Iter(ie+1), Sent(ie+se));
    CHECK(base(r.begin()) == ie+4);
    CHECK(base(r.end()) == ie+se);
    CHECK(ie[0] == 1);
    CHECK(ie[1] == 2);
    CHECK(ie[2] == 3);
    CHECK(ie[3] == 4);
    CHECK(ie[4] == 0);
    r = rotate(Iter(ie), Iter(ie+2), Sent(ie+se));
    CHECK(base(r.begin()) == ie+3);
    CHECK(base(r.end()) == ie+se);
    CHECK(ie[0] == 3);
    CHECK(ie[1] == 4);
    CHECK(ie[2] == 0);
    CHECK(ie[3] == 1);
    CHECK(ie[4] == 2);
    r = rotate(Iter(ie), Iter(ie+3), Sent(ie+se));
    CHECK(base(r.begin()) == ie+2);
    CHECK(base(r.end()) == ie+se);
    CHECK(ie[0] == 1);
    CHECK(ie[1] == 2);
    CHECK(ie[2] == 3);
    CHECK(ie[3] == 4);
    CHECK(ie[4] == 0);
    r = rotate(Iter(ie), Iter(ie+4), Sent(ie+se));
    CHECK(base(r.begin()) == ie+1);
    CHECK(base(r.end()) == ie+se);
    CHECK(ie[0] == 0);
    CHECK(ie[1] == 1);
    CHECK(ie[2] == 2);
    CHECK(ie[3] == 3);
    CHECK(ie[4] == 4);
    r = rotate(Iter(ie), Iter(ie+se), Sent(ie+se));
    CHECK(base(r.begin()) == ie);
    CHECK(base(r.end()) == ie+se);
    CHECK(ie[0] == 0);
    CHECK(ie[1] == 1);
    CHECK(ie[2] == 2);
    CHECK(ie[3] == 3);
    CHECK(ie[4] == 4);

    int ig[] = {0, 1, 2, 3, 4, 5};
    const unsigned sg = sizeof(ig)/sizeof(ig[0]);
    r = rotate(Iter(ig), Iter(ig), Sent(ig+sg));
    CHECK(base(r.begin()) == ig+sg);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 0);
    CHECK(ig[1] == 1);
    CHECK(ig[2] == 2);
    CHECK(ig[3] == 3);
    CHECK(ig[4] == 4);
    CHECK(ig[5] == 5);
    r = rotate(Iter(ig), Iter(ig+1), Sent(ig+sg));
    CHECK(base(r.begin()) == ig+5);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 1);
    CHECK(ig[1] == 2);
    CHECK(ig[2] == 3);
    CHECK(ig[3] == 4);
    CHECK(ig[4] == 5);
    CHECK(ig[5] == 0);
    r = rotate(Iter(ig), Iter(ig+2), Sent(ig+sg));
    CHECK(base(r.begin()) == ig+4);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 3);
    CHECK(ig[1] == 4);
    CHECK(ig[2] == 5);
    CHECK(ig[3] == 0);
    CHECK(ig[4] == 1);
    CHECK(ig[5] == 2);
    r = rotate(Iter(ig), Iter(ig+3), Sent(ig+sg));
    CHECK(base(r.begin()) == ig+3);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 0);
    CHECK(ig[1] == 1);
    CHECK(ig[2] == 2);
    CHECK(ig[3] == 3);
    CHECK(ig[4] == 4);
    CHECK(ig[5] == 5);
    r = rotate(Iter(ig), Iter(ig+4), Sent(ig+sg));
    CHECK(base(r.begin()) == ig+2);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 4);
    CHECK(ig[1] == 5);
    CHECK(ig[2] == 0);
    CHECK(ig[3] == 1);
    CHECK(ig[4] == 2);
    CHECK(ig[5] == 3);
    r = rotate(Iter(ig), Iter(ig+5), Sent(ig+sg));
    CHECK(base(r.begin()) == ig+1);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 3);
    CHECK(ig[1] == 4);
    CHECK(ig[2] == 5);
    CHECK(ig[3] == 0);
    CHECK(ig[4] == 1);
    CHECK(ig[5] == 2);
    r = rotate(Iter(ig), Iter(ig+sg), Sent(ig+sg));
    CHECK(base(r.begin()) == ig);
    CHECK(base(r.end()) == ig+sg);
    CHECK(ig[0] == 3);
    CHECK(ig[1] == 4);
    CHECK(ig[2] == 5);
    CHECK(ig[3] == 0);
    CHECK(ig[4] == 1);
    CHECK(ig[5] == 2);
}

constexpr bool test_constexpr()
{
    int rgi[] = {0, 1, 2, 3, 4, 5};
    auto r = ranges::rotate(rgi, rgi + 2);
    STATIC_CHECK_RETURN(r.begin() == rgi + 4);
    STATIC_CHECK_RETURN(r.end() == ranges::end(rgi));
    STATIC_CHECK_RETURN(rgi[0] == 2);
    STATIC_CHECK_RETURN(rgi[1] == 3);
    STATIC_CHECK_RETURN(rgi[2] == 4);
    STATIC_CHECK_RETURN(rgi[3] == 5);
    STATIC_CHECK_RETURN(rgi[4] == 0);
    STATIC_CHECK_RETURN(rgi[5] == 1);
    return true;
}

int main()
{
    test<ForwardIterator<int *>>();
    test<BidirectionalIterator<int *>>();
    test<RandomAccessIterator<int *>>();

    test<ForwardIterator<int *>, Sentinel<int*>>();
    test<BidirectionalIterator<int *>, Sentinel<int*>>();
    test<RandomAccessIterator<int *>, Sentinel<int*>>();

    // test rvalue ranges
    {
        int rgi[] = {0,1,2,3,4,5};
        auto r = ranges::rotate(ranges::views::all(rgi), rgi+2);
        CHECK(r.begin() == rgi+4);
        CHECK(r.end() == ranges::end(rgi));
        CHECK(rgi[0] == 2);
        CHECK(rgi[1] == 3);
        CHECK(rgi[2] == 4);
        CHECK(rgi[3] == 5);
        CHECK(rgi[4] == 0);
        CHECK(rgi[5] == 1);
    }
    {
        int rgi[] = {0,1,2,3,4,5};
        auto r = ranges::rotate(std::move(rgi), rgi+2);
#ifndef RANGES_WORKAROUND_MSVC_573728
        CHECK(::is_dangling(r));
#endif // RANGES_WORKAROUND_MSVC_573728
        CHECK(rgi[0] == 2);
        CHECK(rgi[1] == 3);
        CHECK(rgi[2] == 4);
        CHECK(rgi[3] == 5);
        CHECK(rgi[4] == 0);
        CHECK(rgi[5] == 1);
    }
    {
        std::vector<int> rgi{0,1,2,3,4,5};
        auto r = ranges::rotate(std::move(rgi), rgi.begin()+2);
        CHECK(::is_dangling(r));
        CHECK(rgi[0] == 2);
        CHECK(rgi[1] == 3);
        CHECK(rgi[2] == 4);
        CHECK(rgi[3] == 5);
        CHECK(rgi[4] == 0);
        CHECK(rgi[5] == 1);
    }

    {
        STATIC_CHECK(test_constexpr());
    }

    return ::test_result();
}
