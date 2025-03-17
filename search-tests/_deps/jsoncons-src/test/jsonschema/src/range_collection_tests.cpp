// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons_ext/jsonschema/common/validator.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/utility/byte_string.hpp>

#include <catch/catch.hpp>

using jsoncons::jsonschema::range;
using jsoncons::jsonschema::range_collection;

TEST_CASE("jsonschema range collection tests")
{
    range_collection ranges;
    ranges.insert(range{0,5});
    ranges.insert(range{10,15});
    ranges.insert(range{7,8});

    SECTION("test 1")
    {
        CHECK(ranges.contains(0));
        CHECK(ranges.contains(1));
        CHECK(ranges.contains(2));
        CHECK(ranges.contains(3));
        CHECK(ranges.contains(4));
        CHECK_FALSE(ranges.contains(5));
        CHECK_FALSE(ranges.contains(6));
        CHECK(ranges.contains(7));
        CHECK_FALSE(ranges.contains(8));
        CHECK_FALSE(ranges.contains(9));
        CHECK(ranges.contains(10));
        CHECK(ranges.contains(11));
        CHECK(ranges.contains(12));
        CHECK(ranges.contains(13));
        CHECK(ranges.contains(14));
    }
    SECTION("test 2")
    {
        range_collection coll2;
        for (auto range : ranges)
        {
            coll2.insert(range);
        }
    }
}

