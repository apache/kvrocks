// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>

#include <ctime>
#include <list>
#include <sstream>
#include <utility>
#include <vector>

#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("issue 355 test")
{
    jsoncons::json someObject;
    jsoncons::json::array someArray(4);
    someArray[0] = 0;
    someArray[1] = 1;
    someArray[2] = 2;
    someArray[3] = 3;

    // This will end-up making a copy the array, which I did not expect.
    someObject.insert_or_assign("someKey", std::move(someArray));
}

