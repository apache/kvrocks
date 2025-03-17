/*
 * Copyright 2015 MongoDB, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons_ext/bson/bson_oid.hpp>

#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <limits>
#include <array>
#include <catch/catch.hpp>

namespace bson = jsoncons::bson;

TEST_CASE("test_bson_oid_init_from_string")
{
    static std::vector<std::string> gTestOids = {"000000000000000000000000",
                                      "010101010101010101010101",
                                      "0123456789abcdefafcdef03",
                                      "fcdeab182763817236817236",
                                      "ffffffffffffffffffffffff",
                                      "eeeeeeeeeeeeeeeeeeeeeeee",
                                      "999999999999999999999999",
                                      "111111111111111111111111"};

    SECTION("test1")
    {
        for (const auto& item : gTestOids) 
        {
           bson::oid_t oid(item);
           std::string s;
           to_string(oid, s);
           CHECK(item == s);
        }
    }
}

