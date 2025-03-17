// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/json_query.hpp>

#include <jsoncons/json.hpp>

#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <unordered_set> // std::unordered_set
#include <fstream>
#include <catch/catch.hpp>

using json = jsoncons::json;
namespace jsonpath = jsoncons::jsonpath;

TEST_CASE("jsonpath custom function test")
{
    json root;
    JSONCONS_TRY
    {
        root = json::parse(R"({ "foo": 60,"bar": 10 })");
    }
    JSONCONS_CATCH (const jsoncons::ser_error& e)
    {
        std::cout << e.what() << '\n';
    }

    jsonpath::custom_functions<json> functions;
    functions.register_function("divide", // function name
         2,        // number of arguments   
         [](jsoncons::span<const jsonpath::parameter<json>> params, std::error_code& ec) -> json 
         {
            if (!(params[0].value().is_number() && params[1].value().is_number())) 
            {
                ec = jsonpath::jsonpath_errc::invalid_type; 
                return json::null();
            }
            return json(params[0].value().as<double>() / params[1].value().as<double>());
         }
    );

    SECTION("test 1")
    {
        auto expr = jsonpath::make_expression<json>("divide(@.foo, @.bar)", functions);
        auto r = expr.evaluate(root);
        REQUIRE(!r.empty());
        CHECK(r[0] == json(6));
    }

    SECTION("test 2")
    {
        auto r = jsonpath::json_query(root, "divide($.foo, $.bar)", jsonpath::result_options(), functions);
        REQUIRE(!r.empty());
        CHECK(r[0] == json(6));
    }

    SECTION("test 3")
    {
        json r;
        jsonpath::json_query(root, "divide($.foo, $.bar)", 
            [&](const std::string&, const json& val) {r = val; },
                             jsonpath::result_options(), functions);
        CHECK(r == json(6));
    }
}

