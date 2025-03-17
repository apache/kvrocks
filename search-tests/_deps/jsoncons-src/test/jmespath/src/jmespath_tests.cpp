// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jmespath/jmespath.hpp>
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

using namespace jsoncons;

void jmespath_tests(const std::string& fpath)
{
    std::fstream is(fpath);
    REQUIRE(is); //-V521

    json tests = json::parse(is);
    for (const auto& test_group : tests.array_range())
    {
        const json& root = test_group["given"];

        for (const auto& test_case : test_group["cases"].array_range())
        {
            std::string expr = test_case["expression"].as<std::string>();
            //std::cout << (fpath + "-" + expr) << "\n";
            try
            {
                json actual = jmespath::search(root, expr);
                if (test_case.contains("result"))
                {
                    const json& expected = test_case["result"];
                    if (actual != expected)
                    {
                        if (test_case.contains("comment"))
                        {
                            std::cout << "\n" << test_case["comment"] << "\n";
                        }
                        std::cout << "Input:\n" << pretty_print(root) << "\n\n";
                        std::cout << "Expression: " << expr << "\n\n";
                        std::cout << "Actual: " << pretty_print(actual) << "\n\n";
                        std::cout << "Expected: " << pretty_print(expected) << "\n\n";
                    }
                    CHECK(expected == actual); //-V521
                }
                else if (test_case.contains("error"))
                {
                    if (test_case.contains("comment"))
                    {
                        std::cout << "Comment: " << test_case["comment"] << "\n";
                    }
                    std::cout << "Error: " << test_case["error"] << "\n\n";
                    std::cout << "Input:\n" << pretty_print(root) << "\n\n";
                    std::cout << "Expression: " << expr << "\n\n";
                    std::cout << "Actual: " << pretty_print(actual) << "\n\n";
                    CHECK(false); //-V521
                }

            }
            catch (const std::exception& e)
            {
                if (test_case.contains("result"))
                {
                    const json& expected = test_case["result"];
                    std::cout << e.what() << "\n";
                    if (test_case.contains("comment"))
                    {
                        std::cout << "Comment: " << test_case["comment"] << "\n\n";
                    }
                    std::cout << "Input\n" << pretty_print(root) << "\n\n";
                    std::cout << "Expression: " << expr << "\n\n";
                    std::cout << "Expected: " << expected << "\n\n";
                    CHECK(false); //-V521
                }
            }
        }
    }
}

TEST_CASE("jmespath-tests")
{
    SECTION("Examples and tutorials")
    {
        jmespath_tests("./jmespath/input/examples/jmespath-examples.json"); 
    }
    SECTION("Issues")
    {
        jmespath_tests("./jmespath/input/issues/issues.json"); 
    }
    SECTION("compliance")
    {
        jmespath_tests("./jmespath/input/compliance/syntax.json"); // OK
        jmespath_tests("./jmespath/input/compliance/basic.json"); // OK
        jmespath_tests("./jmespath/input/compliance/boolean.json"); // OK
        jmespath_tests("./jmespath/input/compliance/current.json"); // OK
        jmespath_tests("./jmespath/input/compliance/escape.json"); // OK
        jmespath_tests("./jmespath/input/compliance/filters.json"); // OK
        jmespath_tests("./jmespath/input/compliance/identifiers.json"); // OK
        jmespath_tests("./jmespath/input/compliance/indices.json");  // OK
        jmespath_tests("./jmespath/input/compliance/literal.json"); // OK
        jmespath_tests("./jmespath/input/compliance/multiselect.json"); // OK 
        jmespath_tests("./jmespath/input/compliance/pipe.json"); // OK
        jmespath_tests("./jmespath/input/compliance/slice.json"); // OK
        jmespath_tests("./jmespath/input/compliance/unicode.json"); // OK
        jmespath_tests("./jmespath/input/compliance/wildcard.json"); // OK
        jmespath_tests("./jmespath/input/compliance/benchmarks.json"); // OK
        jmespath_tests("./jmespath/input/compliance/functions.json"); // OK
    }
}

