// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpath/jsonpath.hpp>
#include <jsoncons/json.hpp>

#include <iostream>
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

void jsonpath_tests(const std::string& fpath)
{
    std::cout << "Test " << fpath << '\n';

    std::fstream is(fpath);
    if (!is)
    {
        std::cerr << "Cannot open " << fpath << "\n";
        exit(1);
    }

    json tests = json::parse(is);
    for (const auto& test_group : tests.array_range())
    {
        const json& instance = test_group["given"];

        for (const auto& test_case : test_group["cases"].array_range())
        {
            std::string expr = test_case["expression"].as<std::string>();
            try
            {
                jsonpath::result_options options = jsonpath::result_options();
                if (test_case.contains("nodups") && test_case.at("nodups").as<bool>())
                {
                    options |= jsonpath::result_options::nodups;
                }
                if (test_case.contains("sort") && test_case.at("sort").as<bool>())
                {
                    options |= jsonpath::result_options::sort;
                }
                auto expression = jsoncons::jsonpath::make_expression<json>(expr);
                if (test_case.contains("result"))
                {
                    jsonpath::result_options rflags = options | jsonpath::result_options::value;
                    json actual = expression.evaluate(instance, rflags);
                    const json& expected = test_case["result"];
                    //std::cout << "actual\n:" << actual << "\n";
                    if (actual != expected)
                    {
                        if (test_case.contains("comment"))
                        {
                            std::cout << "\n" << test_case["comment"] << "\n";
                        }
                        std::cout << "Input:\n" << pretty_print(instance) << "\n\n";
                        std::cout << "Expression: " << expr << "\n\n";
                        std::cout << "Actual: " << pretty_print(actual) << "\n\n";
                        std::cout << "Expected: " << pretty_print(expected) << "\n\n";
                    }
                    CHECK(expected == actual);
                }
                if (test_case.contains("path"))
                {
                    jsonpath::result_options pflags = options | jsonpath::result_options::path;
                    json actual = expression.evaluate(instance, pflags);
                    const json& expected = test_case["path"];
                    //std::cout << "actual\n:" << actual << "\n";
                    if (actual != expected)
                    {
                        if (test_case.contains("comment"))
                        {
                            std::cout << "\n" << test_case["comment"] << "\n";
                        }
                        std::cout << "Input:\n" << pretty_print(instance) << "\n\n";
                        std::cout << "Expression: " << expr << "\n\n";
                        std::cout << "Actual: " << pretty_print(actual) << "\n\n";
                        std::cout << "Expected: " << pretty_print(expected) << "\n\n";
                    }
                    CHECK(expected == actual);
                }
                if (test_case.contains("error"))
                {
                    json actual = expression.evaluate(instance);
                    if (test_case.contains("comment"))
                    {
                        std::cout << "Comment: " << test_case["comment"] << "\n";
                    }
                    std::cout << "Error: " << test_case["error"] << "\n\n";
                    std::cout << "Input:\n" << pretty_print(instance) << "\n\n";
                    std::cout << "Expression: " << expr << "\n\n";
                    std::cout << "Actual: " << pretty_print(actual) << "\n\n";
                    CHECK(false);
                }

            }
            catch (const std::exception& e)
            {
                if (test_case.contains("result"))
                {
                    std::cout << e.what() << "\n";
                    const json& expected = test_case["result"];
                    std::cout << e.what() << "\n";
                    if (test_case.contains("comment"))
                    {
                        std::cout << "Comment: " << test_case["comment"] << "\n\n";
                    }
                    std::cout << "Input\n" << pretty_print(instance) << "\n\n";
                    std::cout << "Expression: " << expr << "\n\n";
                    std::cout << "Expected: " << expected << "\n\n";
                    CHECK(false);
                }
            }
        }
    }
}

TEST_CASE("jsonpath-tests")
{
    SECTION("compliance")
    {
#if defined(JSONCONS_HAS_STD_REGEX)
        jsonpath_tests("./jsonpath/input/test_data/regex.json");
#endif
        jsonpath_tests("./jsonpath/input/test_data/identifiers.json");
        jsonpath_tests("./jsonpath/input/test_data/dot-notation.json"); 
        jsonpath_tests("./jsonpath/input/test_data/indices.json");
        jsonpath_tests("./jsonpath/input/test_data/wildcard.json");
        jsonpath_tests("./jsonpath/input/test_data/recursive-descent.json"); 
        jsonpath_tests("./jsonpath/input/test_data/union.json");       
        jsonpath_tests("./jsonpath/input/test_data/filters.json");
        jsonpath_tests("./jsonpath/input/test_data/functions.json");
        jsonpath_tests("./jsonpath/input/test_data/expressions.json");
        jsonpath_tests("./jsonpath/input/test_data/syntax.json");
        jsonpath_tests("./jsonpath/input/test_data/functions.json");
        jsonpath_tests("./jsonpath/input/test_data/slice.json"); 
        jsonpath_tests("./jsonpath/input/test_data/parent-operator.json"); 
        jsonpath_tests("./jsonpath/input/test.json");
    }
}

