// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
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

void jsonpatch_tests(const std::string& fpath)
{
    std::fstream is(fpath);
    if (!is)
    {
        std::cerr << "Cannot open " << fpath << "\n";
        exit(1);
    }

    json tests = json::parse(is);
    for (const auto& test_group : tests.array_range())
    {
        for (const auto& test_case : test_group["cases"].array_range())
        {
            const json& patch = test_case["patch"];
            if (test_case.contains("result"))
            {
                json target = test_group.at("given");
                std::error_code ec;
                jsonpatch::apply_patch(target, patch, ec);
                const json& expected = test_case["result"];
                if (target != expected)
                {
                    if (test_case.contains("comment"))
                    {
                        std::cout << "\n" << test_case["comment"] << "\n";
                    }
                    std::cout << "Input: " << pretty_print(test_group.at("given")) << "\n\n";
                    std::cout << "Patch: " << pretty_print(patch) << "\n\n";
                    std::cout << "Target: " << pretty_print(target) << "\n\n";
                    std::cout << "Expected: " << pretty_print(expected) << "\n\n";
                }
                CHECK(expected == target); //-V521
            }
            else if (test_case.contains("error"))
            {
                json target = test_group.at("given");
                std::error_code ec;
                jsonpatch::apply_patch(target, patch, ec);
                CHECK(ec);
                const json& expected = test_group.at("given");
                if (target != expected)
                {
                    if (test_case.contains("comment"))
                    {
                        std::cout << "\n" << test_case["comment"] << "\n";
                    }
                    std::cout << "Input: " << pretty_print(test_group.at("given")) << "\n\n";
                    std::cout << "Patch: " << pretty_print(patch) << "\n\n";
                    std::cout << "Target: " << pretty_print(target) << "\n\n";
                    std::cout << "Expected: " << pretty_print(expected) << "\n\n";
                }
                CHECK(expected == target); //-V521
            }
        }
    }
}

TEST_CASE("jsonpatch tests")
{
    SECTION("compliance")
    {
        jsonpatch_tests("./jsonpatch/input/compliance/rfc6902-examples.json");
        //jsonpatch_tests("./jsonpatch/input/compliance/test.json");
    }
}

