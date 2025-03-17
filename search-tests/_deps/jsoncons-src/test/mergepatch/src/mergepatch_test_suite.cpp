// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <iostream>

#include <jsoncons_ext/mergepatch/mergepatch.hpp>

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
#include <catch/catch.hpp>

using namespace jsoncons;

void json_merge_patch_tests(const std::string& fpath)
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
                mergepatch::apply_merge_patch(target, patch);
                const json& expected = test_case["result"];
                if (target == expected)
                {
                    json target2 = test_group.at("given");
                    json patch2 = mergepatch::from_diff(target2, target);
                    mergepatch::apply_merge_patch(target2, patch2);
                    if (target2 != target)
                    {
                        if (test_case.contains("comment"))
                        {
                            std::cout << "\n" << test_case["comment"] << "\n";
                        }
                        std::cout << "Source: " << pretty_print(test_group.at("given")) << "\n\n";
                        std::cout << "Target: " << pretty_print(target) << "\n\n";
                        std::cout << "Diff: " << pretty_print(patch2) << "\n\n";
                        std::cout << "Result: " << pretty_print(target2) << "\n\n";
                    }
                    CHECK(target2 == target);
                }
                else 
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

TEST_CASE("mergepatch tests")
{
    SECTION("compliance")
    {
        json_merge_patch_tests("./mergepatch/input/compliance/rfc7396-test-cases.json");
        //json_merge_patch_tests("./mergepatch/input/compliance/test.json");
    }
}

