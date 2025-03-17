// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <sstream>
#include <jsoncons/json.hpp>
#include <fstream>
#include <iostream>
#include <locale>

#if (defined JSONCONS_HAS_FILESYSTEM && defined(_MSC_VER))
#include <filesystem>
namespace fs = std::filesystem;
#endif

using namespace jsoncons;

#if (defined JSONCONS_HAS_FILESYSTEM && defined(_MSC_VER))
TEST_CASE("JSON Parsing Test Suite")
{
    SECTION("Expected success")
    {
        std::string path = "./corelib/input/JSONTestSuite";
        for (auto& p : fs::directory_iterator(path))
        {
            if (fs::exists(p) && fs::is_regular_file(p) && p.path().extension() == ".json" && p.path().filename().c_str()[0] == 'y')
            {
                std::ifstream is(p.path().c_str());
                auto options = json_options{}.allow_comments(false);
                json_stream_reader reader(is, options);
                std::error_code ec;
                reader.read(ec);
                if (ec)
                {
                    std::cout << p.path().filename().string() << " failed, expected success\n";
                }
                CHECK_FALSE(ec);                        
            }
        }
    }
    SECTION("Expected failure")
    {
        std::string path = "./corelib/input/JSONTestSuite";
        for (auto& p : fs::directory_iterator(path))
        {
            if (fs::exists(p) && fs::is_regular_file(p) && p.path().extension() == ".json" && p.path().filename().c_str()[0] == 'n')
            {
                std::ifstream is(p.path().c_str());
                auto options = json_options{}.allow_comments(false);
                json_stream_reader reader(is, options);
                std::error_code ec;
                reader.read(ec);
                if (!ec)
                {
                    std::cout << p.path().filename().string() << " succeeded, expected failure\n";
                }
                // TODO: revisit
                // CHECK(ec); 
            }
        }
    }
}
#endif




