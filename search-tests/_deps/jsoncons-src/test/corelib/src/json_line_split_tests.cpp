// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>

using namespace jsoncons;
using namespace jsoncons::literals;

bool are_equal(const std::string& s1, const std::string& s2)
{
    std::size_t len1 = s1.size();
    std::size_t len2 = s2.size();

    std::size_t len = std::min(len1,len2);

    for (std::size_t i = 0; i < len; ++i)
    {
        if (s1[i] != s2[i])
        {
            for (std::size_t j = 0; j <= i; ++j)
            {
                std::cout << s1[j];
            }
            std::cout << "|";
            std::cout << "\n";
            std::cout << i << " s1: " << s1[i] << ", " << (int)s1[i] << " s2: " << s2[i] << ", " << (int)s2[i] << "\n";
            std::cout << s1 << "\n";
            std::cout << "---\n";
            std::cout << s2 << "\n";
            return false;
        }
    }
    return true;
}

TEST_CASE("json_encoder line split tests")
{
    json val = json::parse(R"(
    {
        "header" : {"properties": {}},
        "data":
        {
            "tags" : [],
            "id" : [1,2,3],
            "item": [[1,2,3]]    
        }
    }
)");

    SECTION("Default line splits")
    {
std::string expected = R"({
    "data": {
        "id": [1,2,3],
        "item": [
            [1,2,3]
        ],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .object_array_line_splits(line_split_kind::same_line)
            .array_array_line_splits(line_split_kind::new_line);
        std::ostringstream os;
        os << pretty_print(val, options);
        CHECK(expected == os.str());
    }

    SECTION("array_array same_line")
    {
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .object_array_line_splits(line_split_kind::same_line)
            .array_array_line_splits(line_split_kind::same_line);
    std::string expected = R"({
    "data": {
        "id": [1,2,3],
        "item": [[1,2,3]],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        std::ostringstream os;
        os << pretty_print(val,options);

        //std::cout << os.str() << "\n";
        CHECK(expected == os.str());
    }

    SECTION("array_array new_line")
    {
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .array_array_line_splits(line_split_kind::new_line)
            .object_array_line_splits(line_split_kind::same_line);
    std::string expected = R"({
    "data": {
        "id": [1,2,3],
        "item": [
            [1,2,3]
        ],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        std::ostringstream os;
        os << pretty_print(val,options);

        //std::cout << os.str() << "\n";
        CHECK(expected == os.str());
    }

    SECTION("array_array multi_line")
    {
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .array_array_line_splits(line_split_kind::multi_line)
            .object_array_line_splits(line_split_kind::same_line);
    std::string expected = R"({
    "data": {
        "id": [1,2,3],
        "item": [
            [
                1,
                2,
                3
            ]
        ],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        std::ostringstream os;
        os << pretty_print(val,options);
        //std::cout << os.str() << "\n";
        CHECK(expected == os.str());
    }

    SECTION("object_array same_line")
    {
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .object_array_line_splits(line_split_kind::same_line)
            .array_array_line_splits(line_split_kind::new_line);
    std::string expected = R"({
    "data": {
        "id": [1,2,3],
        "item": [
            [1,2,3]
        ],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        std::ostringstream os;
        os << pretty_print(val,options);
        //std::cout << os.str() << "\n";
        CHECK(expected == os.str());
    }

    SECTION("object_array new_line")
    {
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .object_array_line_splits(line_split_kind::new_line)
            .array_array_line_splits(line_split_kind::new_line);
    std::string expected = R"({
    "data": {
        "id": [
            1,2,3
        ],
        "item": [
            [1,2,3]
        ],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        std::ostringstream os;
        os << pretty_print(val,options);
        //std::cout << os.str() << "\n";
        CHECK(expected == os.str());
    }

    SECTION("")
    {
        auto options = json_options{}
            .spaces_around_comma(spaces_option::no_spaces)
            .object_array_line_splits(line_split_kind::multi_line)
            .array_array_line_splits(line_split_kind::same_line);
    std::string expected = R"({
    "data": {
        "id": [
            1,
            2,
            3
        ],
        "item": [
            [1,2,3]
        ],
        "tags": []
    },
    "header": {
        "properties": {}
    }
})";
        std::ostringstream os;
        os << pretty_print(val,options);
        //std::cout << os.str() << "\n";
        CHECK(expected == os.str());
    }
}

// array_array_line_splits_(line_split_kind::new_line)

TEST_CASE("test_array_of_array_of_string_string_array")
{
    json j = R"(
[
    ["NY","LON",
        ["TOR","LON"]
    ]
]
    )"_json;

    //std::cout << pretty_print(j) << '\n';
}


