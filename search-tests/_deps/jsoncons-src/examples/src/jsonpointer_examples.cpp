// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons/json.hpp>

#include <cassert>

// for brevity
using jsoncons::json; 
namespace jsonpointer = jsoncons::jsonpointer;

void jsonpointer_select_RFC6901()
{
    // Example from RFC 6901
    auto j = json::parse(R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )");

    try
    {
        const json& result1 = jsonpointer::get(j, "");
        std::cout << "(1) " << result1 << '\n';
        const json& result2 = jsonpointer::get(j, "/foo");
        std::cout << "(2) " << result2 << '\n';
        const json& result3 = jsonpointer::get(j, "/foo/0");
        std::cout << "(3) " << result3 << '\n';
        const json& result4 = jsonpointer::get(j, "/");
        std::cout << "(4) " << result4 << '\n';
        const json& result5 = jsonpointer::get(j, "/a~1b");
        std::cout << "(5) " << result5 << '\n';
        const json& result6 = jsonpointer::get(j, "/c%d");
        std::cout << "(6) " << result6 << '\n';
        const json& result7 = jsonpointer::get(j, "/e^f");
        std::cout << "(7) " << result7 << '\n';
        const json& result8 = jsonpointer::get(j, "/g|h");
        std::cout << "(8) " << result8 << '\n';
        const json& result9 = jsonpointer::get(j, "/i\\j");
        std::cout << "(9) " << result9 << '\n';
        const json& result10 = jsonpointer::get(j, "/k\"l");
        std::cout << "(10) " << result10 << '\n';
        const json& result11 = jsonpointer::get(j, "/ ");
        std::cout << "(11) " << result11 << '\n';
        const json& result12 = jsonpointer::get(j, "/m~0n");
        std::cout << "(12) " << result12 << '\n';
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cerr << e.what() << '\n';
    }
}

void jsonpointer_contains()
{
    // Example from RFC 6901
    auto j = json::parse(R"(
       {
          "foo": ["bar", "baz"],
          "": 0,
          "a/b": 1,
          "c%d": 2,
          "e^f": 3,
          "g|h": 4,
          "i\\j": 5,
          "k\"l": 6,
          " ": 7,
          "m~n": 8
       }
    )");

    std::cout << "(1) " << jsonpointer::contains(j, "/foo/0") << '\n';
    std::cout << "(2) " << jsonpointer::contains(j, "e^g") << '\n';
}

void jsonpointer_select_author()
{
    auto j = json::parse(R"(
    [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ]
    )");

    // Using exceptions to report errors
    try
    {
        json result = jsonpointer::get(j, "/1/author");
        std::cout << "(1) " << result << '\n';
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cout << e.what() << '\n';
    }

    // Using error codes to report errors
    std::error_code ec;
    const json& result = jsonpointer::get(j, "/0/title", ec);

    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << "(2) " << result << '\n';
    }
}

void jsonpointer_add_member_to_object()
{
    auto target = json::parse(R"(
        { "foo": "bar"}
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_add_element_to_array()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/foo/1", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_add_element_to_end_array()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/foo/-", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_insert_name_exists()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "abc"}
    )");

    std::error_code ec;
    jsonpointer::add_if_absent(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_add_element_outside_range()
{
    auto target = json::parse(R"(
    { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::add(target, "/foo/3", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_insert_or_assign_name_exists()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "abc"}
    )");

    std::error_code ec;
    jsonpointer::add(target, "/baz", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_remove_object_member()
{
    auto target = json::parse(R"(
        { "foo": "bar", "baz" : "qux"}
    )");

    std::error_code ec;
    jsonpointer::remove(target, "/baz", ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_remove_array_element()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "qux", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::remove(target, "/foo/1", ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_replace_object_value()
{
    auto target = json::parse(R"(
        {
          "baz": "qux",
          "foo": "bar"
        }
    )");

    std::error_code ec;
    jsonpointer::replace(target, "/baz", json("boo"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << target << '\n';
    }
}

void jsonpointer_replace_array_value()
{
    auto target = json::parse(R"(
        { "foo": [ "bar", "baz" ] }
    )");

    std::error_code ec;
    jsonpointer::replace(target, "/foo/1", json("qux"), ec);
    if (ec)
    {
        std::cout << ec.message() << '\n';
    }
    else
    {
        std::cout << pretty_print(target) << '\n';
    }
}

void jsonpointer_error_example()
{
    auto j = json::parse(R"(
    [
      { "category": "reference",
        "author": "Nigel Rees",
        "title": "Sayings of the Century",
        "price": 8.95
      },
      { "category": "fiction",
        "author": "Evelyn Waugh",
        "title": "Sword of Honour",
        "price": 12.99
      }
    ]
    )");

    try
    {
        json result = jsonpointer::get(j, "/1/isbn");
        std::cout << "succeeded?" << '\n';
        std::cout << result << '\n';
    }
    catch (const jsonpointer::jsonpointer_error& e)
    {
        std::cout << "Caught jsonpointer_error with category " << e.code().category().name() 
                  << ", code " << e.code().value() 
                  << " and message \"" << e.what() << "\"" << '\n';
    }
}

void jsonpointer_get_examples()
{
    {
        json j(jsoncons::json_array_arg, {"baz","foo"});
        json& item = jsonpointer::get(j,"/0");
        std::cout << "(1) " << item << '\n';
    }
    {
        const json j(jsoncons::json_array_arg, {"baz","foo"});
        const json& item = jsonpointer::get(j,"/1");
        std::cout << "(2) " << item << '\n';
    }
    {
        json j(jsoncons::json_array_arg, {"baz","foo"});

        std::error_code ec;
        json& item = jsonpointer::get(j,"/1",ec);
        std::cout << "(4) " << item << '\n';
    }
    {
        const json j(jsoncons::json_array_arg, {"baz","foo"});

        std::error_code ec;
        const json& item = jsonpointer::get(j,"/0",ec);
        std::cout << "(5) " << item << '\n';
    }
}

void jsonpointer_address_example()
{
    auto j = json::parse(R"(
       {
          "a/b": ["bar", "baz"],
          "m~n": ["foo", "qux"]
       }
    )");

    jsonpointer::json_pointer ptr;
    ptr /= "m~n";
    ptr /= "1";

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& item : ptr)
    {
        std::cout << item << "\n";
    }
    std::cout << "\n";

    json item = jsonpointer::get(j, ptr);
    std::cout << "(3) " << item << "\n";
}

void jsonpointer_address_iterator_example()
{
    jsonpointer::json_pointer ptr("/store/book/1/author");

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : ptr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}

void jsonpointer_address_append_tokens()
{
    jsonpointer::json_pointer ptr;

    ptr /= "a/b";
    ptr /= "";
    ptr /= "m~n";

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : ptr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}

void jsonpointer_address_concatenate()
{
    jsonpointer::json_pointer ptr("/a~1b");

    ptr += jsonpointer::json_pointer("//m~0n");

    std::cout << "(1) " << ptr << "\n\n";

    std::cout << "(2)\n";
    for (const auto& token : ptr)
    {
        std::cout << token << "\n";
    }

    std::cout << "\n";
}

void flatten_and_unflatten()
{
    json input = json::parse(R"(
    {
       "application": "hiking",
       "reputons": [
           {
               "rater": "HikingAsylum",
               "assertion": "advanced",
               "rated": "Marilyn C",
               "rating": 0.90
            },
            {
               "rater": "HikingAsylum",
               "assertion": "intermediate",
               "rated": "Hongmin",
               "rating": 0.75
            }    
        ]
    }
    )");

    json flattened = jsonpointer::flatten(input);

    std::cout << pretty_print(flattened) << "\n\n";

    json unflattened = jsonpointer::unflatten(flattened);

    assert(unflattened == input);
}

void flatten_and_unflatten2()
{
    json input = json::parse(R"(
    {
        "discards": {
            "1000": "Record does not exist",
            "1004": "Queue limit exceeded",
            "1010": "Discarding timed-out partial msg"
        },
        "warnings": {
            "0": "Phone number missing country code",
            "1": "State code missing",
            "2": "Zip code missing"
        }
    }
    )");

    json flattened = jsonpointer::flatten(input);
    std::cout << "(1)\n" << pretty_print(flattened) << "\n";

    json unflattened1 = jsonpointer::unflatten(flattened);
    std::cout << "(2)\n" << pretty_print(unflattened1) << "\n";

    json unflattened2 = jsonpointer::unflatten(flattened,
        jsonpointer::unflatten_options::assume_object);
    std::cout << "(3)\n" << pretty_print(unflattened2) << "\n";
}

void get_and_create_if_missing()
{
    std::vector<std::string> keys = {"foo","bar","baz"};

    jsonpointer::json_pointer ptr;
    for (const auto& key : keys)
    {
        ptr /= key;
    }

    json doc;
    json result = jsonpointer::get(doc, ptr, true);

    std::cout << pretty_print(doc) << "\n\n";
}

void add_and_create_if_missing()
{
    std::vector<std::string> keys = {"foo","bar","baz"};

    jsonpointer::json_pointer ptr;
    for (const auto& key : keys)
    {
        ptr /= key;
    }

    json doc;
    jsonpointer::add(doc, ptr, "str", true);

    std::cout << pretty_print(doc) << "\n\n";
}

void add_if_absent_and_create_if_missing()
{
    std::vector<std::string> keys = { "foo","bar","baz" };

    jsonpointer::json_pointer ptr;
    for (const auto& key : keys)
    {
        ptr /= key;
    }

    json doc;
    jsonpointer::add_if_absent(doc, ptr, "str", true);

    std::cout << pretty_print(doc) << "\n\n";
}

void replace_and_create_if_missing()
{
    std::vector<std::string> keys = {"foo","bar","baz"};

    jsonpointer::json_pointer ptr;
    for (const auto& key : keys)
    {
        ptr /= key;
    }

    json doc;
    jsonpointer::replace(doc, ptr, "str", true);

    std::cout << pretty_print(doc) << "\n\n";
}

int main()
{
    std::cout << "\njsonpointer examples\n\n";
    jsonpointer_select_author();
    jsonpointer_address_example();
    jsonpointer_select_RFC6901();
    jsonpointer_add_member_to_object();
    jsonpointer_add_element_to_array();
    jsonpointer_add_element_to_end_array();
    jsonpointer_add_element_outside_range();
    jsonpointer_remove_object_member();
    jsonpointer_remove_array_element();
    jsonpointer_replace_object_value();
    jsonpointer_replace_array_value();
    jsonpointer_contains();
    jsonpointer_error_example();
    jsonpointer_insert_name_exists();
    jsonpointer_insert_or_assign_name_exists();
    jsonpointer_get_examples();
    jsonpointer_address_iterator_example();
    jsonpointer_address_append_tokens();
    jsonpointer_address_concatenate();
    flatten_and_unflatten();
    flatten_and_unflatten2();
    get_and_create_if_missing();
    add_and_create_if_missing();
    add_if_absent_and_create_if_missing();
    replace_and_create_if_missing();
}

