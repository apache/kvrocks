// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/jsonpatch/jsonpatch.hpp>
#include <jsoncons/json.hpp>

using jsoncons::json;
namespace jsonpatch = jsoncons::jsonpatch;

void jsonpatch_add_add()
{
    // Apply a JSON Patch

    json doc = json::parse(R"(
        { "foo": "bar"}
    )");

    json doc2 = doc;

    json patch = json::parse(R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] }
        ]
    )");

    std::error_code ec;
    jsonpatch::apply_patch(doc, patch, ec);

    std::cout << "(1)\n" << pretty_print(doc) << '\n';

    // Create a JSON Patch

    auto patch2 = jsonpatch::from_diff(doc2,doc);

    std::cout << "(2)\n" << pretty_print(patch2) << '\n';

    jsonpatch::apply_patch(doc2,patch2,ec);

    std::cout << "(3)\n" << pretty_print(doc2) << '\n';
}

void jsonpatch_add_add_add_failed1()
{
    json target = json::parse(R"(
        { "foo": "bar"}
    )");

    json patch = json::parse(R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )");

    try
    {
        jsonpatch::apply_patch(target, patch);
    }
    catch (const jsonpatch::jsonpatch_error& e)
    {
        std::cout << "(1) " << e.what() << '\n';
        std::cout << "(2) " << target << '\n';
    }
}

void jsonpatch_add_add_add_failed2()
{
    json target = json::parse(R"(
        { "foo": "bar"}
    )");

    json patch = json::parse(R"(
        [
            { "op": "add", "path": "/baz", "value": "qux" },
            { "op": "add", "path": "/foo", "value": [ "bar", "baz" ] },
            { "op": "add", "path": "/baz/bat", "value": "qux" } // nonexistent target
        ]
    )");

    std::error_code ec;
    jsonpatch::apply_patch(target, patch, ec);

    std::cout << "(1) " << std::error_code(ec).message() << '\n';
    std::cout << "(2) " << target << '\n';
}

void create_a_json_patch()
{
    json source = json::parse(R"(
        {"/": 9, "foo": "bar"}
    )");

    json target = json::parse(R"(
        { "baz":"qux", "foo": [ "bar", "baz" ]}
    )");

    auto patch = jsonpatch::from_diff(source, target);

    std::error_code ec;
    jsonpatch::apply_patch(source, patch, ec);

    std::cout << "(1)\n" << pretty_print(patch) << '\n';
    std::cout << "(2)\n" << pretty_print(source) << '\n';
}

int main()
{
    std::cout << "\njsonpatch examples\n\n";
    create_a_json_patch();
    jsonpatch_add_add();
    jsonpatch_add_add_add_failed2();
    jsonpatch_add_add_add_failed1();
    std::cout << '\n';
}

