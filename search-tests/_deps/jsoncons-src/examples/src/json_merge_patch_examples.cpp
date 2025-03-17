// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/mergepatch/mergepatch.hpp>
#include <jsoncons/json.hpp>

using jsoncons::json;
namespace mergepatch = jsoncons::mergepatch;

void apply_json_merge_patch()
{
    // Apply a JSON Patch

    json doc = json::parse(R"(
{
         "title": "Goodbye!",
         "author" : {
       "givenName" : "John",
       "familyName" : "Doe"
         },
         "tags":[ "example", "sample" ],
         "content": "This will be unchanged"
}
    )");

    json doc2 = doc;

    json patch = json::parse(R"(
{
         "title": "Hello!",
         "phoneNumber": "+01-123-456-7890",
         "author": {
       "familyName": null
         },
         "tags": [ "example" ]
}
    )");

    mergepatch::apply_merge_patch(doc, patch);

    std::cout << "(1)\n" << pretty_print(doc) << '\n';

    // Create a JSON Patch

    auto patch2 = mergepatch::from_diff(doc2,doc);

    std::cout << "(2)\n" << pretty_print(patch2) << '\n';

    mergepatch::apply_merge_patch(doc2,patch2);

    std::cout << "(3)\n" << pretty_print(doc2) << '\n';
}

void create_json_merge_patch()
{
    json source = json::parse(R"(
{
         "title": "Goodbye!",
         "author" : {
       "givenName" : "John",
       "familyName" : "Doe"
         },
         "tags":[ "example", "sample" ],
         "content": "This will be unchanged"
}
    )");

    json target = json::parse(R"(
{
  "title": "Hello!",
  "author": {
    "givenName": "John"
  },
  "tags": [
    "example"
  ],
  "content": "This will be unchanged",
  "phoneNumber": "\u002B01-123-456-7890"
}
    )");

    auto patch = mergepatch::from_diff(source, target);

    mergepatch::apply_merge_patch(source, patch);

    std::cout << "(1)\n" << pretty_print(patch) << '\n';
    std::cout << "(2)\n" << pretty_print(source) << '\n';
}

int main()
{
    std::cout << "\njson_merge_patch examples\n\n";
    create_json_merge_patch();
    apply_json_merge_patch();
    std::cout << '\n';
}

