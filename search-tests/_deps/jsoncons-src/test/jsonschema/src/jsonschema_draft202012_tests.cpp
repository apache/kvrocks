// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#include <jsoncons_ext/jsonschema/jsonschema.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/utility/byte_string.hpp>

#include <fstream>
#include <iostream>
#include <regex>
#include <catch/catch.hpp>

using jsoncons::json;
namespace jsonschema = jsoncons::jsonschema;

namespace {
 
    json resolver(const jsoncons::uri& uri)
    {
        //std::cout << uri.string() << ", " << uri.path() << "\n";
        std::string pathname = "./jsonschema/JSON-Schema-Test-Suite/remotes";
        pathname += std::string(uri.path());

        std::fstream is(pathname.c_str());
        if (!is)
        {
            return json::null();
        }

        return json::parse(is);       
    }

    void jsonschema_tests(const std::string& fpath,
        jsonschema::evaluation_options options = jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()))
    {
        std::fstream is(fpath);
        if (!is)
        {
            std::cout << "Cannot open file: " << fpath << "\n";
            return;
        }

        json tests = json::parse(is); 
        //std::cout << pretty_print(tests) << "\n";

        int count = 0;
        for (const auto& test_group : tests.array_range()) 
        {
            ++count;
            try
            {
                jsonschema::json_schema<json> compiled = jsonschema::make_json_schema(test_group.at("schema"), resolver, 
                    options);

                int count_test = 0;
                for (const auto& test_case : test_group["tests"].array_range()) 
                {
                    //std::cout << "  Test case " << count << "." << count_test << ": " << test_case["description"] << "\n";
                    ++count_test;
                    std::size_t errors = 0;
                    auto reporter = [&](const jsonschema::validation_message& msg) -> jsonschema::walk_result
                    {
                        ++errors;
                        CHECK_FALSE(test_case["valid"].as<bool>());
                        if (test_case["valid"].as<bool>())
                        {
                            std::cout << "  File: " << fpath << "\n";
                            std::cout << "  Test case " << count << "." << count_test << ": " << test_case["description"] << "\n";
                            std::cout << "  Failed: " << msg.instance_location().string() << ": " << msg.message() << "\n";
                            for (const auto& err : msg.details())
                            {
                                std::cout << "  Nested error: " << err.instance_location().string() << ": " << err.message() << "\n";
                            }
                        }
                        return jsonschema::walk_result::advance;
                    };
                    compiled.validate(test_case.at("data"), reporter);
                    if (errors == 0)
                    {
                        CHECK(test_case["valid"].as<bool>());
                        if (!test_case["valid"].as<bool>())
                        {
                            std::cout << "  File: " << fpath << "\n";
                            std::cout << "  Test case " << count << "." << count_test << ": " << test_case["description"] << "\n";
                        }
                    }
                }
            }
            catch (const std::exception& e)
            {
                std::cout << "  File: " << fpath << " " << count << "\n";
                std::cout << e.what() << "\n\n";
                CHECK(false);
            }
        }
    }
}
 
TEST_CASE("jsonschema draft2020-12 tests")
{
    SECTION("issues")
    {
        //jsonschema_tests("./jsonschema/issues/draft2020-12/issue-uri.json",
        //    jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
        //    require_format_validation(true));
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/uri.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
            require_format_validation(true));
    }

    SECTION("more_tests")
    {
        // unevaluated-tests.json is from https://github.com/networknt/json-schema-validator/tree/master/src/test/resources/schema/unevaluatedTests
        //jsonschema_tests("./jsonschema/more_tests/draft2020-12/unevaluated-tests.json",
        //    jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
        //        compatibility_mode(true));
    }
    SECTION("tests")
    {
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/anchor.json"); 
#ifdef JSONCONS_HAS_STD_REGEX
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/additionalProperties.json");
#endif
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/allOf.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/anyOf.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/boolean_schema.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/const.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/contains.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/default.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/defs.json");  
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/dynamicRef.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/enum.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/exclusiveMaximum.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/exclusiveMinimum.json");

#ifdef JSONCONS_HAS_STD_REGEX
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/format.json");
#endif

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/if-then-else.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/infinite-loop-detection.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/items.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/maximum.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/maxItems.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/maxLength.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/maxProperties.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/minimum.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/minItems.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/minLength.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/minProperties.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/multipleOf.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/not.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/oneOf.json");
#ifdef JSONCONS_HAS_STD_REGEX
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/pattern.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/patternProperties.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/properties.json");
#endif
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/propertyNames.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/ref.json"); 

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/refRemote.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/required.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/type.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/unevaluatedProperties.json");
        
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/unevaluatedItems.json");

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/uniqueItems.json"); 
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/vocabulary.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/dependencies-compatibility.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                compatibility_mode(true));
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/bignum.json");

        // format tests
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/date.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/date-time.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/ecmascript-regex.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/email.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/hostname.json",
        //    jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
        //        require_format_validation(true));
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/idn-email.json");
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/idn-hostname.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/ipv4.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/ipv6.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/iri.json");
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/iri-reference.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/json-pointer.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/regex.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/relative-json-pointer.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/time.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
                require_format_validation(true));

        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/uri.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
            require_format_validation(true));
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/uri-reference.json",
            jsonschema::evaluation_options{}.default_version(jsonschema::schema_version::draft202012()).
            require_format_validation(true));
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/uri-reference.json");
        //jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/optional/format/uri-template.json");
        jsonschema_tests("./jsonschema/JSON-Schema-Test-Suite/tests/draft2020-12/content.json");
    }
}
