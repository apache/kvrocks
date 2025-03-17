// Copyright 2013-2024 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <jsoncons/json.hpp>
#include <jsoncons_ext/jmespath/jmespath.hpp>
#include <catch/catch.hpp>
#include <iostream>
#include <sstream>
#include <vector>
#include <map>
#include <utility>
#include <ctime>
#include <new>
#include <unordered_set> // std::unordered_set
#include <fstream>

namespace jmespath = jsoncons::jmespath;

// When adding custom functions, they are generally placed in their own project's source code and namespace.
namespace myspace {

template <typename Json>
class my_custom_functions : public jmespath::custom_functions<Json>
{
    using reference = const Json&;
    using pointer = const Json*;

    static thread_local size_t current_index;
public:
    my_custom_functions()
    {
        this->register_function("current_date_time", // function name
            0,                                       // number of arguments   
            [](const jsoncons::span<const jmespath::parameter<Json>>,
                jmespath::eval_context<Json>&,
                std::error_code&) -> Json
            {
                auto now = std::chrono::system_clock::now();
                auto milliseconds = std::chrono::duration_cast<std::chrono::milliseconds>(now.time_since_epoch());
                return Json{milliseconds.count()};
            }
        );
        this->register_function("current_index", // function name
            0,                                   // number of arguments   
            [](const jsoncons::span<const jmespath::parameter<Json>>,
                jmespath::eval_context<Json>&,
                std::error_code&) -> Json
            {
                return Json{current_index};
            }
        );
        this->register_function("generate_array", // function name
            4,                                    // number of arguments   
            [](const jsoncons::span<const jmespath::parameter<Json>> params,
                jmespath::eval_context<Json>& context,
                std::error_code& ec) -> Json
            {
                JSONCONS_ASSERT(4 == params.size());

                if (!(params[0].is_value() && params[2].is_expression()))
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                reference ctx = params[0].value();
                reference countValue = get_value(ctx, context, params[1]);
                const auto& expr = params[2].expression();
                const auto& argDefault = params[3];

                if (!countValue.is_number())
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                Json result{jsoncons::json_array_arg};
                std::size_t count = countValue.template as<std::size_t>();
                for (size_t i = 0; i < count; i++)
                {
                    current_index = i;
                    std::error_code ec2;

                    reference ele = expr.evaluate(ctx, context, ec2); 

                    if (ele.is_null())
                    {
                        auto defaultVal = get_value(ctx, context, argDefault);
                        result.emplace_back(std::move(defaultVal));
                    }
                    else
                    {
                        result.emplace_back(ele);
                    }
                }
                current_index = 0;

                return result;
            }
        );
        this->register_function("add", // function name
            2,                         // number of arguments   
            [](jsoncons::span<const jmespath::parameter<Json>> params,
                jmespath::eval_context<Json>& context,
                std::error_code& ec) -> Json
            {
                JSONCONS_ASSERT(2 == params.size());

                if (!(params[0].is_value() && params[1].is_value()))
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                reference arg0 = params[0].value();
                reference arg1 = params[1].value();
                if (!(arg0.is_number() && arg1.is_number()))
                {
                    ec = jmespath::jmespath_errc::invalid_argument;
                    return context.null_value();
                }

                if (arg0.template is<int64_t>() && arg1.template is<int64_t>())
                {
                    int64_t v = arg0.template as<int64_t>() + arg1.template as<int64_t>();
                    return Json(v);
                }
                else
                {
                    double v = arg0.template as<double>() + arg1.template as<double>();
                    return Json(v);
                }
            }
        );
    }

    static reference get_value(reference ctx, jmespath::eval_context<Json>& context,
        const jmespath::parameter<Json>& param)
    {
        if (param.is_expression())
        {
            const auto& expr = param.expression();
            std::error_code ec;
            return expr.evaluate(ctx, context, ec);
        }
        else
        {
            return param.value();
        }
    }
};

template <typename Json>
thread_local size_t my_custom_functions<Json>::current_index = 0;

} // namespace myspace

using json = jsoncons::json;

TEST_CASE("jmespath custom function test")
{
    SECTION("test 1")
    {
        std::string jtext = R"(
              {
                "devices": [
                  {
                    "position": 1,
                    "id": "id-xxx",
                    "state": 1
                  },
                  {
                    "position": 5,
                    "id": "id-yyy",
                    "state": 1
                  },
                  {
                    "position": 9,
                    "id": "id-mmm",
                    "state": 2
                  }
                ]
              }        
        )";
        
        auto expected = json::parse(R"(
[
    {
        "id": "id-xxx",
        "position": 1,
        "state": 1
    },
    {
        "id": "",
        "position": 2,
        "state": 0
    },
    {
        "id": "",
        "position": 3,
        "state": 0
    },
    {
        "id": "",
        "position": 4,
        "state": 0
    },
    {
        "id": "id-yyy",
        "position": 5,
        "state": 1
    },
    {
        "id": "",
        "position": 6,
        "state": 0
    },
    {
        "id": "",
        "position": 7,
        "state": 0
    },
    {
        "id": "",
        "position": 8,
        "state": 0
    },
    {
        "id": "id-mmm",
        "position": 9,
        "state": 2
    },
    {
        "id": "",
        "position": 10,
        "state": 0
    },
    {
        "id": "",
        "position": 11,
        "state": 0
    },
    {
        "id": "",
        "position": 12,
        "state": 0
    },
    {
        "id": "",
        "position": 13,
        "state": 0
    },
    {
        "id": "",
        "position": 14,
        "state": 0
    },
    {
        "id": "",
        "position": 15,
        "state": 0
    },
    {
        "id": "",
        "position": 16,
        "state": 0
    }
]
        )");

        auto expr = jmespath::make_expression<json>("generate_array(devices, `16`, &[?position==add(current_index(), `1`)] | [0], &{id: '', state: `0`, position: add(current_index(), `1`)})",
            myspace::my_custom_functions<json>{});

        auto doc = json::parse(jtext);

        auto result = expr.evaluate(doc);

        CHECK(expected == result);
    }
}

