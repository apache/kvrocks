// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons/json_reader.hpp>
#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <iostream>
#include <catch/catch.hpp>

using namespace jsoncons;
using namespace jsoncons::literals;

TEST_CASE("test_n_objects")
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    auto options = csv::csv_options{}
        .assume_header(true)
           .subfield_delimiter(';');

    json expected = R"(
[
    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    },
    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    }
]
    )"_json;

    JSONCONS_TRY
    {
        json j = csv::decode_csv<json>(s,options);
        CHECK(expected == j);
        //std::cout << pretty_print(j) << '\n';
    }
    JSONCONS_CATCH (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

TEST_CASE("test_n_rows")
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    auto options = csv::csv_options{}
        .mapping_kind(csv::csv_mapping_kind::n_rows)
        .subfield_delimiter(';');

    json expected = R"(
[
    ["calculationPeriodCenters","paymentCenters","resetCenters"],
    [
        ["NY","LON"],"TOR","LON"
    ],
    ["NY","LON",
        ["TOR","LON"]
    ],
    [
        ["NY","LON"],"TOR","LON"
    ],
    ["NY","LON",
        ["TOR","LON"]
    ]
]
    )"_json;

    JSONCONS_TRY
    {
        json j = csv::decode_csv<json>(s,options);
        CHECK(expected == j);
        //std::cout << pretty_print(j) << '\n';
    }
    JSONCONS_CATCH (const std::exception& e)
    {
        std::cerr << e.what() << '\n';
    }
}

TEST_CASE("test_m_columns")
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    auto options = csv::csv_options{}
        .assume_header(true)
           .mapping_kind(csv::csv_mapping_kind::m_columns)
           .subfield_delimiter(';');

    json expected = R"(
{
    "calculationPeriodCenters": [
        ["NY","LON"],"NY",
        ["NY","LON"],"NY"
    ],
    "paymentCenters": ["TOR","LON","TOR","LON"],
    "resetCenters": ["LON",
        ["TOR","LON"],"LON",
        ["TOR","LON"]
    ]
}
    )"_json;

    json j = csv::decode_csv<json>(s,options);
    CHECK(expected == j);
}

