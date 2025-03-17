// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons/json_encoder.hpp>
#include <jsoncons_ext/csv/csv_cursor.hpp>
#include <jsoncons_ext/csv/csv.hpp>

#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("csv_cursor eof test")
{
    const std::string data = "";

    SECTION("csv::csv_mapping_kind::n_rows eof test")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::n_rows);
        std::error_code ec;
        csv::csv_string_cursor cursor(data, options, ec);
        CHECK(ec == csv::csv_errc::source_error);
    }
}

TEST_CASE("csv_cursor n_rows test")
{
    const std::string data = R"(index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";

    SECTION("n_rows test")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_string_cursor cursor(data, options);

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("index_id"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("observation_date"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("rate"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("EUR_LIBOR_06M"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("2015-10-23"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("EUR_LIBOR_06M"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("EUR_LIBOR_06M"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done());
    }
    SECTION("m_columns test")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::m_columns);
        csv::csv_string_cursor cursor(data, options);

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("index_id"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("observation_date"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("rate"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();
        CHECK(cursor.done());
    }
}

TEST_CASE("csv_cursor n_rows with quotes test")
{
    const std::string data = R"("index_id","observation_date","rate"
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";

    SECTION("test 1")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_string_cursor cursor(data, options);
        /* for (; !cursor.done(); cursor.next())
        {
            const auto& event = cursor.current();
            switch (event.event_type())
            {
                case staj_event_type::begin_array:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::end_array:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::begin_object:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::end_object:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::key:
                    // Or std::string_view, if supported
                    std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                    break;
                case staj_event_type::string_value:
                    // Or std::string_view, if supported
                    std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                    break;
                case staj_event_type::null_value:
                    std::cout << event.event_type() << "\n";
                    break;
                case staj_event_type::bool_value:
                    std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << "\n";
                    break;
                case staj_event_type::int64_value:
                    std::cout << event.event_type() << ": " << event.get<int64_t>() << "\n";
                    break;
                case staj_event_type::uint64_value:
                    std::cout << event.event_type() << ": " << event.get<uint64_t>() << "\n";
                    break;
                case staj_event_type::double_value:
                    std::cout << event.event_type() << ": " << event.get<double>() << "\n";
                    break;
                default:
                    std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                    break;
            }
        }*/

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("index_id"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("observation_date"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("rate"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done());
    }
}

TEST_CASE("csv_cursor n_objects test")
{
    const std::string data = R"(index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";

    SECTION("test 2")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::n_objects);
        csv::csv_string_cursor cursor(data, options);
/*
        for (; !cursor.done(); cursor.next())
        {
            const auto& event = cursor.current();
            switch (event.event_type())
            {
                case staj_event_type::begin_array:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::end_array:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::begin_object:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::end_object:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::key:
                    // Or std::string_view, if supported
                    std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                    break;
                case staj_event_type::string_value:
                    // Or std::string_view, if supported
                    std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                    break;
                case staj_event_type::null_value:
                    std::cout << event.event_type() << "\n";
                    break;
                case staj_event_type::bool_value:
                    std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << "\n";
                    break;
                case staj_event_type::int64_value:
                    std::cout << event.event_type() << ": " << event.get<int64_t>() << "\n";
                    break;
                case staj_event_type::uint64_value:
                    std::cout << event.event_type() << ": " << event.get<uint64_t>() << "\n";
                    break;
                case staj_event_type::double_value:
                    std::cout << event.event_type() << ": " << event.get<double>() << "\n";
                    break;
                default:
                    std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                    break;
            }
        }
*/
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("index_id"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("observation_date"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("rate"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("index_id"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("observation_date"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("rate"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("index_id"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("observation_date"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("rate"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::double_value);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done());
    }
}

TEST_CASE("csv_cursor n_objects subfields test")
{
    const std::string data = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";

    SECTION("test 1")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .subfield_delimiter(';');

        csv::csv_string_cursor cursor(data, options);

        /* 
        for (; !cursor.done(); cursor.next())
        {
            const auto& event = cursor.current();
            switch (event.event_type())
            {
                case staj_event_type::begin_array:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::end_array:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::begin_object:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::end_object:
                    std::cout << event.event_type() << " " << "\n";
                    break;
                case staj_event_type::key:
                    // Or std::string_view, if supported
                    std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                    break;
                case staj_event_type::string_value:
                    // Or std::string_view, if supported
                    std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                    break;
                case staj_event_type::null_value:
                    std::cout << event.event_type() << "\n";
                    break;
                case staj_event_type::bool_value:
                    std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << "\n";
                    break;
                case staj_event_type::int64_value:
                    std::cout << event.event_type() << ": " << event.get<int64_t>() << "\n";
                    break;
                case staj_event_type::uint64_value:
                    std::cout << event.event_type() << ": " << event.get<uint64_t>() << "\n";
                    break;
                case staj_event_type::double_value:
                    std::cout << event.event_type() << ": " << event.get<double>() << "\n";
                    break;
                default:
                    std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                    break;
            }
        } 
        */ 

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("calculationPeriodCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("NY"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("paymentCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("TOR"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("resetCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("calculationPeriodCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("NY"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("paymentCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("resetCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("TOR"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("calculationPeriodCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("NY"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("paymentCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("TOR"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("resetCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("calculationPeriodCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("NY"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("paymentCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("resetCenters"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("TOR"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("LON"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done()); 

    }
}

TEST_CASE("csv_cursor n_rows, no header test")
{
    std::string data = "\"b\"";

    SECTION("test 1")
    {
        auto options = csv::csv_options{}
            .mapping_kind(csv::csv_mapping_kind::n_rows)
            .assume_header(false);

        csv::csv_string_cursor cursor(data, options);
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("b"));
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done()); 
    }
}

TEST_CASE("csv_cursor n_objects, header test")
{
    std::string data = "a\n\"4\"";

    SECTION("test 2")
    {
        auto options = csv::csv_options{}
            .assume_header(true);

        csv::csv_string_cursor cursor(data, options);

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("a"));
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("4"));
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done());
    }
}

TEST_CASE("csv_cursor header, subfield no terminating new line test")
{
    std::string data = "a\n4;-5";

    SECTION("test 1")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .subfield_delimiter(';')
               .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_string_cursor cursor(data, options);

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::string_value);
        CHECK(cursor.current().get<std::string>() == std::string("a"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::uint64_value);
        CHECK(cursor.current().get<int>() == 4);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::int64_value);
        CHECK(cursor.current().get<int>() == -5);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done());
    }

    SECTION("test 2")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .subfield_delimiter(';');
        csv::csv_string_cursor cursor(data, options);

        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_object);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::key);
        CHECK(cursor.current().get<std::string>() == std::string("a"));
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::begin_array);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::uint64_value);
        CHECK(cursor.current().get<int>() == 4);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::int64_value);
        CHECK(cursor.current().get<int>() == -5);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();

        CHECK(cursor.current().event_type() == staj_event_type::end_object);
        cursor.next();
        CHECK(cursor.current().event_type() == staj_event_type::end_array);
        cursor.next();
        CHECK(cursor.done());
    }
}
struct remove_mark_csv_filter
{
    bool reject_next_ = false;

    bool operator()(const staj_event& event, const ser_context&) 
    {
        if (event.event_type()  == staj_event_type::key &&
            event.get<jsoncons::string_view>() == "mark")
        {
            reject_next_ = true;
            return false;
        }
        else if (reject_next_)
        {
            reject_next_ = false;
            return false;
        }
        else
        {
            return true;
        }
    }
};

TEST_CASE("csv_cursor with filter tests")
{
    auto j = ojson::parse(R"(
    [
        {
            "enrollmentNo" : 100,
            "firstName" : "Tom",
            "lastName" : "Cochrane",
            "mark" : 55
        },
        {
            "enrollmentNo" : 101,
            "firstName" : "Catherine",
            "lastName" : "Smith",
            "mark" : 95
        },
        {
            "enrollmentNo" : 102,
            "firstName" : "William",
            "lastName" : "Skeleton",
            "mark" : 60
        }
    ]
    )");

    std::string data;
    csv::encode_csv(j, data);

    auto options = csv::csv_options{}
        .assume_header(true);
    csv::csv_string_cursor cursor(data, options);
    auto filtered_c = cursor | remove_mark_csv_filter();

    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_array);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::uint64_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::uint64_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::begin_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::uint64_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::key);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::string_value);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_object);
    filtered_c.next();
    REQUIRE_FALSE(filtered_c.done());
    CHECK(filtered_c.current().event_type() == staj_event_type::end_array);
    filtered_c.next();
    CHECK(filtered_c.done());
}

TEST_CASE("test_csv_parser_reinitialization")
{
    json_decoder<json> decoder;
    csv::csv_parser parser(csv::csv_options().assume_header(true)
                                             .max_lines(2));

    parser.reinitialize();
    std::string input = "h1,h2\n"
                        "3,4\n"
                        "5,6\n";
    parser.update(input.data(), input.size());
    int count = 0;
    while (!parser.stopped() && count < 20)
    {
        parser.parse_some(decoder);
        ++count;
    }
    REQUIRE(parser.stopped());
    decoder.end_array();
    REQUIRE(decoder.is_valid());
    json j = decoder.get_result();
    json expected = json::parse(R"([{"h1":3,"h2":4}])");
    CHECK(expected == j);

    parser.reinitialize();
    input = "h7,h8\n"
            "9,10\n";
    parser.update(input.data(), input.size());
    count = 0;
    while (!parser.stopped() && count < 20)
    {
        parser.parse_some(decoder);
        ++count;
    }
    REQUIRE(parser.stopped());
    decoder.end_array();
    REQUIRE(decoder.is_valid());
    j = decoder.get_result();
    expected = json::parse(R"([{"h7":9,"h8":10}])");
    CHECK(expected == j);
}

template <typename CursorType>
void check_csv_cursor_table(std::string info, CursorType& cursor,
                            std::string expected_key, unsigned expected_value)
{
    INFO(info);

    REQUIRE_FALSE(cursor.done());
    CHECK(cursor.current().event_type() == staj_event_type::begin_array);

    CHECK_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::begin_object);

    CHECK_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::key);
    CHECK(cursor.current().template get<std::string>() == expected_key);

    CHECK_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::uint64_value);
    CHECK(cursor.current().template get<unsigned>() == expected_value);

    CHECK_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::end_object);

    CHECK_FALSE(cursor.done());
    cursor.next();
    CHECK(cursor.current().event_type() == staj_event_type::end_array);

    cursor.next();
    CHECK(cursor.done());
}

TEMPLATE_TEST_CASE("csv_cursor reset test", "",
                   (std::pair<csv::csv_string_cursor, std::string>),
                   (std::pair<csv::csv_stream_cursor, std::istringstream>))
{
    using cursor_type = typename TestType::first_type;
    using input_type = typename TestType::second_type;

    SECTION("with another source")
    {
        input_type input1("h1\n1\n");
        input_type input2("");
        input_type input3("h3\n3\n");
        auto options = csv::csv_options().assume_header(true);
        std::error_code ec;
        cursor_type cursor(input1, options, ec);
        check_csv_cursor_table("with input1", cursor, "h1", 1);

        cursor.reset(input2, ec);
        CHECK(ec == csv::csv_errc::source_error);
        CHECK_FALSE(cursor.done());

        // Check that cursor can reused be upon reset following an error.
        ec = csv::csv_errc::success;
        cursor.reset(input3);
        check_csv_cursor_table("with input3", cursor, "h3", 3);
    }
}
