// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons/json.hpp>

#include <vector>
#include <utility>
#include <ctime>
#include <fstream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("oss-fuzz issues")
{
#if 0
    // Fuzz target: fuzz_parse
    // Issue: Stack-overflow
    // Diagnosis: During basic_json destruction, an internal compiler stack error occurred in std::vector 
    //            destructor after reaching a certain nesting depth,
    //            approximately 270 with visual studio on windows.
    // Resolution: 
    // - Implement destructors for json_array and json_object that flatten std::vector elements
    // - max_nesting_depth option for all parsers and encoders (default 1024)
    SECTION("issue 21589")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_parse-5763671533027328";
        auto options = json_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        REQUIRE_THROWS_WITH(json::parse(is, options), Catch::Matchers::Contains(json_error_category_impl().message((int)json_errc::expected_comma_or_rbracket).c_str()));
    }

    // Fuzz target: fuzz_cbo
    // Issue: Abrt in __cxxabiv1::failed_throw
    // Diagnosis: Huge length field in binary data formats
    // Resolution: Read from source in chunks, to avoid bad_alloc, and fail with unexpected_eof
    SECTION("issue 21619")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_cbor-5171679883165696";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        auto options = cbor::cbor_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        default_json_visitor visitor;

        cbor::cbor_stream_reader reader(is,visitor,options);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec.value() == (int)cbor::cbor_errc::unexpected_eof ||  // x64 arch //-V521
               ec.value() == (int)cbor::cbor_errc::number_too_large)); // x86 arch  
    }

    // Fuzz target: fuzz_cbor
    // Issue: Timeout in fuzz_cbor
    SECTION("issue 21631")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_cbor-5639265590706176";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        default_json_visitor visitor;

        auto options = cbor::cbor_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        cbor::cbor_stream_reader reader(is,visitor,options);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == cbor::cbor_errc::unknown_type || // x64 arch //-V521
               ec == cbor::cbor_errc::number_too_large)); // x86 arch
    }

    // Fuzz target: fuzz_csv
    // Issue: Integer-overflow
    SECTION("issue 21663")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_csv-5762751990595584";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        json_decoder<json> visitor;

        auto options = csv::csv_options{}
            .assume_header(true)
            .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_stream_reader reader(is,visitor,options);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK_FALSE(ec); //-V521

        //std::cout << visitor.get_result() << "" << '\n';
    }

    SECTION("issue 21663b")
    {
        std::string s = "-6.6E6";
        int64_t n{ 0 };
        auto result = jsoncons::detail::to_integer(s.data(),s.size(), n);
        CHECK_FALSE(result); //-V521
    }

    // Fuzz target: fuzz_ubjson
    // Issue: Out-of-memory
    // Diagnosis:  Issue with huge length for a strongly typed array of no-op, null, false, or true,
    // e.g. [[][$][T][#][I][9223372036854775807]
    // Resolution: limit number of items to max_items set in options
    SECTION("issue 21667")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson-5738905124208640";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        default_json_visitor visitor;

        ubjson::ubjson_stream_reader reader(is,visitor);
        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == ubjson::ubjson_errc::max_items_exceeded || // x64 arch //-V521
               ec == ubjson::ubjson_errc::number_too_large)); // x86 arch
    }

    // Fuzz target: fuzz_ubjson
    // Issue: Timeout 
    SECTION("issue 21697")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson-5737197673381888";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        json_decoder<json> visitor;

        ubjson::ubjson_stream_reader reader(is,visitor);
        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec == ubjson::ubjson_errc::key_expected); //-V521
    }

    // Fuzz target: fuzz_cbor
    // Issue: Stack overflow
    SECTION("issue 21709")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_cbor-5740910806827008.fuzz";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        auto options = cbor::cbor_options{}
            .max_nesting_depth(10000);
        REQUIRE_THROWS_WITH(cbor::decode_cbor<json>(is,options), Catch::Matchers::Contains(cbor::cbor_error_category_impl().message((int)cbor::cbor_errc::max_nesting_depth_exceeded).c_str()));
    }

    // Fuzz target: fuzz_cbor
    // Issue: Stack overflow
    SECTION("issue 21710")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor-5141282369568768";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        auto options = cbor::cbor_options{}
            .max_nesting_depth(10000);

        REQUIRE_THROWS_WITH(cbor::decode_cbor<json>(is,options), Catch::Matchers::Contains(cbor::cbor_error_category_impl().message((int)cbor::cbor_errc::max_nesting_depth_exceeded).c_str()));
    }

    SECTION("issue 21710b")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor-5141282369568768";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        auto options = cbor::cbor_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        default_json_visitor visitor;
        cbor::cbor_stream_reader reader(is,visitor,options);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec.value() == (int)cbor::cbor_errc::unknown_type); //-V521
    }

    // Fuzz target: fuzz_msgpack
    // Issue: Out of memory
    SECTION("issue 21801")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_msgpack-5651190114418688";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        json_decoder<json> visitor;

        auto options = msgpack::msgpack_options{};

        msgpack::msgpack_stream_reader reader(is,visitor);
        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec == msgpack::msgpack_errc::unexpected_eof); //-V521
    }

    // Fuzz target: fuzz_cbor
    // Issue: Stack overflow
    SECTION("issue 21805")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor-5687592176844800";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        default_json_visitor visitor;

        auto options = cbor::cbor_options{}

        cbor::cbor_stream_reader reader(is,visitor,options);

        std::error_code ec;
        //reader.read(ec);
        //std::cout << ec.message() << "" << '\n';
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == cbor::cbor_errc::max_nesting_depth_exceeded));  //-V521
    }

    // Fuzz target: fuzz_msgpack
    // Issue: Timeout
    SECTION("issue 21813")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_msgpack-5727715157344256";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        default_json_visitor visitor;

        auto options = msgpack::msgpack_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        msgpack::msgpack_stream_reader reader(is,visitor,options);
        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec == msgpack::msgpack_errc::unexpected_eof); //-V521
    }

    // Fuzz target: fuzz_ubjson
    // Issue: Timeout
    SECTION("issue 21865")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_bson-5637264110780416";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        default_json_visitor visitor;

        auto options = bson::bson_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        bson::bson_stream_reader reader(is,visitor,options);
        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec == bson::bson_errc::unexpected_eof);
    }

    // Fuzz target: fuzz_csv_encoder
    // Issue: Failed throw
    // Resolution: check if csv_parser is still in start state when no more input
    SECTION("issue 21912")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_csv_encoder-5202115606872064.fuzz";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::string s2;
        csv::csv_string_encoder visitor(s2);

        csv::csv_stream_reader reader(is, visitor);
        std::error_code ec;

        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == csv::csv_errc::source_error)); //-V521

        //std::cout << visitor.get_result() << "" << '\n';
    }

    // Fuzz target: fuzz_cbor
    // Issue: failed_throw
    SECTION("issue 21948")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_cbor-5743359164678144";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        json_decoder<json> visitor;
        auto options = cbor::cbor_options{};

        cbor::cbor_stream_reader reader(is,visitor,options);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == cbor::cbor_errc::unknown_type));  //-V521
    }

    // Fuzz target: fuzz_csv_encoder
    // Issue: Failed throw
    // Resolution: Fixed check for floating point values
    SECTION("issue 21990")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_csv_encoder-5682837304115200.fuzz";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::string s2;
        csv::csv_string_encoder visitor(s2);

        csv::csv_stream_reader reader(is, visitor);
        std::error_code ec;

        REQUIRE_NOTHROW(reader.read(ec));
    }

    // Fuzz target: fuzz_cbor_encoder
    // Issue: failed_throw
    // Resolution: Replaced assert that array containing decimal fraction
    //             has size 2 with error code invalid_decimal_fraction
    SECTION("issue  22000")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor_encoder-5685492533428224";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> buf;
        cbor::cbor_bytes_encoder encoder(buf);
        cbor::cbor_stream_reader reader(is, encoder);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec == cbor::cbor_errc::invalid_decimal_fraction); //-V521
    }

    // Fuzz target: fuzz_cbor_encoder
    // Issue: failed_throw
    // Resolution: prettify_string with decimal fractions failed with exponents >= 1000

    SECTION("issue 22018")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor_encoder-5673305546948608";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> buf;
        cbor::cbor_bytes_encoder encoder(buf);
        cbor::cbor_stream_reader reader(is, encoder);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == cbor::cbor_errc::illegal_chunked_string || ec == cbor::cbor_errc::invalid_decimal_fraction)); //-V521
    }
    // Fuzz target: fuzz_cbor_encoder
    // Issue: Stack-overflow
    // Resolution: 

    SECTION("issue 22023")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor_encoder-5681910597812224";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> buf;
        cbor::cbor_bytes_encoder encoder(buf);

        cbor::cbor_stream_reader reader(is, encoder);

        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        CHECK(ec == cbor::cbor_errc::unexpected_eof); //-V521
    }

    // Fuzz target: fuzz_msgpack_encoder
    // Issue: Timeout
    SECTION("issue 22024")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_msgpack_encoder-5677646685143040";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> buf;
        msgpack::msgpack_bytes_encoder visitor(buf);

        msgpack::msgpack_stream_reader reader(is,visitor);
        std::error_code ec;

        REQUIRE_NOTHROW(reader.read(ec));
        CHECK((ec == msgpack::msgpack_errc::unexpected_eof || //-V521
               ec == msgpack::msgpack_errc::unknown_type));
    }

    // Fuzz target: fuzz_cbor_encoder
    // Issue: TIMEOUT
    // Resolution: 
/*
    SECTION("issue 22379")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_cbor_encoder-6266427819687936";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is);

        std::vector<uint8_t> buf;
        cbor::cbor_bytes_encoder encoder(buf);

        cbor::cbor_stream_reader reader(is, encoder);

        std::error_code ec;
        try
        {
            reader.read(ec);
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "\n";
        }
        //REQUIRE_NOTHROW(reader.read(ec));
        //CHECK(ec == cbor::cbor_errc::unexpected_eof);
    }
*/
    // Fuzz target: fuzz_ubjson
    // Issue: Direct-leak in std::__1::__libcpp_allocate
    SECTION("issue 24216")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson-5667315455361024";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        try {
           json j2 = ubjson::decode_ubjson<json>(is);
        }
        catch(const jsoncons::ser_error&) 
        {
            //std::cout << e.what() << "\n\n";
        }
    }

    // Fuzz target: fuzz_msgpack_parser_max
    // Issue:  Integer-overflow
    SECTION("issue 24574")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_msgpack_parser_max-6248108141576192";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        default_json_visitor visitor;
        auto options = msgpack::msgpack_options{}
            .max_nesting_depth(std::numeric_limits<int>::max());

        msgpack::msgpack_stream_reader reader(is, visitor, options);
        std::error_code ec;
        reader.read(ec);
    }

    // Fuzz target: fuzz_ubjson
    // Issue: failed_throw
    SECTION("issue 25891")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson-5751108612653056";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        try {
           json j2 = ubjson::decode_ubjson<json>(is);
        }
        catch(const jsoncons::ser_error&) {}
    }

    // Fuzz target: fuzz_json_encoder
    // Issue: Out-of-memory
    SECTION("issue 27342")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_json_encoder-5769370349076480";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::string s2;
        json_string_encoder visitor(s2);

        json_stream_reader reader(is, visitor);
        try
        {
            std::error_code ec;
            reader.read(ec);
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << '\n';
        }
    }

    // Fuzz target: fuzz_json_encoder
    // Issue: Container-overflow READ 1
    SECTION("issue 33781")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_json_encoder-5639543840702464";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::string s2;
        json_string_encoder visitor(s2);

        json_stream_reader reader(is, visitor);
        std::error_code ec;
        reader.read(ec);
    }
    // Fuzz target: jsoncons:fuzz_json_cursor
    // Issue: failed_throw
    SECTION("issue 22091")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_json_cursor-5686693027119104";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::error_code ec;
        json_stream_cursor reader(is, ec);
        while (!reader.done() && !ec)
        {
            const auto& event = reader.current();
            std::string s2 = event.get<std::string>(ec); 
            if (!ec)
            {
                reader.next(ec);
            }
        }
        CHECK(ec == conv_errc::not_string); //-V521
    }
    // Fuzz target: jsoncons:fuzz_bson_encoder
    // Issue: Index-out-of-bounds in jsoncons::bson::decimal128_from_chars
    SECTION("issue 34814")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_bson_encoder-5420549982519296";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> s1;
        bson::bson_bytes_encoder encoder(s1);
        bson::bson_stream_reader reader(is, encoder);

        std::error_code ec;
        reader.read(ec);

        //CHECK(ec == bson::bson_errc::unexpected_eof); 
        //std::cout << ec.message() << "\n";
    }

    // Fuzz target: fuzz_ubjson
    // Issue: Timeout in fuzz_ubjson
    // Diagnosis:  
    // Resolution: 
    SECTION("issue 31888")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson-5151420333555712";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        //json_decoder<json> visitor;
        default_json_visitor visitor;

        ubjson::ubjson_stream_reader reader(is,visitor);
        std::error_code ec;
        REQUIRE_NOTHROW(reader.read(ec));
        std::cout << ec.message() << "\n";
    }
    // Fuzz target: fuzz_parse
    // Issue: Abrt in __cxxabiv1::failed_throw
    // Diagnosis:  
    // Resolution: 
    SECTION("issue 35879")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_parse-5981995560730624.fuzz";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        try
        {
            json::parse(is);
        }
        catch(const jsoncons::ser_error&) {}
    }

    // Fuzz target: jsoncons:fuzz_json_cursor
    // Issue: Timeout (exceeds 60 secs)
    SECTION("issue 36013")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_json_cursor-6585089218707456";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::error_code ec;
        json_stream_cursor reader(is, ec);
        while (!reader.done() && !ec)
        {
            const auto& event = reader.current();
            std::string s2 = event.get<std::string>(ec); 
            if (!ec)
            {
                reader.next(ec);
            }
        }
        //CHECK(ec == json_errc::unexpected_eof); 
    }

    // Fuzz target: jsoncons:fuzz_json_cursor
    // Issue: Timeout (exceeds 60 secs)
    SECTION("issue  36926")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_json_cursor-5656793396150272";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::error_code ec;
        json_stream_cursor reader(is, ec);
        while (!reader.done() && !ec)
        {
            const auto& event = reader.current();
            std::string s2 = event.get<std::string>(ec); 
            if (!ec)
            {
                reader.next(ec);
            }
        }
    }
#endif
    // Fuzz target: fuzz_csv_encoder
    // Issue: Failed throw
    // Resolution: 
    SECTION("issue 391917540")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_csv_encoder-4850580052312064";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::string s2;
        csv::csv_string_encoder visitor(s2);

        csv::csv_stream_reader reader(is, visitor);
        std::error_code ec;

        REQUIRE_NOTHROW(reader.read(ec));
        std::cout << ec.message() << "\n";
        
        //CHECK((ec == csv::csv_errc::source_error)); //-V521

        //std::cout << visitor.get_result() << "" << '\n';
    }
}

#if 0
TEST_CASE("Fuzz target: fuzz_ubjson_encoder")
{
    // Issue: Timeout
    /*SECTION("issue 23840")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson_encoder-5711604342849536";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> output;
        ubjson::ubjson_bytes_encoder encoder(output);
        ubjson::ubjson_stream_reader reader(is, encoder);

        std::error_code ec;
        reader.read(ec);
        CHECK(ec == ubjson::ubjson_errc::unknown_type); //-V521
    }*/

    // Issue: Timeout (exceeds 60 secs)
    /*SECTION("issue 378891965")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_ubjson_encoder-6542820946542592";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        std::vector<uint8_t> output;
        ubjson::ubjson_bytes_encoder encoder(output);
        ubjson::ubjson_stream_reader reader(is, encoder);

        std::error_code ec;
        reader.read(ec);
        CHECK(ec == ubjson::ubjson_errc::unknown_type); //-V521
    }*/
}

TEST_CASE("Fuzz target: fuzz_cbor_encoder")
{
    // Fuzz target: fuzz_cbor_encoder
    // Issue: failed_throw
    // Resolution: change assert to illegal_chunked_string error code
/*    SECTION("issue 21902")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-fuzz_cbor_encoder-5665976638242816";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        try
        {
            std::vector<uint8_t> buf;
            cbor::cbor_bytes_encoder encoder(buf);
            cbor::cbor_stream_reader reader(is, encoder);

            std::error_code ec;
            REQUIRE_NOTHROW(reader.read(ec));
            CHECK(ec == cbor::cbor_errc::illegal_chunked_string); //-V521
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "" << '\n';
        }
    }
*/
    // Fuzz target: fuzz_cbor_encoder
    // Issue: Integer-overflow
    SECTION("issue 42538003")
    {
        std::string pathname = "fuzz_regression/input/clusterfuzz-testcase-minimized-fuzz_cbor_encoder-4729089884225536";

        std::ifstream is(pathname, std::ios_base::in | std::ios_base::binary);
        CHECK(is); //-V521

        try
        {
            std::vector<uint8_t> buf;
            cbor::cbor_bytes_encoder encoder(buf);
            cbor::cbor_stream_reader reader(is, encoder);

            std::error_code ec;
            REQUIRE_NOTHROW(reader.read(ec));
            CHECK_FALSE(ec); //-V521
        }
        catch (const std::exception& e)
        {
            std::cout << e.what() << "" << '\n';
        }
    }
}
#endif


