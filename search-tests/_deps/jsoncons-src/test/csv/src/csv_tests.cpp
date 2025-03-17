// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/csv/csv.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_reader.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <iostream>
#include <fstream>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("csv subfield delimiter tests")
{
    SECTION("n_objects tests")
    {
    const std::string input = R"(
    [
       {"a":[1,-2,3.0],"b":[true,false,null]},
       {"a":["7","8","9"],"b":["10","11","12"]}
    ]
    )";

       json j = json::parse(input);
       //std::cout << pretty_print(j) << "\n\n";

       auto options = csv::csv_options{}
           .assume_header(true)
              .subfield_delimiter(';')
              .quote_style(csv::quote_style_kind::nonnumeric)
              .mapping_kind(csv::csv_mapping_kind::n_objects);

       std::string output;
       csv::encode_csv(j, output, options);
       json other = csv::decode_csv<json>(output, options);
       //std::cout << pretty_print(other) << "\n\n";

       CHECK(j == other);
    }

    SECTION("n_rows tests")
    {
    const std::string input = R"(
    [
       [[1,2,3],[4,5,6]],
       [[7,8,9],[10,11,12]]   
    ]
    )";

       json j = json::parse(input);
       //std::cout << pretty_print(j) << "\n\n";

       auto options = csv::csv_options{}
           .subfield_delimiter(';');

       std::string output;
       csv::encode_csv(j, output, options);
       json other = csv::decode_csv<json>(output, options);

       CHECK(j == other);
    }

    SECTION("m_columns tests")
    {
    const std::string input = R"(
    {
       "a" : [[1,true,null],[-4,5.5,"6"]],
       "b" : [[7,8,9],[10,11,12]],
       "c" : [15,16,17]       
    }
    )";

       json j = json::parse(input);
       //std::cout << pretty_print(j) << "\n\n";

       auto options = csv::csv_options{}
           .subfield_delimiter(';')
           .quote_style(csv::quote_style_kind::nonnumeric)
           .mapping_kind(csv::csv_mapping_kind::m_columns)
           .assume_header(true)
           .ignore_empty_values(true);

       std::string output;
       csv::encode_csv(j, output, options);
       //std::cout << output << "\n\n";

       json other = csv::decode_csv<json>(output, options);
       //std::cout << other << "\n\n";

       CHECK(j == other);
    }
}

TEST_CASE("csv_test_empty_values_with_defaults x")
{
    std::string input = "bool-f,string-f"
"\n,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true); 

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    //std::cout << pretty_print(val) << '\n';

    CHECK(val[0]["string-f"].as<std::string>() == "");
}

TEST_CASE("n_objects_test")
{
    const std::string bond_yields = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-07,0.0063,0.0076,0.0084,0.0112
)";

    json_decoder<ojson> decoder;
    auto options = csv::csv_options{}
        .assume_header(true)
        .mapping_kind(csv::csv_mapping_kind::n_rows);
    csv::csv_string_reader reader1(bond_yields,decoder,options);
    reader1.read();
    ojson val1 = decoder.get_result();
    //std::cout << "\n(1)\n"<< pretty_print(val1) << "\n";
    CHECK(val1.size() == 4);

    options.assume_header(true);
    options.mapping_kind(csv::csv_mapping_kind::n_objects);
    csv::csv_string_reader reader2(bond_yields,decoder,options);
    reader2.read();
    ojson val2 = decoder.get_result();
    //std::cout << "\n(2)\n"<< pretty_print(val2) << "\n";
    REQUIRE(val2.size() == 3);
    CHECK("2017-01-09" == val2[0]["Date"].as<std::string>());
}

TEST_CASE("m_columns_test")
{
    const std::string bond_yields = R"(Date,ProductType,1Y,2Y,3Y,5Y
2017-01-09,"Bond",0.0062,0.0075,0.0083,0.011
2017-01-08,"Bond",0.0063,0.0076,0.0084,0.0112
2017-01-08,"Bond",0.0063,0.0076,0.0084,0.0112
)";

    json_decoder<ojson> decoder;
    auto options = csv::csv_options{}
        .assume_header(true)
           .mapping_kind(csv::csv_mapping_kind::m_columns);

    std::istringstream is(bond_yields);
    csv::csv_stream_reader reader(is, decoder, options);
    reader.read();
    ojson j = decoder.get_result();

    CHECK(6 == j.size());
    CHECK(3 == j["Date"].size());
    CHECK(3 == j["1Y"].size());
    CHECK(3 == j["2Y"].size());
    CHECK(3 == j["3Y"].size());
    CHECK(3 == j["5Y"].size());
}

TEST_CASE("csv ignore_empty_value")
{
    const std::string bond_yields = R"(Date,ProductType,1Y,2Y,3Y,5Y
2017-01-09,"Bond",0.0062,0.0075,0.0083,0.011
2017-01-08,"Bond",0.0063,0.0076,,0.0112
2017-01-08,"Bond",0.0063,0.0076,0.0084,
)";
    //std::cout << bond_yields << '\n' << "\n\n";

    SECTION("m_columns")
    {
        json_decoder<ojson> decoder;
        auto options = csv::csv_options{}
            .assume_header(true)
               .ignore_empty_values(true)
               .mapping_kind(csv::csv_mapping_kind::m_columns);

        std::istringstream is(bond_yields);
        csv::csv_stream_reader reader(is, decoder, options);
        reader.read();
        ojson j = decoder.get_result();
        //std::cout << "\n(1)\n"<< pretty_print(j) << "\n";
        CHECK(6 == j.size());
        CHECK(3 == j["Date"].size());
        CHECK(3 == j["1Y"].size());
        CHECK(3 == j["2Y"].size());
        CHECK(2 == j["3Y"].size());
        CHECK(2 == j["5Y"].size());
    }
/*
    SECTION("n_rows")
    {
        json_decoder<ojson> decoder;
        auto options = csv::csv_options{}
        options.assume_header(false)
               .ignore_empty_values(true)
               .mapping_kind(csv::csv_mapping_kind::n_rows);

        std::istringstream is(bond_yields);
        csv::csv_stream_reader reader(is, decoder, options);
        reader.read();
        ojson j = decoder.get_result();
        std::cout << "\n(1)\n"<< pretty_print(j) << "\n\n";
    }
*/
}

TEST_CASE("csv_test_empty_values")
{
    std::string input = "bool-f,int-f,float-f,string-f"
"\n,,,,"
"\ntrue,12,24.7,\"test string\","
"\n,,,,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true)
           .column_types("boolean,integer,float,string");

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"].as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());

    CHECK(val[1]["bool-f"] .as<bool>()== true);
    CHECK(val[1]["bool-f"].is<bool>());
    CHECK(val[1]["int-f"] .as<int>()== 12);
    CHECK(val[1]["int-f"].is<int>());
    CHECK(val[1]["float-f"] .as<double>()== 24.7);
    CHECK(val[1]["float-f"].is<double>());
    CHECK(val[1]["string-f"].as<std::string>() == "test string");
    CHECK(val[1]["string-f"].is<std::string>());

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());
}

TEST_CASE("csv_test_empty_values_with_defaults")
{
    std::string input = "bool-f,int-f,float-f,string-f"
"\n,,,,"
"\ntrue,12,24.7,\"test string\","
"\n,,,,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true) 
           .column_types("boolean,integer,float,string")
           .column_defaults("false,0,0.0,\"\"");

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    //std::cout << pretty_print(val) << '\n';

    CHECK(val[0]["bool-f"].as<bool>() == false);
    CHECK(val[0]["bool-f"].is<bool>());
    CHECK(val[0]["int-f"] .as<int>()== 0);
    CHECK(val[0]["int-f"].is<int>());
    CHECK(val[0]["float-f"].as<double>() == 0.0);
    CHECK(val[0]["float-f"].is<double>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());

    CHECK(val[1]["bool-f"] .as<bool>()== true);
    CHECK(val[1]["bool-f"].is<bool>());
    CHECK(val[1]["int-f"] .as<int>()== 12);
    CHECK(val[1]["int-f"].is<int>());
    CHECK(val[1]["float-f"] .as<double>()== 24.7);
    CHECK(val[1]["float-f"].is<double>());
    CHECK(val[1]["string-f"].as<std::string>() == "test string");
    CHECK(val[1]["string-f"].is<std::string>());

    CHECK(val[2]["bool-f"].as<bool>() == false);
    CHECK(val[2]["bool-f"].is<bool>());
    CHECK(val[2]["int-f"] .as<int>()== 0);
    CHECK(val[2]["int-f"].is<int>());
    CHECK(val[2]["float-f"].as<double>() == 0.0);
    CHECK(val[2]["float-f"].is<double>());
    CHECK(val[2]["string-f"].as<std::string>() == "");
    CHECK(val[2]["string-f"].is<std::string>());
}

TEST_CASE("csv_test_empty_values_with_empty_defaults")
{
    std::string input = "bool-f,int-f,float-f,string-f"
"\n,,,,"
"\ntrue,12,24.7,\"test string\","
"\n,,,,";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true)
           .column_types("boolean,integer,float,string")
           .column_defaults(",,,");

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());

    CHECK(val[1]["bool-f"] .as<bool>() == true);
    CHECK(val[1]["bool-f"].is<bool>());
    CHECK(val[1]["int-f"] .as<int>()== 12);
    CHECK(val[1]["int-f"].is<int>());
    CHECK(val[1]["float-f"] .as<double>()== 24.7);
    CHECK(val[1]["float-f"].is<double>());
    CHECK(val[1]["string-f"].as<std::string>() == "test string");
    CHECK(val[1]["string-f"].is<std::string>());

    CHECK(val[0]["bool-f"].is_null());
    CHECK(val[0]["bool-f"].is<null_type>());
    CHECK(val[0]["int-f"].is_null());
    CHECK(val[0]["int-f"].is<null_type>());
    CHECK(val[0]["float-f"].is_null());
    CHECK(val[0]["float-f"].is<null_type>());
    CHECK(val[0]["string-f"] .as<std::string>() == "");
    CHECK(val[0]["string-f"].is<std::string>());
}

TEST_CASE("csv_test1_array_1col_skip1_a")
{
    std::string text = "a\n1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .header_lines(1);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json(1));
    CHECK(val[1][0]==json(4));
}

TEST_CASE("csv_test1_array_1col_skip1_b")
{
    std::string text = "a\n1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .header_lines(1)
        .infer_types(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json("1"));
    CHECK(val[1][0]==json("4"));
}

TEST_CASE("csv_test1_array_1col_a")
{
    std::string text = "1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json(1));
    CHECK(val[1][0]==json(4));
}

TEST_CASE("csv_test1_array_1col_b")
{
    std::string text = "1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false)
        .infer_types(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json("1"));
    CHECK(val[1][0]==json("4"));
}

TEST_CASE("csv_test1_array_3cols")
{
    std::string text = "a,b,c\n1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json(4));
    CHECK(val[2][1]==json(5));
    CHECK(val[2][2]==json(6));
}
TEST_CASE("csv_test1_array_3cols_trim_leading")
{
    std::string text = "a ,b ,c \n 1, 2, 3\n 4 , 5 , 6 ";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false)
        .trim_leading(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a "));
    CHECK(val[0][1]==json("b "));
    CHECK(val[0][2]==json("c "));
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json("4 "));
    CHECK(val[2][1]==json("5 "));
    CHECK(val[2][2]==json("6 "));
}

TEST_CASE("csv_test1_array_3cols_trim_trailing")
{
    std::string text = "a ,b ,c \n 1, 2, 3\n 4 , 5 , 6 ";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false)
        .trim_trailing(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(json(" 1") == val[1][0]);
    CHECK(val[1][1]==json(" 2"));
    CHECK(val[1][2]==json(" 3"));
    CHECK(val[2][0]==json(" 4"));
    CHECK(val[2][1]==json(" 5"));
    CHECK(val[2][2]==json(" 6"));
}

TEST_CASE("csv_test1_array_3cols_trim")
{
    std::string text = "a ,, \n 1, 2, 3\n 4 , 5 , 6 ";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false)
        .trim(true)
        .unquoted_empty_value_is_null(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json::null());
    CHECK(val[0][2]==json::null());
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json(4));
    CHECK(val[2][1]==json(5));
    CHECK(val[2][2]==json(6));
}

TEST_CASE("csv_test1_array_3cols_comment")
{
    std::string text = "a,b,c\n#1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .comment_starter('#');

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(val[1][0]==json(4));
    CHECK(val[1][1]==json(5));
    CHECK(val[1][2]==json(6));
}

TEST_CASE("csv comment header line")
{
    std::string data = "#a,b,c\nA,B,C\n1,2,3";

    auto options = csv::csv_options{}
        .comment_starter('#')
        .assume_header(true);

    auto j = csv::decode_csv<ojson>(data, options);

    REQUIRE(j.is_array());
    REQUIRE(j.size() == 1);
    REQUIRE(j[0]["A"].as<int>() == 1);
    REQUIRE(j[0]["B"].as<int>() == 2);
    REQUIRE(j[0]["C"].as<int>() == 3);
}

TEST_CASE("csv_test1_object_1col")
{
    std::string text = "a\n1\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[1]["a"]==json(4));
}

TEST_CASE("csv_test1_object_3cols")
{
    std::string text = "a,b,c\n1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[0]["b"]==json(2));
    CHECK(val[0]["c"]==json(3));
    CHECK(val[1]["a"]==json(4));
    CHECK(val[1]["b"]==json(5));
    CHECK(val[1]["c"]==json(6));
}

TEST_CASE("csv_test1_object_3cols_header")
{
    std::string text = "a,b,c\n1,2,3\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .column_names("x,y,z")
        .header_lines(1);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["x"]==json(1));
    CHECK(val[0]["y"]==json(2));
    CHECK(val[0]["z"]==json(3));
    CHECK(val[1]["x"]==json(4));
    CHECK(val[1]["y"]==json(5));
    CHECK(val[1]["z"]==json(6));
}

TEST_CASE("csv_test1_object_3cols_bool")
{
    std::string text = "a,b,c\n1,0,1\ntrue,FalSe,TrUe";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .column_names("x,y,z")
        .column_types("boolean,boolean,boolean")
        .header_lines(1);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["x"]==json(true));
    CHECK(val[0]["y"]==json(false));
    CHECK(val[0]["z"]==json(true));
    CHECK(val[1]["x"]==json(true));
    CHECK(val[1]["y"]==json(false));
    CHECK(val[1]["z"]==json(true));
}

TEST_CASE("csv_test1_object_1col_quoted")
{
    std::string text = "a\n\"1\"\n\"4\"";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    REQUIRE(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0]["a"]==json("1"));
    CHECK(val[1]["a"]==json("4"));
}

TEST_CASE("csv_test1_object_3cols_quoted")
{
    std::string text = "a,b,c\n\"1\",\"2\",\"3\"\n4,5,\"6\"";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["a"]==json("1"));
    CHECK(val[0]["b"]==json("2"));
    CHECK(val[0]["c"]==json("3"));
    CHECK(val[1]["a"]==json(4));
    CHECK(val[1]["b"]==json(5));
    CHECK(val[1]["c"]==json("6"));
}

TEST_CASE("csv_test1_array_1col_crlf")
{
    std::string text = "1\r\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0][0]==json(1));
    CHECK(val[1][0]==json(4));
}

TEST_CASE("csv_test1_array_3cols_crlf")
{
    std::string text = "a,b,c\r\n1,2,3\r\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0]==json("a"));
    CHECK(val[0][1]==json("b"));
    CHECK(val[0][2]==json("c"));
    CHECK(val[1][0]==json(1));
    CHECK(val[1][1]==json(2));
    CHECK(val[1][2]==json(3));
    CHECK(val[2][0]==json(4));
    CHECK(val[2][1]==json(5));
    CHECK(val[2][2]==json(6));
}

TEST_CASE("csv_test1_object_1col_crlf")
{
    std::string text = "a\r\n1\r\n4";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==1);
    CHECK(val[1].size()==1);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[1]["a"]==json(4));
}

TEST_CASE("csv_test1_object_3cols_crlf")
{
    std::string text = "a,b,c\r\n1,2,3\r\n4,5,6";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==2);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[0]["a"]==json(1));
    CHECK(val[0]["b"]==json(2));
    CHECK(val[0]["c"]==json(3));
    CHECK(val[1]["a"]==json(4));
    CHECK(val[1]["b"]==json(5));
    CHECK(val[1]["c"]==json(6));
}

TEST_CASE("read_comma_delimited_file")
{
    std::string in_file = "./csv/input/countries.csv";
    std::ifstream is(in_file);
    REQUIRE(is);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json countries = decoder.get_result();

    CHECK(4 == countries.size());
    CHECK(json("ABW") == countries[0]["country_code"]);
    CHECK(json("ARUBA") ==countries[0]["name"]);
    CHECK(json("ATF") == countries[1]["country_code"]);
    CHECK(json("FRENCH SOUTHERN TERRITORIES, D.R. OF") == countries[1]["name"]);
    CHECK(json("VUT") == countries[2]["country_code"]);
    CHECK(json("VANUATU") ==countries[2]["name"]);
    CHECK(json("WLF") == countries[3]["country_code"]);
    CHECK(json("WALLIS & FUTUNA ISLANDS") == countries[3]["name"]);
}

TEST_CASE("read_comma_delimited_file_header")
{
    std::string in_file = "./csv/input/countries.csv";
    std::ifstream is(in_file);
    REQUIRE(is);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .column_names("Country Code,Name")
        .header_lines(1);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json countries = decoder.get_result();
    CHECK(4 == countries.size());
    CHECK(json("ABW") == countries[0]["Country Code"]);
    CHECK(json("ARUBA") ==countries[0]["Name"]);
    CHECK(json("ATF") == countries[1]["Country Code"]);
    CHECK(json("FRENCH SOUTHERN TERRITORIES, D.R. OF") == countries[1]["Name"]);
    CHECK(json("VUT") == countries[2]["Country Code"]);
    CHECK(json("VANUATU") == countries[2]["Name"]);
    CHECK(json("WLF") == countries[3]["Country Code"]);
    CHECK(json("WALLIS & FUTUNA ISLANDS") == countries[3]["Name"]);
}
 
TEST_CASE("serialize_comma_delimited_file")
{
    std::string in_file = "./csv/input/countries.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    auto options = csv::csv_options{}
        .assume_header(false);

    json_decoder<ojson> encoder1;
    json_stream_reader reader1(is,encoder1);
    reader1.read();
    ojson countries1 = encoder1.get_result();

    std::stringstream ss;
    csv::csv_stream_encoder encoder(ss,options);
    countries1.dump(encoder);

    json_decoder<ojson> encoder2;
    csv::csv_stream_reader reader2(ss,encoder2,options);
    reader2.read();
    ojson countries2 = encoder2.get_result();

    CHECK(countries1 == countries2);
}

TEST_CASE("test_tab_delimited_file")
{
    std::string in_file = "./csv/input/employees.txt";
    std::ifstream is(in_file);
    REQUIRE(is);

    json_decoder<json> decoder;
    auto options = csv::csv_options{}
        .field_delimiter('\t')
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json employees = decoder.get_result();
    CHECK(4 == employees.size());
    CHECK(std::string("00000001") ==employees[0]["employee-no"].as<std::string>());
    CHECK(std::string("00000002") ==employees[1]["employee-no"].as<std::string>());
    CHECK(std::string("00000003") ==employees[2]["employee-no"].as<std::string>());
    CHECK(std::string("00000004") ==employees[3]["employee-no"].as<std::string>());
}

TEST_CASE("serialize_tab_delimited_file")
{
    std::string in_file = "./csv/input/employees.json";
    std::ifstream is(in_file);
    REQUIRE(is);

    json_decoder<ojson> decoder;
    auto options = csv::csv_options{}
        .assume_header(false)
        .header_lines(1)
        .column_names("dept,employee-name,employee-no,note,comment,salary")
        .field_delimiter('\t');

    json_stream_reader reader(is,decoder);
    reader.read_next();
    ojson employees1 = decoder.get_result();
    
    std::cout << pretty_print(employees1) << "\n";

    std::stringstream ss;
    csv::csv_stream_encoder encoder(ss,options);
    //std::cout << pretty_print(employees1) << '\n';
    employees1.dump(encoder);
    std::cout << ss.str() << '\n';

    json_decoder<ojson> encoder2;
    csv::csv_stream_reader reader2(ss,encoder2,options);
    reader2.read();
    ojson employees2 = encoder2.get_result();
    std::cout << pretty_print(employees2) << '\n';

    CHECK(employees1.size() == employees2.size());

    for (std::size_t i = 0; i < employees1.size(); ++i)
    {
        CHECK(employees1[i]["dept"] == employees2[i]["dept"]);
        CHECK(employees1[i]["employee-name"] ==employees2[i]["employee-name"]);
        CHECK(employees1[i]["employee-no"] ==employees2[i]["employee-no"]);
        CHECK(employees1[i]["salary"] == employees2[i]["salary"]);
        CHECK(employees1[i].get_value_or<std::string>("note","") == employees2[i].get_value_or<std::string>("note",""));
    }
}

TEST_CASE("csv_test1_array_3cols_grouped1")
{
    std::string text = "1,2,3\n4,5,6\n7,8,9";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false)
        .column_types("integer,integer*");

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    CHECK(val.size()==3);
    CHECK(val[0].size()==3);
    CHECK(val[1].size()==3);
    CHECK(val[2].size()==3);
    CHECK(val[0][0].as<int>() == 1);
    CHECK(val[0][1].as<int>() == 2);
    CHECK(val[0][2].as<int>() == 3);
    CHECK(val[1][0].as<int>() == 4);
    CHECK(val[1][1].as<int>() == 5);
    CHECK(val[1][2].as<int>() == 6);
    CHECK(val[2][0].as<int>() == 7);
    CHECK(val[2][1].as<int>() == 8);
    CHECK(val[2][2].as<int>() == 9); 
}

TEST_CASE("csv_test1_array_3cols_grouped2")
{
    std::string text = "1,2,3,4,5\n4,5,6,7,8\n7,8,9,10,11";
    std::istringstream is(text);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(false)
        .column_types("integer,[integer,integer]*");

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json val = decoder.get_result();

    //std::cout << val << '\n';
/*
    REQUIRE(options.column_types().size() == 4);
    CHECK(options.column_types()[0].first == csv::csv_column_type::integer_t);
    CHECK(options.column_types()[0].second == 0);
    CHECK(options.column_types()[1].first == csv::csv_column_type::integer_t);
    CHECK(options.column_types()[1].second == 1);
    CHECK(options.column_types()[2].first == csv::csv_column_type::integer_t);
    CHECK(options.column_types()[2].second == 1);
    CHECK(options.column_types()[3].first == csv::csv_column_type::repeat_t);
    CHECK(options.column_types()[3].second == 2);
*/
}

TEST_CASE("csv_test1_repeat")
{
    const std::string text = R"(Date,1Y,2Y,3Y,5Y
    2017-01-09,0.0062,0.0075,0.0083,0.011,0.012
    2017-01-08,0.0063,0.0076,0.0084,0.0112,0.013
    2017-01-08,0.0063,0.0076,0.0084,0.0112,0.014
    )";    

    std::vector<csv::csv_type_info> result;
    jsoncons::csv::detail::parse_column_types(std::string("string,float*"), result);
    REQUIRE(result.size() == 3);
    CHECK(result[0].col_type == csv::csv_column_type::string_t);
    CHECK(result[0].level == 0);
    CHECK(0 == result[0].rep_count);
    CHECK(result[1].col_type == csv::csv_column_type::float_t);
    CHECK(result[1].level == 0);
    CHECK(0 == result[1].rep_count);
    CHECK(result[2].col_type == csv::csv_column_type::repeat_t);
    CHECK(result[2].level == 0);
    CHECK(1 == result[2].rep_count);

    std::vector<csv::csv_type_info> result2;
    jsoncons::csv::detail::parse_column_types(std::string("string,[float*]"), result2);
    REQUIRE(result2.size() == 3);
    CHECK(result2[0].col_type == csv::csv_column_type::string_t);
    CHECK(result2[0].level == 0);
    CHECK(0 == result2[0].rep_count);
    CHECK(result2[1].col_type == csv::csv_column_type::float_t);
    CHECK(result2[1].level == 1);
    CHECK(0 == result2[1].rep_count);
    CHECK(result2[2].col_type == csv::csv_column_type::repeat_t);
    CHECK(result2[2].level == 1); //-V521
    CHECK(1 == result2[2].rep_count); //-V521

    std::vector<csv::csv_type_info> result3;
    jsoncons::csv::detail::parse_column_types(std::string("string,[float]*"), result3);
    REQUIRE(result3.size() == 3); //-V521
    CHECK(result3[0].col_type == csv::csv_column_type::string_t); //-V521
    CHECK(result3[0].level == 0); //-V521
    CHECK(0 == result3[0].rep_count); //-V521
    CHECK(result3[1].col_type == csv::csv_column_type::float_t); //-V521
    CHECK(result3[1].level == 1); //-V521
    CHECK(0 == result3[1].rep_count); //-V521
    CHECK(result3[2].col_type == csv::csv_column_type::repeat_t); //-V521
    CHECK(result3[2].level == 0); //-V521
    CHECK(1 == result3[2].rep_count); //-V521
}

TEST_CASE("csv_test1_repeat2")
{
    auto options1 = csv::csv_options{}       
        .column_types("[integer,string]*");
    //for (auto x : options1.column_types())
    //{
    //    std::cout << (int)x.col_type << " " << x.level << " " << x.rep_count << '\n';
    //}
}

TEST_CASE("empty_line_test_1")
{
    std::string input = R"(country_code,name
ABW,ARUBA

ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    REQUIRE(j.size() == 4); //-V521
    CHECK(j[0]["country_code"].as<std::string>() == std::string("ABW")); //-V521
    CHECK(j[0]["name"].as<std::string>() == std::string("ARUBA")); //-V521
    CHECK(j[1]["country_code"].as<std::string>() == std::string("ATF")); //-V521
    CHECK(j[1]["name"].as<std::string>() == std::string("FRENCH SOUTHERN TERRITORIES, D.R. OF")); //-V521
    CHECK(j[2]["country_code"].as<std::string>() == std::string("VUT")); //-V521
    CHECK(j[2]["name"].as<std::string>() == std::string("VANUATU")); //-V521
    CHECK(j[3]["country_code"].as<std::string>() == std::string("WLF")); //-V521
    CHECK(j[3]["name"].as<std::string>() == std::string("WALLIS & FUTUNA ISLANDS")); //-V521

    //std::cout << pretty_print(j) << '\n';
}

TEST_CASE("empty_line_test_2")
{
    std::string input = R"(country_code,name
ABW,ARUBA

ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true)
           .ignore_empty_lines(false);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();

    //std::cout << pretty_print(j) << "\n";
    REQUIRE(j.size() == 5); //-V521

    CHECK(j[0]["country_code"].as<std::string>() == std::string("ABW")); //-V521
    CHECK(j[0]["name"].as<std::string>() == std::string("ARUBA")); //-V521
    CHECK(!j[1].contains("country_code")); // ok, no delimiter //-V521
    CHECK(j[2]["country_code"].as<std::string>() == std::string("ATF")); //-V521
    CHECK(j[2]["name"].as<std::string>() == std::string("FRENCH SOUTHERN TERRITORIES, D.R. OF")); //-V521
    CHECK(j[3]["country_code"].as<std::string>() == std::string("VUT")); //-V521
    CHECK(j[3]["name"].as<std::string>() == std::string("VANUATU")); //-V521
    CHECK(j[4]["country_code"].as<std::string>() == std::string("WLF")); //-V521
    CHECK(j[4]["name"].as<std::string>() == std::string("WALLIS & FUTUNA ISLANDS")); //-V521
}

TEST_CASE("line_with_one_space")
{
    std::string input = R"(country_code,name
ABW,ARUBA
 
ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    REQUIRE(j.size() == 5); //-V521
    CHECK(j[0]["country_code"].as<std::string>() == std::string("ABW")); //-V521
    CHECK(j[0]["name"].as<std::string>() == std::string("ARUBA")); //-V521
    CHECK(j[1]["country_code"].as<std::string>() == std::string(" ")); // ok, one space, no delimiter //-V521
    CHECK(j[2]["country_code"].as<std::string>() == std::string("ATF")); //-V521
    CHECK(j[2]["name"].as<std::string>() == std::string("FRENCH SOUTHERN TERRITORIES, D.R. OF")); //-V521
    CHECK(j[3]["country_code"].as<std::string>() == std::string("VUT")); //-V521
    CHECK(j[3]["name"].as<std::string>() == std::string("VANUATU")); //-V521
    CHECK(j[4]["country_code"].as<std::string>() == std::string("WLF")); //-V521
    CHECK(j[4]["name"].as<std::string>() == std::string("WALLIS & FUTUNA ISLANDS")); //-V521

    //std::cout << pretty_print(j) << '\n';
}

TEST_CASE("line_with_one_space_and_trim")
{
    std::string input = R"(country_code,name
ABW,ARUBA
 
ATF,"FRENCH SOUTHERN TERRITORIES, D.R. OF"
VUT,VANUATU
WLF,WALLIS & FUTUNA ISLANDS
)";

    std::istringstream is(input);

    json_decoder<json> decoder;

    auto options = csv::csv_options{}
        .assume_header(true)
           .trim(true);

    csv::csv_stream_reader reader(is,decoder,options);
    reader.read();
    json j = decoder.get_result();
    REQUIRE(j.size() == 4); //-V521
    CHECK(j[0]["country_code"].as<std::string>() == std::string("ABW")); //-V521
    CHECK(j[0]["name"].as<std::string>() == std::string("ARUBA")); //-V521
    CHECK(j[1]["country_code"].as<std::string>() == std::string("ATF")); //-V521
    CHECK(j[1]["name"].as<std::string>() == std::string("FRENCH SOUTHERN TERRITORIES, D.R. OF")); //-V521
    CHECK(j[2]["country_code"].as<std::string>() == std::string("VUT")); //-V521
    CHECK(j[2]["name"].as<std::string>() == std::string("VANUATU")); //-V521
    CHECK(j[3]["country_code"].as<std::string>() == std::string("WLF")); //-V521
    CHECK(j[3]["name"].as<std::string>() == std::string("WALLIS & FUTUNA ISLANDS")); //-V521

    //std::cout << pretty_print(j) << '\n';
}

TEST_CASE("Test decode_csv, terminating newline")
{
    std::string data = "some label\nsome value\nanother value\n";

    SECTION("From string")
    {
        auto options = csv::csv_options{}
            .assume_header(true);
        auto j = csv::decode_csv<json>(data,options);
        REQUIRE(j.is_array()); //-V521
        REQUIRE(j.size() == 2); //-V521
        CHECK(j[0]["some label"].as<std::string>() == std::string("some value")); //-V521
        CHECK(j[1]["some label"].as<std::string>() == std::string("another value")); //-V521
    }

    SECTION("From stream")
    {
        std::stringstream is(data);

        auto options = csv::csv_options{}
            .assume_header(true);
        auto j = csv::decode_csv<json>(is,options);
        REQUIRE(j.is_array()); //-V521
        REQUIRE(j.size() == 2); //-V521
        CHECK(j[0]["some label"].as<std::string>() == std::string("some value")); //-V521
        CHECK(j[1]["some label"].as<std::string>() == std::string("another value")); //-V521
    }

    SECTION("m_columns")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::m_columns);
        auto j = csv::decode_csv<json>(data,options);
        REQUIRE(j.is_object()); //-V521
        REQUIRE(j.size() == 1); //-V521
        CHECK(j["some label"][0].as<std::string>() == std::string("some value")); //-V521
        CHECK(j["some label"][1].as<std::string>() == std::string("another value")); //-V521
    }
}

TEST_CASE("Test decode_csv, no terminating newline")
{
    std::string data = "some label\nsome value\nanother value";

    SECTION("From string")
    {
        auto options = csv::csv_options{}
            .assume_header(true);
        auto j = csv::decode_csv<json>(data,options);
        REQUIRE(j.is_array()); //-V521
        REQUIRE(j.size() == 2); //-V521
        CHECK(j[0]["some label"].as<std::string>() == std::string("some value")); //-V521
        CHECK(j[1]["some label"].as<std::string>() == std::string("another value")); //-V521
    }

    SECTION("From stream")
    {
        std::stringstream is(data);

        auto options = csv::csv_options{}
            .assume_header(true);
        auto j = csv::decode_csv<json>(is,options);
        REQUIRE(j.is_array()); //-V521
        REQUIRE(j.size() == 2); //-V521
        CHECK(j[0]["some label"].as<std::string>() == std::string("some value")); //-V521
        CHECK(j[1]["some label"].as<std::string>() == std::string("another value")); //-V521
    }

    SECTION("m_columns")
    {
        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::m_columns);
        auto j = csv::decode_csv<json>(data,options);
        REQUIRE(j.is_object()); //-V521
        REQUIRE(j.size() == 1); //-V521
        CHECK(j["some label"][0].as<std::string>() == std::string("some value")); //-V521
        CHECK(j["some label"][1].as<std::string>() == std::string("another value")); //-V521
    }
}

TEST_CASE("test encode_csv")
{
    json j(json_array_arg);
    j.push_back(json(json_object_arg, { {"a",1},{"b",2} }));

    SECTION("To stream")
    {
        auto options = csv::csv_options{}
            .assume_header(true);
        std::stringstream ss;
        csv::encode_csv(j, ss, options);

        auto j2 = csv::decode_csv<json>(ss, options);

        REQUIRE(j2.is_array()); //-V521
        REQUIRE(j2.size() == 1); //-V521
        CHECK(j2[0]["a"].as<int>() == 1); //-V521
        CHECK(j2[0]["b"].as<int>() == 2); //-V521
    }
}

TEST_CASE("test_type_inference")
{
    const std::string input = R"(customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,0272561313,01001,0.05,431.65
"Jane Doe",false,416-272-2561,55416,0.15,480.70
"Joe Bloggs",false,"4162722561","55416",0.15,300.70
"John Smith",FALSE,NULL,22313-1450,0.15,300.70
)";

    SECTION("n_rows")
    {
        auto expected = ojson::parse(R"(
[
    ["customer_name", "has_coupon", "phone_number", "zip_code", "sales_tax_rate", "total_amount"],
    ["John Roe", true, "0272561313", "01001", 0.05, 431.65],
    ["Jane Doe", false, "416-272-2561", 55416, 0.15, 480.7],
    ["Joe Bloggs", false, "4162722561", "55416", 0.15, 300.7],
    ["John Smith", false, null, "22313-1450", 0.15, 300.7]
]
        )");

        auto options = csv::csv_options{}
            .mapping_kind(csv::csv_mapping_kind::n_rows);

        ojson j = csv::decode_csv<ojson>(input,options);
        REQUIRE(expected == j); //-V521
    }

    SECTION("n_objects")
    {
        auto expected = ojson::parse(R"(
[
    {
        "customer_name": "John Roe",
        "has_coupon": true,
        "phone_number": "0272561313",
        "zip_code": "01001",
        "sales_tax_rate": 0.05,
        "total_amount": 431.65
    },
    {
        "customer_name": "Jane Doe",
        "has_coupon": false,
        "phone_number": "416-272-2561",
        "zip_code": 55416,
        "sales_tax_rate": 0.15,
        "total_amount": 480.7
    },
    {
        "customer_name": "Joe Bloggs",
        "has_coupon": false,
        "phone_number": "4162722561",
        "zip_code": "55416",
        "sales_tax_rate": 0.15,
        "total_amount": 300.7
    },
    {
        "customer_name": "John Smith",
        "has_coupon": false,
        "phone_number": null,
        "zip_code": "22313-1450",
        "sales_tax_rate": 0.15,
        "total_amount": 300.7
    }
]
        )");

        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::n_objects);
        ojson j = csv::decode_csv<ojson>(input,options);

        REQUIRE(expected == j); //-V521
    }
    
    SECTION("m_columns")
    {
        auto expected = ojson::parse(R"(
{
    "customer_name": ["John Roe", "Jane Doe", "Joe Bloggs", "John Smith"],
    "has_coupon": [true, false, false, false],
    "phone_number": ["0272561313", "416-272-2561", "4162722561", null],
    "zip_code": ["01001", 55416, "55416", "22313-1450"],
    "sales_tax_rate": [0.05, 0.15, 0.15, 0.15],
    "total_amount": [431.65, 480.7, 300.7, 300.7]
}
        )");

        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::m_columns);
        ojson j = csv::decode_csv<ojson>(input,options);

        REQUIRE(expected == j); //-V521
    }
}

TEST_CASE("csv_options lossless_number")
{
    const std::string input = R"(index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";

    auto options = csv::csv_options{}
        .assume_header(true)
           .mapping_kind(csv::csv_mapping_kind::n_objects)
           .trim(true)
           .lossless_number(true);

    ojson j = csv::decode_csv<ojson>(input,options);
    REQUIRE(j.size() == 3); //-V521
    CHECK(j[0]["rate"].tag() == semantic_tag::bigdec); //-V521
    CHECK((j[0]["rate"].as<std::string>() == "0.0000214")); //-V521
    CHECK(j[1]["rate"].tag() == semantic_tag::bigdec); //-V521
    CHECK((j[1]["rate"].as<std::string>() == "0.0000143")); //-V521
    CHECK(j[2]["rate"].tag() == semantic_tag::bigdec); //-V521
    CHECK((j[2]["rate"].as<std::string>() == "0.0000001")); //-V521
}

// Test case contributed by karimhm
TEST_CASE("csv detect bom")
{
    const std::string input = "\xEF\xBB\xBFstop_id,stop_name,stop_lat,stop_lon,location_type,parent_station\n"
"\"8220B007612\",\"Hotel Merrion Street\",\"53\",\"-6\",\"\",\"\"";

    auto options = csv::csv_options{}
        .assume_header(true);
    
    std::istringstream is(input);
    ojson j = csv::decode_csv<ojson>(is, options);
    REQUIRE(j.size() == 1); //-V521
    ojson station = j[0];
    
    JSONCONS_TRY {
        auto it = station.find("stop_id");
        REQUIRE((it != station.object_range().end())); //-V521
        std::string stop_id = it->value().as<std::string>();
        CHECK((stop_id == "8220B007612")); //-V521
    } JSONCONS_CATCH (const std::exception& e) {
        std::cout << e.what() << '\n';
    }
}
/*
#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1 
 
#include <common/free_list_allocator.hpp>
#include <scoped_allocator>
 
template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

TEST_CASE("csv_reader constructors")
{
    const std::string input = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

    SECTION("stateful allocator")
    {
        using cust_json = basic_json<char,sorted_policy,MyScopedAllocator<char>>;

        MyScopedAllocator<char> my_allocator{1}; 

        auto options = csv::csv_options{}
            .assume_header(true)
               .mapping_kind(csv::csv_mapping_kind::n_objects);

        json_decoder<cust_json,MyScopedAllocator<char>> decoder(my_allocator,
                                                              my_allocator);
        csv::basic_csv_reader<char,string_source<char>,MyScopedAllocator<char>> reader(input, decoder, options, my_allocator);
        reader.read();

        cust_json j = decoder.get_result();
        CHECK(j.size() == 3); //-V521
        //std::cout << pretty_print(j) << "\n";
    }
}
#endif
*/

TEST_CASE("infinite loop")
{
    SECTION("Whitespace follows quoted field")
    {
        char data[4] = {'\"', '\"', ' ', '\n'};
        int size = 4;
        std::string input(data, size);
        json_decoder<ojson> decoder;
        auto options = csv::csv_options{}
            .assume_header(true)
            .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_string_reader reader(input, decoder, options);
        std::error_code ec;
        reader.read(ec);
        CHECK(!ec); //-V521
    }
    SECTION("Invalid character follows quoted field")
    {
        char data[4] = {'\"', '\"', '\x01', '\n'};
        int size = 4;
        std::string input(data, size);
        json_decoder<ojson> decoder;
        auto options = csv::csv_options{}
            .assume_header(true)
            .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_string_reader reader(input, decoder, options);
        std::error_code ec;
        reader.read(ec);
        CHECK(ec == csv::csv_errc::unexpected_char_between_fields); //-V521
    }
}

TEST_CASE("csv_parser number detection")
{

    SECTION("test 1")
    {
        std::string data = R"(Number
5001173100
5E10
5E-10
3e05
3e-05
5001173100E95978)";
        auto options = jsoncons::csv::csv_options{}
            .assume_header(true)
               .field_delimiter(',');

        const auto csv = jsoncons::csv::decode_csv<jsoncons::json>(data, options);

        CHECK(5001173100 == csv[0].at("Number").as<int64_t>());
        CHECK(5E10       == csv[1].at("Number").as<double>());
        CHECK(5E-10      == Approx(csv[2].at("Number").as<double>()));
        CHECK(3e5        == csv[3].at("Number").as<double>());
        CHECK(3e-5       == Approx(csv[4].at("Number").as<double>()));
        CHECK(std::numeric_limits<double>::infinity() == csv[5].at("Number").as<double>()); // infinite
    }

    SECTION("test 2")
    {
        std::string data = R"(Number
5001173100
5e10
5e-10
3e05
3e-05
5001173100E95978)";
        auto options = jsoncons::csv::csv_options{}
            .field_delimiter(',')
            .header_lines(1)
            .mapping_kind(jsoncons::csv::csv_mapping_kind::n_rows);

        auto result = jsoncons::csv::decode_csv<std::vector<std::vector<double>>>(data, options);

        CHECK(5001173100 == result[0][0]);
        CHECK(5E10       == result[1][0]);
        CHECK(5E-10      == Approx(result[2][0]));
        CHECK(3e5        == result[3][0]);
        CHECK(3e-5       == Approx(result[4][0]));
        CHECK(std::numeric_limits<double>::infinity() == result[5][0]); // infinite
    }
}

