// Copyright 2013-2024 Daniel Parker
// Distributed under Boost license

#include <string>
#include <jsoncons/json.hpp>
#include <iomanip>
#include <assert.h>
#include <boost/multiprecision/cpp_int.hpp>

using namespace jsoncons;
namespace boost_mp = boost::multiprecision;

void serialization_example1()
{
    json val = json::parse(R"(
{
    "sfm_data_version": "0.2",
    "root_path": "D:\\Lagring\\Plugg\\Examensarbete\\Data\\images",
    "views": [],
    "intrinsics": [],
    "extrinsics": [
        {
            "key": 0,
            "value": {
                "rotation": [
                    [
                        0.89280214808572156,
                        0.35067276062587932,
                        -0.28272413998197254
                    ],
                    [
                        -0.090429686592667424,
                        0.75440463553446824,
                        0.65015084224113584
                    ],
                    [
                        0.44127859245183554,
                        -0.5548894131618759,
                        0.70524530697098287
                    ]
                ],
                "center": [
                    -0.60959634064871249,
                    0.24123645392011658,
                    0.57783384588917808
                ]
            }
        }
    ]
}   

)");

    std::cout << "Default pretty print" << std::endl;
    std::cout << pretty_print(val) << std::endl;


    std::cout << "array_array_line_splits(line_split_kind::new_line)" << std::endl;
    std::cout << "array_object_line_splits(line_split_kind::new_line)" << std::endl;

    auto options = json_options{}
        .array_array_line_splits(line_split_kind::new_line)
        .array_object_line_splits(line_split_kind::new_line);
    std::cout << pretty_print(val,options) << std::endl;
}

void serialization_example2()
{

    json val;

    val["verts"] = json(json_array_arg, {1, 2, 3});
    val["normals"] = json(json_array_arg, {1, 0, 1});
    val["uvs"] = json(json_array_arg, {0, 0, 1, 1});

    std::cout << "Default object-array same line options" << std::endl;
    std::cout << pretty_print(val) << std::endl;

    std::cout << "object_array_line_splits(line_split_kind::same_line)" << std::endl;
    auto options1 = json_options{}
        .object_array_line_splits(line_split_kind::same_line);
    std::cout << pretty_print(val,options1) << std::endl;

    std::cout << "object_array_line_splits(line_split_kind::new_line)" << std::endl;
    auto options2 = json_options{}
        .object_array_line_splits(line_split_kind::new_line);
    std::cout << pretty_print(val,options2 ) << std::endl;

    std::cout << "object_array_line_splits(line_split_kind::multi_line)" << std::endl;
    auto options3 = json_options{}
        .object_array_line_splits(line_split_kind::multi_line);
    std::cout << pretty_print(val,options3) << std::endl;
}

void serialization_example3()
{
    {
        json val = json::parse(R"(
        [
            {"first-name" : "John",
             "last-name" : "Doe"},
            {"first-name" : "Jane",
             "last-name" : "Doe"}
        ]
        )");

        auto options = json_options{}
            .array_object_line_splits(line_split_kind::same_line);
        std::cout << "array_object_line_splits(line_split_kind::same_line)" << std::endl;
        std::cout << pretty_print(val,options) << std::endl;
    }

    {
        json val = json::parse(R"({
           "verts" : [1, 2, 3],

           "normals" : [1, 0, 1],

           "uvs" : [ 0, 0, 1, 1 ]
        }
    )");
        std::cout << "Default print" << std::endl;
        std::cout << print(val) << std::endl;

        std::cout << "Default pretty print" << std::endl;
        std::cout << pretty_print(val) << std::endl;

        auto options1 = json_options{}
            .array_array_line_splits(line_split_kind::same_line);
        std::cout << pretty_print(val,options1) << std::endl;

        auto options = json_options{}
            .object_object_line_splits(line_split_kind::new_line);;
        std::cout << pretty_print(val,options) << std::endl;
    }

    {
        json val2 = json::parse(R"(
        {
       "data":
       {
           "item": [[2],[4,5,2,3],[4],[4,5,2,3],[2],[4,5,3],[2],[4,3]],    //A two-dimensional array 
                                                                                               //blank line
           "id": [0,1,2,3,4,5,6,7]                                                   //A one-dimensional array 
       }
        }
    )");

        std::cout << "Default" << std::endl;
        std::cout << pretty_print(val2) << std::endl;
     
        std::cout << "array_array_line_splits(line_split_kind::new_line)" << std::endl;
        auto options2 = json_options{}
            .array_array_line_splits(line_split_kind::new_line);
        std::cout << pretty_print(val2,options2 ) << std::endl;

        std::cout << "array_array_line_splits(line_split_kind::same_line)" << std::endl;
        auto options4 = json_options{}
            .array_array_line_splits(line_split_kind::same_line);
        std::cout << pretty_print(val2, options4) << std::endl;

        std::cout << "array_array_line_splits(line_split_kind::same_line)" << std::endl;
        auto options5 = json_options{}
            .array_array_line_splits(line_split_kind::same_line);
        std::cout << pretty_print(val2, options5) << std::endl;
    }

    json val3 = json::parse(R"(
    {
   "data":
   {
       "item": [[2]]    //A two-dimensional array 
   }
    }
)");
    std::cout << "array_array_line_splits(line_split_kind::new_line)" << std::endl;
    auto options6 = json_options{}
        .array_array_line_splits(line_split_kind::new_line);
    std::cout << pretty_print(val3,options6) << std::endl;
}

void serialization_example4()
{
    json val;
    val["data"]["id"] = json(json_array_arg, {0,1,2,3,4,5,6,7});
    val["data"]["item"] = json(json_array_arg, {json(json_array_arg, {2}),
                                      json(json_array_arg, {4,5,2,3}),
                                      json(json_array_arg, {4}),
                                      json(json_array_arg, {4,5,2,3}),
                                      json(json_array_arg, {2}),
                                      json(json_array_arg, {4,5,3}),
                                      json(json_array_arg, {2}),
                                      json(json_array_arg, {4,3})});

    std::cout << "Default array-array split line options" << std::endl;
    std::cout << pretty_print(val) << std::endl;

    std::cout << "Array-array same line options" << std::endl;
    auto options1 = json_options{}
        .array_array_line_splits(line_split_kind::same_line);
    std::cout << pretty_print(val, options1) << std::endl;

    std::cout << "object_array_line_splits(line_split_kind::new_line)" << std::endl;
    std::cout << "array_array_line_splits(line_split_kind::same_line)" << std::endl;
    auto options2 = json_options{}
        .object_array_line_splits(line_split_kind::new_line)
        .array_array_line_splits(line_split_kind::same_line);
    std::cout << pretty_print(val, options2 ) << std::endl;

    std::cout << "object_array_line_splits(line_split_kind::new_line)" << std::endl;
    std::cout << "array_array_line_splits(line_split_kind::multi_line)" << std::endl;
    auto options3 = json_options{}
        .object_array_line_splits(line_split_kind::new_line)
        .array_array_line_splits(line_split_kind::multi_line);
    std::cout << pretty_print(val, options3) << std::endl;

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
        std::cout << "Default" << std::endl;
        std::cout << pretty_print(val) << std::endl;

        std::string style1 = "array_array_line_splits(line_split_kind:same_line)";
        std::cout << style1 << std::endl;
        auto options1 = json_options{}
            .array_array_line_splits(line_split_kind::same_line);
        std::cout << pretty_print(val,options1) << std::endl;

        std::string style2 = "array_array_line_splits(line_split_kind::new_line)";
        std::cout << style2 << std::endl;
        auto options2 = json_options{}
            .array_array_line_splits(line_split_kind::new_line);
        std::cout << pretty_print(val,options2 ) << std::endl;

        std::string style3 = "array_array_line_splits(line_split_kind::multi_line)";
        std::cout << style3 << std::endl;
        auto options3 = json_options{}
            .array_array_line_splits(line_split_kind::multi_line);
        std::cout << pretty_print(val,options3) << std::endl;

        std::string style4 = "object_array_line_splits(line_split_kind:same_line)";
        std::cout << style4 << std::endl;
        auto options4 = json_options{}
            .object_array_line_splits(line_split_kind::same_line);
        std::cout << pretty_print(val,options4) << std::endl;

        std::string style5 = "object_array_line_splits(line_split_kind::new_line)";
        std::cout << style5 << std::endl;
        auto options5 = json_options{}
            .object_array_line_splits(line_split_kind::new_line);
        std::cout << pretty_print(val,options5) << std::endl;

        std::string style6 = "object_array_line_splits(line_split_kind::multi_line)";
        std::cout << style6 << std::endl;
        auto options6 = json_options{}
            .object_array_line_splits(line_split_kind::multi_line);
        std::cout << pretty_print(val,options6) << std::endl;
    }
}

void dump_json_fragments()
{
    const json some_books = json::parse(R"(
    [
        {
            "title" : "Kafka on the Shore",
            "author" : "Haruki Murakami",
            "price" : 25.17
        },
        {
            "title" : "Women: A Novel",
            "author" : "Charles Bukowski",
            "price" : 12.00
        }
    ]
    )");

    const json more_books = json::parse(R"(
    [
        {
            "title" : "A Wild Sheep Chase: A Novel",
            "author" : "Haruki Murakami",
            "price" : 9.01
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer",
            "price" : 8.00
        }
    ]
    )");

    json_stream_encoder encoder(std::cout); // pretty print
    encoder.begin_array();
    for (const auto& book : some_books.array_range())
    {
        book.dump(encoder);
    }
    for (const auto& book : more_books.array_range())
    {
        book.dump(encoder);
    }
    encoder.end_array();
    encoder.flush();
}

void nan_inf_replacement()
{
    json j;
    j["field1"] = std::sqrt(-1.0);
    j["field2"] = 1.79e308 * 1000;
    j["field3"] = -1.79e308 * 1000;

    auto options = json_options{}
        .nan_to_str("NaN")
        .inf_to_str("Inf");

    std::ostringstream os;
    os << pretty_print(j, options);

    std::cout << "(1)\n" << os.str() << std::endl;

    json j2 = json::parse(os.str(),options);

    std::cout << "\n(2) " << j2["field1"].as<double>() << std::endl;
    std::cout << "(3) " << j2["field2"].as<double>() << std::endl;
    std::cout << "(4) " << j2["field3"].as<double>() << std::endl;

    std::cout << "\n(5)\n" << pretty_print(j2,options) << std::endl;
}

void bignum_serialization_examples1()
{
    std::string s = "-18446744073709551617";

    json j(bigint::from_string(s.c_str()));

    std::cout << "(default) ";
    j.dump(std::cout);
    std::cout << "\n\n";

    std::cout << "(integer) ";
    auto options1 = json_options{}
        .bigint_format(bigint_chars_format::number);
    j.dump(std::cout, options1);
    std::cout << "\n\n";

    std::cout << "(base64) ";
    auto options3 = json_options{}
        .bigint_format(bigint_chars_format::base64);
    j.dump(std::cout, options3);
    std::cout << "\n\n";

    std::cout << "(base64url) ";
    auto options4 = json_options{}
        .bigint_format(bigint_chars_format::base64url);
    j.dump(std::cout, options4);
    std::cout << "\n\n";
}

void bignum_serialization_examples2()
{
    std::string s = "-18446744073709551617";

    json j = json::parse(s);

    std::cout << "(1) ";
    j.dump(std::cout);
    std::cout << "\n\n";

    std::cout << "(2) ";
    auto options1 = json_options{}
        .bigint_format(bigint_chars_format::number);
    j.dump(std::cout, options1);
    std::cout << "\n\n";

    std::cout << "(3) ";
    auto options2 = json_options{}
        .bigint_format(bigint_chars_format::base64url);
    j.dump(std::cout, options2);
    std::cout << "\n\n";
}

void bignum_access_examples()
{
    std::string input = "-18446744073709551617";

    json j = json::parse(input);

    // Access as string
    std::string s = j.as<std::string>();
    std::cout << "(1) " << s << "\n\n";

    // Access as double
    double d = j.as<double>();
    std::cout << "(2) " << std::setprecision(17) << d << "\n\n";

    // Access as jsoncons::bigint
    jsoncons::bigint bn = j.as<jsoncons::bigint>();
    std::cout << "(3) " << bn << "\n\n";

    // If your compiler supports extended integral types
#if (defined(__GNUC__) || defined(__clang__)) && defined(JSONCONS_HAS_INT128)
    __int128 i = j.as<__int128>();
    boost_mp::int128_t boost_i = static_cast<boost_mp::int128_t>(i);
    std::cout << "(4) " << boost_i << "\n\n";
#endif
}

void decimal_precision_examples()
{
    std::string s = R"(
    {
        "a" : 12.00,
        "b" : 1.23456789012345678901234567890
    }
    )";

    // Default
    json j = json::parse(s);

    std::cout.precision(15);

    // Access as string
    std::cout << "(1) a: " << j["a"].as<std::string>() << ", b: " << j["b"].as<std::string>() << "\n"; 
    // Access as double
    std::cout << "(2) a: " << j["a"].as<double>() << ", b: " << j["b"].as<double>() << "\n\n"; 

    // Using lossless_number option
    auto options = json_options{}
        .lossless_number(true);

    json j2 = json::parse(s, options);
    // Access as string
    std::cout << "(3) a: " << j2["a"].as<std::string>() << ", b: " << j2["b"].as<std::string>() << "\n";
    // Access as double
    std::cout << "(4) a: " << j2["a"].as<double>() << ", b: " << j2["b"].as<double>() << "\n\n"; 
}

void chinese_char()
{
    jsoncons::json j;

    std::string s = (const char*)u8"你好";
    j.try_emplace("hello", s);
    assert(j["hello"].as<std::string>() == s);

    std::string json_string;
    j.dump(json_string);
}

#ifdef __cpp_char8_t

void chinese_uchar8_t()
{
    jsoncons::json j;

    std::u8string s = u8"你好";
    j.try_emplace("hello", s);
    assert(j["hello"].as<std::u8string>() == s);

    std::string json_string;
    j.dump(json_string);
}

#endif

int main()
{
    std::cout << "\nSerialization examples\n\n";
    serialization_example1();
    serialization_example2();
    serialization_example3();
    serialization_example4();
    dump_json_fragments();
    nan_inf_replacement();
    bignum_serialization_examples2();
    bignum_serialization_examples1();
    decimal_precision_examples();
    bignum_access_examples(); 
    chinese_char();
    #ifdef __cpp_char8_t
        chinese_uchar8_t();
    #endif
    std::cout << std::endl;
}

