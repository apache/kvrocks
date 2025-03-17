// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif

#include <jsoncons_ext/csv/csv.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_reader.hpp>

#include <map>
#include <vector>
#include <catch/catch.hpp>

using namespace jsoncons;

namespace 
{
    class MyIterator
    {
        const char* p_;
    public:
        using iterator_category = std::input_iterator_tag;
        using value_type = char;
        using difference_type = std::ptrdiff_t;
        using pointer = const char*; 
        using reference = const char&;

        MyIterator(const char* p)
            : p_(p)
        {
        }

        reference operator*() const
        {
            return *p_;
        }

        pointer operator->() const 
        {
            return p_;
        }

        MyIterator& operator++()
        {
            ++p_;
            return *this;
        }

        MyIterator operator++(int) 
        {
            MyIterator temp(*this);
            ++*this;
            return temp;
        }

        bool operator!=(const MyIterator& rhs) const
        {
            return p_ != rhs.p_;
        }
    };

} // namespace

TEST_CASE("encode decode csv source")
{
    using cpp_type = std::vector<std::tuple<std::string,int>>;
    std::string input = "\"a\",1\n\"b\",2";
    auto options = csv::csv_options{}
        .mapping_kind(csv::csv_mapping_kind::n_rows)
        .assume_header(false);

    SECTION("from string")
    {
        cpp_type v = csv::decode_csv<cpp_type>(input, options);
        REQUIRE(v.size() == 2); //-V521
        CHECK(std::get<0>(v[0]) == "a"); //-V521
        CHECK(std::get<1>(v[0]) == 1); //-V521
        CHECK(std::get<0>(v[1]) == "b"); //-V521
        CHECK(std::get<1>(v[1]) == 2); //-V521

        std::string s2;
        csv::encode_csv(v, s2, options);

        json j1 = csv::decode_csv<json>(input);
        json j2 = csv::decode_csv<json>(s2);

        CHECK(j2 == j1); //-V521

        json j3 = csv::decode_csv<json>(s2.begin(), s2.end());
        CHECK(j3 == j1); //-V521
    }

    SECTION("from stream")
    {
        std::stringstream is(input);
        cpp_type v = csv::decode_csv<cpp_type>(is, options);
        REQUIRE(v.size() == 2); //-V521
        CHECK(std::get<0>(v[0]) == "a"); //-V521
        CHECK(std::get<1>(v[0]) == 1); //-V521
        CHECK(std::get<0>(v[1]) == "b"); //-V521
        CHECK(std::get<1>(v[1]) == 2); //-V521

        std::stringstream ss2;
        csv::encode_csv(v, ss2, options);

        json j1 = csv::decode_csv<json>(input);

        json j2 = csv::decode_csv<json>(ss2);
        CHECK(j2 == j1); //-V521
    }

    SECTION("from iterator")
    {
        cpp_type v = csv::decode_csv<cpp_type>(input.begin(), input.end(), options);
        REQUIRE(v.size() == 2); //-V521
        CHECK(std::get<0>(v[0]) == "a"); //-V521
        CHECK(std::get<1>(v[0]) == 1); //-V521
        CHECK(std::get<0>(v[1]) == "b"); //-V521
        CHECK(std::get<1>(v[1]) == 2); //-V521

        std::stringstream ss2;
        csv::encode_csv(v, ss2, options);

        json j1 = csv::decode_csv<json>(input);
        json j2 = csv::decode_csv<json>(ss2);

        CHECK(j2 == j1); //-V521
    }

    SECTION("from custom iterator")
    {
        MyIterator it(input.data());
        MyIterator end(input.data() + input.length());

        cpp_type v = csv::decode_csv<cpp_type>(it, end, options);
        REQUIRE(v.size() == 2); //-V521
        CHECK(std::get<0>(v[0]) == "a"); //-V521
        CHECK(std::get<1>(v[0]) == 1); //-V521
        CHECK(std::get<0>(v[1]) == "b"); //-V521
        CHECK(std::get<1>(v[1]) == 2); //-V521

        std::stringstream ss2;
        csv::encode_csv(v, ss2, options);

        json j1 = csv::decode_csv<json>(input);
        json j2 = csv::decode_csv<json>(ss2);

        CHECK(j2 == j1); //-V521
    }
}


namespace
{

struct csv_string_encoder_reset_test_fixture
{
    std::string output1;
    std::string output2;
    csv::csv_string_encoder encoder;

    csv_string_encoder_reset_test_fixture()
        : encoder(output1, csv::csv_options().assume_header(true))
    {}

    std::string string1() const {return output1;}
    std::string string2() const {return output2;}
};

struct csv_stream_encoder_reset_test_fixture
{
    std::ostringstream output1;
    std::ostringstream output2;
    csv::csv_stream_encoder encoder;

    csv_stream_encoder_reset_test_fixture()
        : encoder(output1, csv::csv_options().assume_header(true))
    {}

    std::string string1() const {return output1.str();}
    std::string string2() const {return output2.str();}
};

} // namespace

TEMPLATE_TEST_CASE("test_csv_encoder_reset", "",
                   csv_string_encoder_reset_test_fixture,
                   csv_stream_encoder_reset_test_fixture)
{
    using fixture_type = TestType;
    fixture_type f;

    // Parially encode, reset, then fully encode to same sink
    f.encoder.begin_array();
        f.encoder.begin_array();
            f.encoder.string_value("h1");
            f.encoder.string_value("h2");
        f.encoder.end_array();
        f.encoder.begin_array();
            f.encoder.uint64_value(1);
        // Missing column and array end
    f.encoder.flush();

    CHECK("h1,h2\n1" == f.string1()); // streaming case
    f.encoder.reset();
    f.encoder.begin_array();
        f.encoder.begin_array();
            f.encoder.string_value("h3");
            f.encoder.string_value("h4");
        f.encoder.end_array();
        f.encoder.begin_array();
            f.encoder.uint64_value(3);
            f.encoder.uint64_value(4);
        f.encoder.end_array();
    f.encoder.end_array();
    f.encoder.flush();
    CHECK("h1,h2\n1h3,h4\n3,4\n" == f.string1()); // streaming case

    // Reset and encode to different sink
    f.encoder.reset(f.output2);
    f.encoder.begin_array();
        f.encoder.begin_array();
            f.encoder.string_value("h5");
            f.encoder.string_value("h6");
        f.encoder.end_array();
        f.encoder.begin_array();
            f.encoder.uint64_value(5);
            f.encoder.uint64_value(6);
        f.encoder.end_array();
    f.encoder.end_array();
    f.encoder.flush();
    CHECK("h5,h6\n5,6\n" == f.string2());
}

namespace { namespace ns {

    struct Person
    {
        std::string name;
    };

}}

JSONCONS_ALL_MEMBER_TRAITS(ns::Person, name)

#if defined(JSONCONS_HAS_STATEFUL_ALLOCATOR) && JSONCONS_HAS_STATEFUL_ALLOCATOR == 1

#include <scoped_allocator>
#include <common/free_list_allocator.hpp>

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

TEST_CASE("encode_csv allocator_set overloads")
{
    MyScopedAllocator<char> temp_alloc(1);

    auto alloc_set = temp_allocator_only(temp_alloc);

    json persons(json_array_arg);

    json person(json_object_arg);
    person.try_emplace("name", "John Smith");

    persons.emplace_back(std::move(person));

    SECTION("json, stream")
    {
        std::string s;
        std::stringstream ss(s);

        auto options = jsoncons::csv::csv_options{}
            .assume_header(true);
        options.mapping_kind(jsoncons::csv::csv_mapping_kind::n_objects);
        csv::encode_csv(/*alloc_set,*/ persons, ss, options);
        json other = csv::decode_csv<json>(/*alloc_set,*/ ss, options);
        CHECK(other == persons);
    }
    SECTION("custom, stream")
    {
        std::string s;
        std::stringstream ss(s);
        auto options = jsoncons::csv::csv_options{}
            .assume_header(true);
        options.mapping_kind(jsoncons::csv::csv_mapping_kind::n_objects);
        csv::encode_csv(/*alloc_set,*/ persons, ss, options);
        auto other = csv::decode_csv<std::vector<ns::Person>>(/*alloc_set,*/ ss, options);
        REQUIRE(other.size() == 1);
        CHECK(other[0].name == persons[0].at("name").as_string());
    }
}

#endif
