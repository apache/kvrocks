// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <stdexcept>
#include <string>
#include <vector>
#include <map>
#include <fstream>
#include <jsoncons/json.hpp>
#include "free_list_allocator.hpp"
#include <scoped_allocator>

using namespace jsoncons;

template <typename T>
using MyScopedAllocator = std::scoped_allocator_adaptor<free_list_allocator<T>>;

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

void custom_iterator_source()
{
    char source[] = {'[','\"', 'f','o','o','\"',',','\"', 'b','a','r','\"',']'};

    MyIterator first(source);
    MyIterator last(source + sizeof(source));

    json j = json::parse(first, last);

    std::cout << j << "\n\n";
}

void read_mulitple_json_objects()
{
    std::ifstream is("./input/multiple-json-objects.json");
    if (!is.is_open())
    {
        throw std::runtime_error("Cannot open file");
    }

    json_decoder<json> decoder;
    json_stream_reader reader(is, decoder);

    while (!reader.eof())
    {
        reader.read_next();
        
        // until 1.0.0
        //if (!reader.eof())
        //{
        //    json j = decoder.get_result();
        //    std::cout << j << '\n';
        //}       
        // since 1.0.0
        json j = decoder.get_result();
        std::cout << j << '\n';
    }
}

// https://jsonlines.org/
void read_json_lines()
{
    std::string data = R"(
["Name", "Session", "Score", "Completed"]
["Gilbert", "2013", 24, true]
["Alexa", "2013", 29, true]
["May", "2012B", 14, false]
["Deloise", "2012A", 19, true] 
        )";

    std::stringstream is(data);
    json_decoder<json> decoder;
    json_stream_reader reader(is, decoder);

    while (!reader.eof())
    {
        reader.read_next();
        if (!reader.eof())
        {
            json j = decoder.get_result();
            std::cout << j << '\n';
        }
    }
}

void read_with_stateful_allocator()
{
    using cust_json = basic_json<char,sorted_policy, MyScopedAllocator<char>>;
    std::string input = R"(
[ 
  { 
      "author" : "Haruki Murakami",
      "title" : "Hard-Boiled Wonderland and the End of the World",
      "isbn" : "0679743464",
      "publisher" : "Vintage",
      "date" : "1993-03-02",
      "price": 18.90
  },
  { 
      "author" : "Graham Greene",
      "title" : "The Comedians",
      "isbn" : "0099478374",
      "publisher" : "Vintage Classics",
      "date" : "2005-09-21",
      "price": 15.74
  }
]
)";

    // Until 0.171.0
    //json_decoder<cust_json, MyScopedAllocator<char>> decoder(result_allocator_arg, MyScopedAllocator<char>(1),
    //    MyScopedAllocator<char>(2));

    // Since 0.171.0
    json_decoder<cust_json, MyScopedAllocator<char>> decoder(MyScopedAllocator<char>(1),
        MyScopedAllocator<char>(2));

    auto myAlloc = MyScopedAllocator<char>(3);

    basic_json_reader<char,string_source<char>, MyScopedAllocator<char>> reader(input, decoder, myAlloc);
    reader.read();

    cust_json j = decoder.get_result();
    std::cout << pretty_print(j) << "\n";
}

int main()
{
    std::cout << "\njson_reader examples\n\n";

    //read_mulitple_json_objects();
    //read_with_stateful_allocator();
    //custom_iterator_source();
    read_json_lines();
}
