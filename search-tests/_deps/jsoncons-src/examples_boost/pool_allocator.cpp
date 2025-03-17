// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <boost/pool/pool_alloc.hpp>
#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cstddef>

using namespace jsoncons;

using cust_json = basic_json<char,sorted_policy,boost::pool_allocator<char>>;

void pool_allocator_examples()
{
    std::cout << "pool_allocator examples\n\n";

    jsoncons::json_decoder<cust_json,boost::pool_allocator<char>> decoder;

    static std::string s("[1,2,3,4,5,6]");
    std::istringstream is(s);

    basic_json_reader<char,stream_source<char>,boost::pool_allocator<char>> reader(is,decoder); 
    reader.read();

    cust_json j = decoder.get_result();

    std::cout << j << '\n';
}

