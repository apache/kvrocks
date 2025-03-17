// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons_ext/bson/bson.hpp>
#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <jsoncons_ext/ubjson/ubjson.hpp>

#include <jsoncons/json.hpp>

#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <iomanip>

using namespace jsoncons;

#if (defined(__GNUC__) || defined(__clang__)) && defined(JSONCONS_HAS_INT128) 
    void int128_example()
    {
        json j1("-18446744073709551617", semantic_tag::bigint);
        std::cout << j1 << "\n\n";

        __int128 val = j1.as<__int128>();

        json j2(val);

        assert(j2 == j1);
    }
#endif

int main()
{
    std::cout << "\njson traits integer examples\n\n";

#if (defined(__GNUC__) || defined(__clang__)) && defined(JSONCONS_HAS_INT128) 
    int128_example();
#endif

    std::cout << '\n';
}

