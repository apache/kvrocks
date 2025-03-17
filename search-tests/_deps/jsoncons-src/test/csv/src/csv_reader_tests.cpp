// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/csv/csv_reader.hpp>
#include <jsoncons/json.hpp>

#include <catch/catch.hpp>

namespace csv = jsoncons::csv; 

TEST_CASE("test csv_reader buffered read")
{
    SECTION("test 1")
    {
        const std::string bond_yields = R"(Date,1Y,2Y,3Y,5Y
    2017-01-09,0.0062,0.0075,0.0083,0.011
    2017-01-08,0.0063,0.0076,0.0084,0.0112
    2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

        jsoncons::json_decoder<jsoncons::ojson> decoder;
        auto options = csv::csv_options{}
            .assume_header(true)
            .mapping_kind(csv::csv_mapping_kind::n_rows);
        csv::csv_string_reader reader(bond_yields,decoder,options);
        reader.read();
        auto val = decoder.get_result();
        std::cout << jsoncons::pretty_print(val) << "\n";
        CHECK(val.size() == 4);
    }
}


