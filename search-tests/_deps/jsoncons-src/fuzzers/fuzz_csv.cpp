#include <stdio.h>

#include <jsoncons_ext/csv/csv.hpp>

#include <jsoncons/json.hpp>
#include <jsoncons/json_reader.hpp>

using namespace jsoncons;
extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
        std::string input(reinterpret_cast<const char*>(data), size);
        json_decoder<ojson> decoder;
        auto options = csv::csv_options{}
            .assume_header(true)
            .mapping_kind(csv::csv_mapping_kind::n_rows);
        try {
                csv::csv_string_reader reader1(input, decoder, options);
                reader1.read();
        }
        catch (jsoncons::ser_error e) {}
        catch (jsoncons::json_runtime_error<std::runtime_error> e) {}
        catch (json_runtime_error<std::invalid_argument> e3) {}
        return 0;
}
