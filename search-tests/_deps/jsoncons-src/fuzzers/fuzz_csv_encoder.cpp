#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons_ext/csv/csv_reader.hpp>
#include <jsoncons/json.hpp>

using namespace jsoncons;
using namespace jsoncons::csv;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
    std::string s(reinterpret_cast<const char*>(data), size);
    std::istringstream is(s);

    std::string s2;
    csv_string_encoder visitor(s2);
    csv_stream_reader reader(is, visitor);
    std::error_code ec;
    reader.read(ec);

    return 0;
}
