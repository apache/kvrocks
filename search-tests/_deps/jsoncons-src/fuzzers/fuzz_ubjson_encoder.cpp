#include <jsoncons_ext/ubjson/ubjson.hpp>
#include <jsoncons_ext/ubjson/ubjson_reader.hpp>
#include <jsoncons/json.hpp>
#include <sstream>

using namespace jsoncons;
using namespace jsoncons::ubjson;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
    std::string s(reinterpret_cast<const char*>(data), size);
    std::istringstream is(s);
    std::vector<uint8_t> s1;
    ubjson_bytes_encoder encoder(s1);
    ubjson_stream_reader reader(is, encoder);

    std::error_code ec;
    reader.read(ec);

    return 0;
}
