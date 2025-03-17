#include <jsoncons_ext/msgpack/msgpack.hpp>
#include <jsoncons_ext/msgpack/msgpack_reader.hpp>

#include <jsoncons/json.hpp>
#include <sstream>

using namespace jsoncons;
using namespace jsoncons::msgpack;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
    std::string s(reinterpret_cast<const char*>(data), size);
    std::istringstream is(s);
    std::vector<uint8_t> s1;
    msgpack_bytes_encoder encoder(s1);
    msgpack_stream_reader reader(is, encoder);

    std::error_code ec;
    reader.read(ec);

    return 0;
}
