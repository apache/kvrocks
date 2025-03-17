#include <jsoncons_ext/cbor/cbor.hpp>
#include <jsoncons_ext/cbor/cbor_reader.hpp>

#include <jsoncons/json.hpp>

#include <sstream>

using namespace jsoncons;
using namespace jsoncons::cbor;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
        std::string input(reinterpret_cast<const char*>(data), size);
        std::istringstream is(input);
        try {
                json j2 = decode_cbor<json>(is);
        }
        catch(const jsoncons::ser_error&) {}
        catch(jsoncons::json_runtime_error<std::runtime_error> e2) {}

        return 0;
}
