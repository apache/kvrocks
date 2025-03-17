#include <jsoncons/json_parser.hpp>
#include <jsoncons/json.hpp>

using namespace jsoncons;

extern "C" int LLVMFuzzerTestOneInput(const uint8_t *data, std::size_t size)
{
    std::string s(reinterpret_cast<const char*>(data), size);
    std::istringstream is(s);

    default_json_visitor visitor;
    auto options = json_options{}
        .max_nesting_depth(std::numeric_limits<int>::max());

    json_stream_reader reader(is, visitor, options);
    std::error_code ec;
    reader.read(ec);

    return 0;
}
