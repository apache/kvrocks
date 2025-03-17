// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json_cursor.hpp>
#include <jsoncons/json.hpp> // json_decoder and json
#include <string>
#include <sstream>
#include <fstream>

using namespace jsoncons;

namespace {
namespace ns {

    struct book
    {
        std::string author;
        std::string title;
        double price;
    };

} // namespace ns
} // namespace

JSONCONS_ALL_MEMBER_TRAITS(ns::book,author,title,price)

// Create some JSON (push)
void create_some_json()
{
    std::ofstream os("./output/book_catalog.json", 
                     std::ios_base::out | std::ios_base::trunc);
    assert(os);

    compact_json_stream_encoder encoder(os); // no indent

    encoder.begin_array();
    encoder.begin_object();
    encoder.key("author");
    encoder.string_value("Haruki Murakami");
    encoder.key("title");
    encoder.string_value("Hard-Boiled Wonderland and the End of the World");
    encoder.key("price");
    encoder.double_value(18.9);
    encoder.end_object();
    encoder.begin_object();
    encoder.key("author");
    encoder.string_value("Graham Greene");
    encoder.key("title");
    encoder.string_value("The Comedians");
    encoder.key("price");
    encoder.double_value(15.74);
    encoder.end_object();
    encoder.end_array();
    encoder.flush();

    os.close();

    // Read the JSON and write it prettified to std::cout
    json_stream_encoder writer(std::cout); // indent

    std::ifstream is("./output/book_catalog.json");
    assert(is);

    json_stream_reader reader(is, writer);
    reader.read();
    std::cout << "\n\n";
}

// Read some JSON (pull)

// In the example, the application pulls the next event in the 
// JSON input stream by calling next().

void read_json_parse_events()
{
    std::ifstream is("./output/book_catalog.json");
    assert(is);

    json_stream_cursor cursor(is);

    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::end_array:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::begin_object:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::end_object:
                std::cout << event.event_type() << " " << "\n";
                break;
            case staj_event_type::key:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                break;
            case staj_event_type::string_value:
                // Or std::string_view, if supported
                std::cout << event.event_type() << ": " << event.get<jsoncons::string_view>() << "\n";
                break;
            case staj_event_type::null_value:
                std::cout << event.event_type() << "\n";
                break;
            case staj_event_type::bool_value:
                std::cout << event.event_type() << ": " << std::boolalpha << event.get<bool>() << "\n";
                break;
            case staj_event_type::int64_value:
                std::cout << event.event_type() << ": " << event.get<int64_t>() << "\n";
                break;
            case staj_event_type::uint64_value:
                std::cout << event.event_type() << ": " << event.get<uint64_t>() << "\n";
                break;
            case staj_event_type::double_value:
                std::cout << event.event_type() << ": " << event.get<double>() << "\n";
                break;
            default:
                std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                break;
        }
    }
}

// Filtering the stream
void filtering_a_json_stream()
{
    bool author_next = false;
    auto filter = [&](const staj_event& event, const ser_context&) -> bool
    {
        if (event.event_type() == staj_event_type::key &&
            event.get<jsoncons::string_view>() == "author")
        {
            author_next = true;
            return false;
        }
        if (author_next)
        {
            author_next = false;
            return true;
        }
        return false;
    };

    std::ifstream is("./output/book_catalog.json");
    assert(is);

    json_stream_cursor cursor(is);
    auto filtered_c = cursor | filter;

    for (; !filtered_c.done(); filtered_c.next())
    {
        const auto& event = filtered_c.current();
        switch (event.event_type())
        {
            case staj_event_type::string_value:
                std::cout << event.get<jsoncons::string_view>() << "\n";
                break;
            default:
                std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                break;
        }
    }
}

void read_nested_objects_to_basic_json()
{
    std::ifstream is("./output/book_catalog.json");
    assert(is);

    json_stream_cursor cursor(is);

    json_decoder<json> decoder;
    for (; !cursor.done(); cursor.next())
    {
        const auto& event = cursor.current();
        switch (event.event_type())
        {
            case staj_event_type::begin_array:
            {
                std::cout << event.event_type() << " " << "\n";
                break;
            }
            case staj_event_type::end_array:
            {
                std::cout << event.event_type() << " " << "\n";
                break;
            }
            case staj_event_type::begin_object:
            {
                std::cout << event.event_type() << " " << "\n";
                cursor.read_to(decoder);
                json j = decoder.get_result();
                std::cout << pretty_print(j) << "\n";
                break;
            }
            default:
            {
                std::cout << "Unhandled event type: " << event.event_type() << " " << "\n";
                break;
            }
        }
    }
}

void iterate_over_complete_objects1()
{
    std::ifstream is("./output/book_catalog.json");
    assert(is);

    json_stream_cursor cursor(is);

    auto view = staj_array<json>(cursor);
    for (const auto& j : view)
    {
        std::cout << pretty_print(j) << "\n";
    }
}

void iterate_over_complete_objects2()
{
    std::ifstream is("./output/book_catalog.json");
    assert(is);

    json_stream_cursor cursor(is);

    auto view = staj_array<ns::book>(cursor);
    for (const auto& book : view)
    {
        std::cout << book.author << ", " << book.title << "\n";
    }
}

int main()
{
    std::cout << "\njson_cursor examples\n\n";

    std::cout << "\n";
    create_some_json();
    read_json_parse_events();
    filtering_a_json_stream();
    read_nested_objects_to_basic_json();
    iterate_over_complete_objects1();
    iterate_over_complete_objects2();

    std::cout << "\n";
}

