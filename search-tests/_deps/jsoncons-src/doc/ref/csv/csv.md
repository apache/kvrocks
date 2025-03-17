### csv extension

The csv extension implements decode from and encode to the [CSV format](https://www.rfc-editor.org/rfc/rfc4180.txt)

[decode_csv](decode_csv.md)

[basic_csv_cursor](basic_csv_cursor.md)

[encode_csv](encode_csv.md)

[basic_csv_options](basic_csv_options.md)

[basic_csv_reader](basic_csv_reader.md)

[basic_csv_encoder](basic_csv_encoder.md)

### Working with CSV data

For the examples below you need to include some header files and initialize a string of CSV data:

```cpp
#include <iomanip>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

    const std::string data = R"(index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";
```

jsoncons allows you to work with the CSV data similarly to JSON data:

- As a variant-like data structure, [basic_json](../basic_json.md) 

- As a strongly typed C++ data structure that implements [json_type_traits](../json_type_traits.md)

- With [cursor-level access](doc/ref/csv/basic_csv_cursor.md) to a stream of parse events

#### As a variant-like data structure

```cpp
int main()
{
    auto options = csv::csv_options{}
        .assume_header(true);

    // Parse the CSV data into an ojson value
    ojson j = csv::decode_csv<ojson>(data, options);

    // Pretty print
    auto print_options = json_options{}
        .float_format(float_chars_format::fixed);
    std::cout << "(1)\n" << pretty_print(j, print_options) << "\n\n";

    // Iterate over the rows
    std::cout << "(2)\n";
    for (const auto& row : j.array_range())
    {
        // Access rated as string and rating as double
        std::cout << row["index_id"].as<std::string>() << ", " 
                  << row["observation_date"].as<std::string>() << ", " 
                  << row["rate"].as<double>() << "\n";
    }
}
```
Output:
```
(1)
[
    {
        "index_id": "EUR_LIBOR_06M",
        "observation_date": "2015-10-23",
        "rate": 0.0000214
    },
    {
        "index_id": "EUR_LIBOR_06M",
        "observation_date": "2015-10-26",
        "rate": 0.0000143
    },
    {
        "index_id": "EUR_LIBOR_06M",
        "observation_date": "2015-10-27",
        "rate": 0.0000001
    }
]

(2)
EUR_LIBOR_06M, 2015-10-23, 0.0000214
EUR_LIBOR_06M, 2015-10-26, 0.0000143
EUR_LIBOR_06M, 2015-10-27, 0.0000001
```

#### As a strongly typed C++ data structure

jsoncons supports transforming CSV data into C++ data structures. The functions decode_csv and encode_csv convert strings or streams of 
CSV data to C++ data structures and back. Decode and encode work for all C++ classes that have [json_type_traits](../json_type_traits.md) defined. 
jsoncons already supports many types in the standard library, and your own types will be supported too if you specialize [json_type_traits](../json_type_traits.md) 
in the jsoncons namespace.
```cpp
#include <boost/date_time/gregorian/gregorian.hpp>

namespace ns {

    class fixing
    {
        std::string index_id_;
        boost::gregorian::date observation_date_;
        double rate_;
    public:
        fixing(const std::string& index_id, boost::gregorian::date observation_date, double rate)
            : index_id_(index_id), observation_date_(observation_date), rate_(rate)
        {
        }

        const std::string& index_id() const {return  index_id_;}

        boost::gregorian::date observation_date() const {return  observation_date_;}

        double rate() const {return rate_;}
    };

} // namespace ns

template <typename Json>
struct json_type_traits<Json,boost::gregorian::date>
{
    static bool is(const Json& val) noexcept
    {
        if (!val.is_string())
            return false;
        try
        {
            std::string s = val.template as<std::string>();
            boost::gregorian::from_simple_string(s);
            return true;
        }
        catch (...)
        {
            return false;
        }
    }
    static boost::gregorian::date as(const Json& val)
    {
        std::string s = val.template as<std::string>();
        return boost::gregorian::from_simple_string(s);
    }
    static Json to_json(boost::gregorian::date val, 
                        Json::allocator_type alloc = Json::allocator_type())
    {
        return Json(to_iso_extended_string(val), alloc);
    }
};

JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::fixing, index_id, observation_date, rate)

int main()
{
    auto options = csv::csv_options{}
        .assume_header(true)
        .float_format(float_chars_format::fixed);

    // Decode the CSV data into a c++ structure
    std::vector<ns::fixing> v = csv::decode_csv<std::vector<ns::fixing>>(data, options);

    // Iterate over values
    std::cout << std::fixed << std::setprecision(7);
    std::cout << "(1)\n";
    for (const auto& item : v)
    {
        std::cout << item.index_id() << ", " << item.observation_date() << ", " << item.rate() << "\n";
    }

    // Encode the c++ structure into CSV data
    std::string s;
    csv::encode_csv(v, s, options);
    std::cout << "(2)\n";
    std::cout << s << "\n";
}
```
Output:
```
(1)
EUR_LIBOR_06M, 2015-10-23, 0.0000214
EUR_LIBOR_06M, 2015-10-26, 0.0000143
EUR_LIBOR_06M, 2015-10-27, 0.0000001
(2)
index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
```

#### With cursor-level access

```cpp
int main()
{
    auto options = csv::csv_options{}
        .assume_header(true);
    csv::csv_string_cursor cursor(data, options);

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
```
Output:
```
begin_array
begin_object
key: index_id
string_value: EUR_LIBOR_06M
key: observation_date
string_value: 2015-10-23
key: rate
double_value: 0.0000214
end_object
begin_object
key: index_id
string_value: EUR_LIBOR_06M
key: observation_date
string_value: 2015-10-26
key: rate
double_value: 0.0000143
end_object
begin_object
key: index_id
string_value: EUR_LIBOR_06M
key: observation_date
string_value: 2015-10-27
key: rate
double_value: 0.0000001
end_object
end_array
```

You can use a [staj_array_iterator](../staj_array_iterator.md) to group the CSV parse events into [basic_json](../basic_json.md) records:
```cpp
int main()
{
    auto options = csv::csv_options{}
        .assume_header(true);

    csv::csv_string_cursor cursor(data, options);

    auto view = staj_array<ojson>(cursor);

    auto print_options = json_options{}
        .float_format(float_chars_format::fixed);
    for (const auto& item : view)
    {
        std::cout << pretty_print(item, print_options) << "\n";
    }
}
```
Output:
```
{
    "index_id": "EUR_LIBOR_06M",
    "observation_date": "2015-10-23",
    "rate": 0.0000214
}
{
    "index_id": "EUR_LIBOR_06M",
    "observation_date": "2015-10-26",
    "rate": 0.0000143
}
{
    "index_id": "EUR_LIBOR_06M",
    "observation_date": "2015-10-27",
    "rate": 0.0000001
}
```

Or into strongly typed records:
```cpp

int main()
{
    using record_type = std::tuple<std::string,std::string,double>;

    auto options = csv::csv_options{}
        .assume_header(true);
    csv::csv_string_cursor cursor(data, options);

    auto view = staj_array<record_type>(cursor);

    std::cout << std::fixed << std::setprecision(7);
    for (const auto& record : view)
    {
        std::cout << std::get<0>(record) << ", " << std::get<1>(record) << ", " << std::get<2>(record) << "\n";
    }
}
```
Output
```
EUR_LIBOR_06M, 2015-10-23, 0.0000214
EUR_LIBOR_06M, 2015-10-26, 0.0000143
EUR_LIBOR_06M, 2015-10-27, 0.0000001
```
