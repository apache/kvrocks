### `jsoncons::basic_json::dump`

```cpp
template <CharContainer>
void dump(CharContainer& cont,
    const basic_json_encode_options<char_type>& options 
        = basic_json_encode_options<char_type>(),
    indenting = indenting::no_indent) const;                            (1)

template <CharContainer>
void dump(CharContainer& cont, indenting indent) const;                 

void dump(std::basic_ostream<char_type>& os, 
    const basic_json_encode_options<char_type>& options 
        = basic_json_encode_options<char_type>(),
    indenting = indenting::no_indent) const;                            (2)

void dump(std::basic_ostream<char_type>& os, indenting indent) const;   

template <CharContainer>
void dump_pretty(CharContainer& cont,
    const basic_json_encode_options<char_type>& options 
        = basic_json_encode_options<char_type>()) const;                (3)

void dump_pretty(std::basic_ostream<char_type>& os, 
    const basic_json_encode_options<char_type>& options 
        = basic_json_encode_options<char_type>()) const;                (4)

void dump(basic_json_visitor<char_type>& visitor) const;                (5)
```

(1) Dumps a json value to a character container with "minified" output.

(2) Dumps a json value to an output stream with "minified" output.

Functions (3)-(4) are identical to (1)-(2) except indenting is on.

(5) Dumps a json value to the specified [visitor](../basic_json_visitor.md).

#### Exceptions

Throws [ser_error](ser_error.md) if there is a serialization error. 

### Examples

#### Dump json value to csv file

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

using namespace jsoncons;

int main()
{
    const json books = json::parse(R"(
    [
        {
            "title" : "Kafka on the Shore",
            "author" : "Haruki Murakami",
            "price" : 25.17
        },
        {
            "title" : "Women: A Novel",
            "author" : "Charles Bukowski",
            "price" : 12.00
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer"
        }
    ]
    )");

    auto options = csv_options{}          
        .column_names("author,title,price");

    csv_stream_encoder encoder(std::cout, options);

    books.dump(encoder);
}
```

Output:

```csv
author,title,price
Haruki Murakami,Kafka on the Shore,25.17
Charles Bukowski,Women: A Novel,12.0
Ivan Passer,Cutter's Way,
```

#### Dump json content into a larger document

```cpp
#include <jsoncons/json.hpp>

using namespace jsoncons;

int main()
{
    const json some_books = json::parse(R"(
    [
        {
            "title" : "Kafka on the Shore",
            "author" : "Haruki Murakami",
            "price" : 25.17
        },
        {
            "title" : "Women: A Novel",
            "author" : "Charles Bukowski",
            "price" : 12.00
        }
    ]
    )");

    const json more_books = json::parse(R"(
    [
        {
            "title" : "A Wild Sheep Chase: A Novel",
            "author" : "Haruki Murakami",
            "price" : 9.01
        },
        {
            "title" : "Cutter's Way",
            "author" : "Ivan Passer",
            "price" : 8.00
        }
    ]
    )");

    json_stream_encoder encoder(std::cout); // pretty print
    encoder.begin_array();
    for (const auto& book : some_books.array_range())
    {
        book.dump(encoder);
    }
    for (const auto& book : more_books.array_range())
    {
        book.dump(encoder);
    }
    encoder.end_array();
    encoder.flush();
}
```

Output:

```json
[
    {
        "author": "Haruki Murakami",
        "price": 25.17,
        "title": "Kafka on the Shore"
    },
    {
        "author": "Charles Bukowski",
        "price": 12.0,
        "title": "Women: A Novel"
    },
    {
        "author": "Haruki Murakami",
        "price": 9.01,
        "title": "A Wild Sheep Chase: A Novel"
    },
    {
        "author": "Ivan Passer",
        "price": 8.0,
        "title": "Cutter's Way"
    }
]
```
