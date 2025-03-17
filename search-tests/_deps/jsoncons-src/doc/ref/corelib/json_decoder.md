### jsoncons::json_decoder

```cpp
#include <jsoncons/json_decoder.hpp>

template <typename Json,class TempAllocator>
json_decoder
```

![json_decoder](./diagrams/json_decoder.png)

#### Member types

Member type                         |Definition
------------------------------------|------------------------------
`result_allocator_type`|Json::allocator_type (until 0.171.0)
`allocator_type`|Json::allocator_type (since 0.171.0)
`temp_allocator_type`|TempAllocator

#### Constructors

    json_decoder(result_allocator_arg_t, 
        const result_allocator_type& alloc);                            (1)   (until 0.171.0)

    json_decoder(result_allocator_arg_t, 
        const result_allocator_type& alloc, 
        const temp_allocator_type& temp_alloc);                         (1)   (until 0.171.0)

    json_decoder(const allocator_type& alloc = allocator_type(),         
        const temp_allocator_type& temp_alloc = temp_allocator_type()); (1)   (since 0.171.0)

    json_decoder(const temp_allocator_type& temp_alloc = 
        temp_allocator_type());                                         (2)   (until 0.171.0)

    json_decoder(temp_allocator_arg_t, 
        const temp_allocator_type& temp_alloc);                         (2)   (since 0.171.0)

#### Member functions

    allocator_type get_allocator() const
Returns the allocator associated with the json value.

    bool is_valid() const
Checks if the decoder contains a valid `basic_json` value. 
Initially `is_valid()` is false, becomes `true` when a `basic_json`
value has been received, and becomes false when `get_result()` is called.

    Json get_result()
Returns the json value `v` stored in the decoder as `std::move(v)`.
If before calling this function `is_valid()` is false, an `assertion_error` is thrown.
After `get_result()` is called, 'is_valid()' becomes false.
Once the result has been retrieved, `get_result` cannot be called again until
another `basic_json` value has been received.

### Examples

#### Decode a JSON text using stateful result and work allocators

Input JSON file `book_catalog.json`:

```json
[ 
  { 
      "author" : "Haruki Murakami",
      "title" : "Hard-Boiled Wonderland and the End of the World",
      "isbn" : "0679743464",
      "publisher" : "Vintage",
      "date" : "1993-03-02",
      "price": 18.90
  },
  { 
      "author" : "Graham Greene",
      "title" : "The Comedians",
      "isbn" : "0099478374",
      "publisher" : "Vintage Classics",
      "date" : "2005-09-21",
      "price": 15.74
  }
]
```

```cpp
int main()
{
    // Given allocator my_alloc with a single-argument constructor my_alloc(int),
    // use my_alloc(1) to allocate basic_json memory, my_alloc(2) to allocate
    // working memory used by json_decoder, and my_alloc(3) to allocate
    // working memory used by basic_json_reader. 

    using my_json = basic_json<char,sorted_policy,my_alloc>;

    std::ifstream is("book_catalog.json");
    json_decoder<my_json,my_alloc> decoder(my_alloc(1),my_alloc(2));
    basic_json_reader<char,stream_source<char>,my_alloc> reader(is, decoder, my_alloc(3));
    reader.read();

    my_json j = decoder.get_result();
    std::cout << pretty_print(j) << "\n";
}
```
Output:
```
[
    {
        "author": "Haruki Murakami",
        "date": "1993-03-02",
        "isbn": "0679743464",
        "price": 18.9,
        "publisher": "Vintage",
        "title": "Hard-Boiled Wonderland and the End of the World"
    },
    {
        "author": "Graham Greene",
        "date": "2005-09-21",
        "isbn": "0099478374",
        "price": 15.74,
        "publisher": "Vintage Classics",
        "title": "The Comedians"
    }
]
```

#### Decode a CSV text using stateful result and work allocators

Input CSV file `bond_yields.csv`:

```
Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
```

```cpp
int main()
{
    // Given allocator my_alloc with a single-argument constructor my_alloc(int),
    // use my_alloc(1) to allocate basic_json memory, my_alloc(2) to allocate
    // working memory used by json_decoder, and my_alloc(3) to allocate
    // working memory used by basic_csv_reader. 

    using my_json = basic_json<char,sorted_policy,my_alloc>;

    auto options = csv::csv_options{}
        .assume_header(true);

    std::ifstream is("bond_yields.csv");
    json_decoder<my_json,my_alloc> decoder(my_alloc(1),my_alloc(2));
    csv::basic_csv_reader<char,stream_source<char>,free_list_allocator<char>> reader(is, decoder, options, my_alloc(3));
    reader.read();

    my_json j = decoder.get_result();
    std::cout << pretty_print(j) << "\n";
}
```
Output:
```
[
    {
        "1Y": 0.0062,
        "2Y": 0.0075,
        "3Y": 0.0083,
        "5Y": 0.011,
        "Date": "2017-01-09"
    },
    {
        "1Y": 0.0063,
        "2Y": 0.0076,
        "3Y": 0.0084,
        "5Y": 0.0112,
        "Date": "2017-01-08"
    },
    {
        "1Y": 0.0063,
        "2Y": 0.0076,
        "3Y": 0.0084,
        "5Y": 0.0112,
        "Date": "2017-01-08"
    }
]
```

### See also

[basic_json_visitor](basic_json_visitor.md)

