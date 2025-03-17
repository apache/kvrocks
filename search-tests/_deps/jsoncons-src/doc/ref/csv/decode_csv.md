### jsoncons::csv::decode_csv

Decodes a [comma-separated variables (CSV)](https://en.wikipedia.org/wiki/Comma-separated_values) data format into a C++ data structure.

```cpp
#include <jsoncons_ext/csv/csv.hpp>

template <typename T,class Source>
T decode_csv(const Source& s, 
    const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>()));                          (1)

template <typename T,class CharT>
T decode_csv(std::basic_istream<CharT>& is, 
    const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>()));                          (2)

template <typename T,class InputIt>
T decode_csv(InputIt first, InputIt last,
    const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>()));                          (3) (since 0.153.0)

template <typename T,typename Source,typename Allocator,typename TempAllocator>
T decode_csv(allocator_set<Allocator,TempAllocator> alloc_set,
    const Source& s,
    const basic_csv_decode_options<Source::value_type>& options = basic_csv_decode_options<Source::value_type>()); (4)

template <typename T,typename CharT,typename Allocator,typename TempAllocator>
T decode_csv(allocator_set<Allocator,TempAllocator> alloc_set,
    std::basic_istream<CharT>& is,
    const basic_csv_decode_options<CharT>& options = basic_csv_decode_options<CharT>());                           (5)
```

(1) Reads CSV data from a contiguous character sequence into a type T, using the specified (or defaulted) [options](basic_csv_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

(2) Reads CSV data from an input stream into a type T, using the specified (or defaulted) [options](basic_csv_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

(3) Reads CSV data from the range [`first`,`last`) into a type T, using the specified (or defaulted) [options](basic_csv_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md).

Functions (4)-(5) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument and
provides allocators for result data and temporary allocations.

#### Return value

Returns a value of type `T`.

#### Exceptions

Throws a [ser_error](../ser_error.md) if parsing fails, and a [conv_error](conv_error.md) if type conversion fails.

### Examples

#### Decode a CSV file with type inference (default)

Example file (sales.csv)
```csv
customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,0272561313,01001,0.05,431.65
"Jane Doe",false,416-272-2561,55416,0.15,480.70
"Joe Bloggs",false,"4162722561","55416",0.15,300.70
"John Smith",FALSE,NULL,22313-1450,0.15,300.70
```

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>
#include <fstream>

using namespace jsoncons;

int main()
{
    auto options = csv::csv_options{}
        .assume_header(true)
        .mapping_kind(csv::csv_mapping_kind::n_objects);

    std::ifstream is1("input/sales.csv");
    ojson j1 = csv::decode_csv<ojson>(is1,options);
    std::cout << "\n(1)\n"<< pretty_print(j1) << "\n";

    options.mapping_kind(csv::csv_mapping_kind::n_rows);
    std::ifstream is2("input/sales.csv");
    ojson j2 = csv::decode_csv<ojson>(is2,options);
    std::cout << "\n(2)\n"<< pretty_print(j2) << "\n";

    options.mapping_kind(csv::csv_mapping_kind::m_columns);
    std::ifstream is3("input/sales.csv");
    ojson j3 = csv::decode_csv<ojson>(is3,options);
    std::cout << "\n(3)\n"<< pretty_print(j3) << "\n";
}
```
Output:
```json
(1)
[
    {
        "customer_name": "John Roe",
        "has_coupon": true,
        "phone_number": "0272561313",
        "zip_code": "01001",
        "sales_tax_rate": 0.05,
        "total_amount": 431.65
    },
    {
        "customer_name": "Jane Doe",
        "has_coupon": false,
        "phone_number": "416-272-2561",
        "zip_code": 55416,
        "sales_tax_rate": 0.15,
        "total_amount": 480.7
    },
    {
        "customer_name": "Joe Bloggs",
        "has_coupon": false,
        "phone_number": "4162722561",
        "zip_code": "55416",
        "sales_tax_rate": 0.15,
        "total_amount": 300.7
    },
    {
        "customer_name": "John Smith",
        "has_coupon": false,
        "phone_number": null,
        "zip_code": "22313-1450",
        "sales_tax_rate": 0.15,
        "total_amount": 300.7
    }
]

(2)
[
    ["customer_name","has_coupon","phone_number","zip_code","sales_tax_rate","total_amount"],
    ["John Roe",true,"0272561313","01001",0.05,431.65],
    ["Jane Doe",false,"416-272-2561",55416,0.15,480.7],
    ["Joe Bloggs",false,"4162722561","55416",0.15,300.7],
    ["John Smith",false,null,"22313-1450",0.15,300.7]
]

(3)
{
    "customer_name": ["John Roe","Jane Doe","Joe Bloggs","John Smith"],
    "has_coupon": [true,false,false,false],
    "phone_number": ["0272561313","416-272-2561",4162722561,null],
    "zip_code": ["01001",55416,55416,"22313-1450"],
    "sales_tax_rate": [0.05,0.15,0.15,0.15],
    "total_amount": [431.65,480.7,300.7,300.7]
}
```

#### Decode a CSV string without type inference

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

using namespace jsoncons;

int main()
{
    std::string s = R"(employee-no,employee-name,dept,salary
00000001,"Smith,Matthew",sales,150000.00
00000002,"Brown,Sarah",sales,89000.00
)";

    auto options = csv::csv_options{}
        .assume_header(true)
        .infer_types(false);
    ojson j = csv::decode_csv<ojson>(s,options);

    std::cout << pretty_print(j) << '\n';
}
```
Output:
```json
[
    {
        "employee-no": "00000001",
        "employee-name": "Smith,Matthew",
        "dept": "sales",
        "salary": "150000.00"
    },
    {
        "employee-no": "00000002",
        "employee-name": "Brown,Sarah",
        "dept": "sales",
        "salary": "89000.00"
    }
]
```

#### Decode a CSV string with specified types

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

using namespace jsoncons;

int main()
{
    const std::string s = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

    auto options = csv::csv_options{}
        .assume_header(true)
        .column_types("string,float,float,float,float");

    // csv_mapping_kind::n_objects
    options.mapping_kind(csv::csv_mapping_kind::n_objects);
    ojson j1 = csv::decode_csv<ojson>(s,options);
    std::cout << "\n(1)\n"<< pretty_print(j1) << "\n";

    // csv_mapping_kind::n_rows
    options.mapping_kind(csv::csv_mapping_kind::n_rows);
    ojson j2 = csv::decode_csv<ojson>(s,options);
    std::cout << "\n(2)\n"<< pretty_print(j2) << "\n";

    // csv_mapping_kind::m_columns
    options.mapping_kind(csv::csv_mapping_kind::m_columns);
    ojson j3 = csv::decode_csv<ojson>(s,options);
    std::cout << "\n(3)\n" << pretty_print(j3) << "\n";
}
```
Output:
```json
(1)
[
    {
        "Date": "2017-01-09",
        "1Y": 0.0062,
        "2Y": 0.0075,
        "3Y": 0.0083,
        "5Y": 0.011
    },
    {
        "Date": "2017-01-08",
        "1Y": 0.0063,
        "2Y": 0.0076,
        "3Y": 0.0084,
        "5Y": 0.0112
    },
    {
        "Date": "2017-01-08",
        "1Y": 0.0063,
        "2Y": 0.0076,
        "3Y": 0.0084,
        "5Y": 0.0112
    }
]

(2)
[
    ["Date","1Y","2Y","3Y","5Y"],
    ["2017-01-09",0.0062,0.0075,0.0083,0.011],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112],
    ["2017-01-08",0.0063,0.0076,0.0084,0.0112]
]

(3)
{
    "Date": ["2017-01-09","2017-01-08","2017-01-08"],
    "1Y": [0.0062,0.0063,0.0063],
    "2Y": [0.0075,0.0076,0.0076],
    "3Y": [0.0083,0.0084,0.0084],
    "5Y": [0.011,0.0112,0.0112]
}
```
#### Decode a CSV string with multi-valued fields separated by subfield delimiters

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

using namespace jsoncons;

int main()
{
    const std::string s = R"(calculationPeriodCenters,paymentCenters,resetCenters
NY;LON,TOR,LON
NY,LON,TOR;LON
"NY";"LON","TOR","LON"
"NY","LON","TOR";"LON"
)";
    auto print_options = json_options{}
        .array_array_line_splits(line_split_kind::same_line);

    auto options1 = csv::csv_options{}
        .assume_header(true)
        .subfield_delimiter(';');

    json j1 = csv::decode_csv<json>(s,options1);
    std::cout << "(1)\n" << pretty_print(j1,print_options) << "\n\n";

    auto options2 = csv::csv_options{}       
        .mapping_kind(csv::csv_mapping_kind::n_rows)
        .subfield_delimiter(';');

    json j2 = csv::decode_csv<json>(s,options2);
    std::cout << "(2)\n" << pretty_print(j2,print_options) << "\n\n";

    auto options3 = csv::csv_options{}
        assume_header(true)
        .mapping_kind(csv::csv_mapping_kind::m_columns)
        .subfield_delimiter(';');

    json j3 = csv::decode_csv<json>(s,options3);
    std::cout << "(3)\n" << pretty_print(j3,print_options) << "\n\n";
}
```
Output:
```json
(1)
[

    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    },
    {
        "calculationPeriodCenters": ["NY","LON"],
        "paymentCenters": "TOR",
        "resetCenters": "LON"
    },
    {
        "calculationPeriodCenters": "NY",
        "paymentCenters": "LON",
        "resetCenters": ["TOR","LON"]
    }
]

(2)
[

    ["calculationPeriodCenters","paymentCenters","resetCenters"],
    [["NY","LON"],"TOR","LON"],
    ["NY","LON",["TOR","LON"]],
    [["NY","LON"],"TOR","LON"],
    ["NY","LON",["TOR","LON"]]
]

(3)
{
    "calculationPeriodCenters": [["NY","LON"],"NY",["NY","LON"],"NY"],
    "paymentCenters": ["TOR","LON","TOR","LON"],
    "resetCenters": ["LON",["TOR","LON"],"LON",["TOR","LON"]]
}
```

#### Convert a CSV source to a C++ data structure that satisfies [json_type_traits](../json_type_traits.md) requirements, and back

```cpp
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>
#include <iostream>

using namespace jsoncons;

int main()
{
    const std::string input = R"(Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
)";

    auto ioptions = csv::csv_options{}
        .header_lines(1)
        .mapping_kind(csv::csv_mapping_kind::n_rows);

    using table_type = std::vector<std::tuple<std::string,double,double,double,double>>;

    table_type table = csv::decode_csv<table_type>(input,ioptions);

    std::cout << "(1)\n";
    for (const auto& row : table)
    {
        std::cout << std::get<0>(row) << "," 
                  << std::get<1>(row) << "," 
                  << std::get<2>(row) << "," 
                  << std::get<3>(row) << "," 
                  << std::get<4>(row) << "\n";
    }
    std::cout << "\n";

    std::string output;

    auto ooptions = csv::csv_options{}
        .column_names("Date,1Y,2Y,3Y,5Y");
    csv::encode_csv<table_type>(table, output, ooptions);

    std::cout << "(2)\n";
    std::cout << output << "\n";
}
```
Output:
```
(1)
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.011
2017-01-08,0.0063,0.0076,0.0084,0.011

(2)
Date,1Y,2Y,3Y,5Y
2017-01-09,0.0062,0.0075,0.0083,0.011
2017-01-08,0.0063,0.0076,0.0084,0.0112
2017-01-08,0.0063,0.0076,0.0084,0.0112
```

