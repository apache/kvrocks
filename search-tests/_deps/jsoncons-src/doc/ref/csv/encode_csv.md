### jsoncons::csv::encode_csv

Encodes a C++ data structure into the CSV data format.

```cpp
#include <jsoncons_ext/csv/csv.hpp>

template <typename T,class CharContainer>
void encode_csv(const T& val, CharContainer& cont, 
    const basic_csv_encode_options<CharContainer::value_type>& options 
        = basic_csv_encode_options<CharContainer::value_type>());           (1)

template <typename T,typename CharT>
void encode_csv(const T& val, std::basic_ostream<CharT>& os, 
    const basic_csv_encode_options<CharT>& options 
        = basic_csv_encode_options<CharT>());                               (2)

template <typename T,class CharContainer>
void encode_csv(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& val, CharContainer& cont, 
    const basic_csv_encode_options<CharContainer::value_type>& options 
        = basic_csv_encode_options<CharContainer::value_type>());           (3) (since 0.171.0)

template <typename T,typename CharT>
void encode_csv(const allocator_set<Allocator,TempAllocator>& alloc_set,
    const T& val, std::basic_ostream<CharT>& os, 
    const basic_csv_encode_options<CharT>& options 
        = basic_csv_encode_options<CharT>());                               (4) (since 0.171.0)
```

(1) Writes a value of type T into a character container in the CSV data format, using the specified (or defaulted) [options](basic_csv_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 

(2) Writes a value of type T into an output stream in the CSV data format, using the specified (or defaulted) [options](basic_csv_options.md). 
Type 'T' must be an instantiation of [basic_json](../basic_json.md) 
or support [json_type_traits](../json_type_traits.md). 

Functions (3)-(4) are identical to (1)-(2) except an [allocator_set](../allocator_set.md) is passed as an additional argument.

### Examples

[Encode a JSON array of "flat" objects (n_objects format)](#eg1)  
[Encode a JSON array of "flat" arrays (n_rows format)](#eg2)  
[Encode a JSON object of name-array members (m_columns format)](#eg3)  
[Encode a JSON array of objects to CSV with subfields](#eg4)  
[Encode nested JSON to CSV (since 1.2.0)](#eg5)  
[Encode nested JSON to CSV with column mapping (since 1.2.0)](#eg6)  

 <div id="eg1"/>

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

namespace csv = jsoncons::csv;

int main()
{
    const std::string jtext = R"(
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
        "zip_code": "55416",
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
    )";

    auto j = jsoncons::ojson::parse(jtext);

    std::string output;
    auto ioptions = csv::csv_options{}
        .quote_style(csv::quote_style_kind::nonnumeric);
    csv::encode_csv(j, output, ioptions);
    std::cout << output << "\n\n";

    auto ooptions = csv::csv_options{}
        .assume_header(true);
    auto other = csv::decode_csv<jsoncons::ojson>(output, ooptions);
    assert(other == j);
}
```
Output:
```
customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,"0272561313","01001",0.05,431.65
"Jane Doe",false,"416-272-2561","55416",0.15,480.7
"Joe Bloggs",false,"4162722561","55416",0.15,300.7
"John Smith",false,null,"22313-1450",0.15,300.7
```

 <div id="eg2"/>

#### Encode a json array of arrays (n_rows format)

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

namespace csv = jsoncons::csv;

int main()
{
    const std::string jtext = R"(
[
    ["customer_name","has_coupon","phone_number","zip_code","sales_tax_rate","total_amount"],
    ["John Roe",true,"0272561313","01001",0.05,431.65],
    ["Jane Doe",false,"416-272-2561","55416",0.15,480.7],
    ["Joe Bloggs",false,"4162722561","55416",0.15,300.7],
    ["John Smith",false,null,"22313-1450",0.15,300.7]
]
    )";


    auto j = jsoncons::json::parse(jtext);

    std::string output;
    auto ioptions = csv::csv_options{}
        .quote_style(csv::quote_style_kind::nonnumeric);
    csv::encode_csv(j, output, ioptions);
    std::cout << output << "\n\n";

    auto ooptions = csv::csv_options{}
        .assume_header(true)
        .mapping_kind(csv::csv_mapping_kind::n_rows);
    auto other = csv::decode_csv<jsoncons::json>(output, ooptions);
    assert(other == j);
}
```
Output:
```
"customer_name","has_coupon","phone_number","zip_code","sales_tax_rate","total_amount"
"John Roe",true,"0272561313","01001",0.05,431.65
"Jane Doe",false,"416-272-2561","55416",0.15,480.7
"Joe Bloggs",false,"4162722561","55416",0.15,300.7
"John Smith",false,null,"22313-1450",0.15,300.7
```

 <div id="eg3"/>

#### Encode a json object of name-array members (m_columns format)

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

namespace csv = jsoncons::csv;

int main()
{
    const std::string jtext = R"(
{
    "customer_name": ["John Roe","Jane Doe","Joe Bloggs","John Smith"],
    "has_coupon": [true,false,false,false],
    "phone_number": ["0272561313","416-272-2561","4162722561",null],
    "zip_code": ["01001","55416","55416","22313-1450"],
    "sales_tax_rate": [0.05,0.15,0.15,0.15],
    "total_amount": [431.65,480.7,300.7,300.7]
}
    )";

    auto j = jsoncons::ojson::parse(jtext);

    std::string output;
    auto ioptions = csv::csv_options{}
        .quote_style(csv::quote_style_kind::nonnumeric);
    csv::encode_csv(j, output, ioptions);
    std::cout << output << "\n\n";

    auto ooptions = csv::csv_options{}
        .assume_header(true)
        .mapping_kind(csv::csv_mapping_kind::m_columns);
    auto other = csv::decode_csv<jsoncons::ojson>(output, ooptions);
    assert(other == j);
}
```
Output:
```
customer_name,has_coupon,phone_number,zip_code,sales_tax_rate,total_amount
"John Roe",true,"0272561313","01001",0.05,431.65
"Jane Doe",false,"416-272-2561","55416",0.15,480.7
"Joe Bloggs",false,"4162722561","55416",0.15,300.7
"John Smith",false,null,"22313-1450",0.15,300.7
```

 <div id="eg4"/>

#### Encode a JSON array of objects to CSV with subfields

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

namespace csv = jsoncons::csv;

int main()
{
    std::string jtext = R"(
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
        )";

    auto j = jsoncons::ojson::parse(jtext);

    auto options = csv::csv_options{}
        .subfield_delimiter(';');

    std::string buf;
    csv::encode_csv(j, buf, options);
    std::cout << buf << "\n";
}
```
Output:
```
NY;LON,TOR,LON
NY,LON,TOR;LON
NY;LON,TOR,LON
NY,LON,TOR;LON
```

 <div id="eg5"/>

#### Encode nested JSON to CSV (since 1.2.0)

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

namespace csv = jsoncons::csv;

int main()
{
    std::string jtext = R"(
[
    {
        "text": "Chicago Reader", 
        "float": 1.0, 
        "datetime": "1971-01-01T04:14:00", 
        "boolean": true,
        "nested": {
          "time": "04:14:00",
          "nested": {
            "date": "1971-01-01",
            "integer": 40
          }
        }
    }, 
    {
        "text": "Chicago Sun-Times", 
        "float": 1.27, 
        "datetime": "1948-01-01T14:57:13", 
        "boolean": true,
        "nested": {
          "time": "14:57:13",
          "nested": {
            "date": "1948-01-01",
            "integer": 63
          }
        }
    }
]
        )";

    auto j = jsoncons::ojson::parse(jtext);

    auto options = csv::csv_options{}
        .flat(false);

    std::string buf;
    csv::encode_csv(j, buf, options);
    std::cout << buf << "\n";
}
```
Output:
```
/text,/float,/datetime,/boolean,/nested/time,/nested/nested/date,/nested/nested/integer
Chicago Reader,1.0,1971-01-01T04:14:00,true,04:14:00,1971-01-01,40
Chicago Sun-Times,1.27,1948-01-01T14:57:13,true,14:57:13,1948-01-01,63
```

 <div id="eg6"/>

#### Encode nested JSON to CSV with column mapping (since 1.2.0)

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>
#include <jsoncons_ext/csv/csv.hpp>

namespace csv = jsoncons::csv;

int main()
{
    std::string jtext = R"(
[
    {
        "text": "Chicago Reader", 
        "float": 1.0, 
        "datetime": "1971-01-01T04:14:00", 
        "boolean": true,
        "nested": {
          "time": "04:14:00",
          "nested": {
            "date": "1971-01-01",
            "integer": 40
          }
        }
    }, 
    {
        "text": "Chicago Sun-Times", 
        "float": 1.27, 
        "datetime": "1948-01-01T14:57:13", 
        "boolean": true,
        "nested": {
          "time": "14:57:13",
          "nested": {
            "date": "1948-01-01",
            "integer": 63
          }
        }
    }
]
        )";

    auto j = jsoncons::ojson::parse(jtext);

    auto options = csv::csv_options{}
        .flat(false)
        .column_mapping({
            {"/datetime","Timestamp"},
            {"/text", "Newspaper"},
            {"/nested/nested/integer","Count"}
        });

    std::string buf;
    csv::encode_csv(j, buf, options);
    std::cout << buf << "\n";
}
```
Output:
```
Timestamp,Newspaper,Count
1971-01-01T04:14:00,Chicago Reader,40
1948-01-01T14:57:13,Chicago Sun-Times,63
```


