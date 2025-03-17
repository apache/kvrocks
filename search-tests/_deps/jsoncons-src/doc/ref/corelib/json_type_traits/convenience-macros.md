### Convenience Macros

The `jsoncons` library provides a number of macros that can be used to generate the code to specialize `json_type_traits`
for a user-defined class.

Macro names follow naming conventions.

Component | Description
----------|--------------------
TPL       | Template class with a specified number of template parameters
ALL       | All data members are mandatory
N         | A specified number of data members are mandatory
MEMBER    | Accesses and modifies class data members
CTOR      | Requires constructor that takes all data members in the order they appear in the list
GETTER    | Accesses data members through getter functions
SETTER    | Modifies data members through setter functions
NAME      | Serialize with provided names (instead of C++ member names)

The `_NAME_` macros are the most general. They allow optional
parameters that affect data member mappings. Optionality is
indicated by square brackets.

The maximum number of parameters allowed in macros is 70 (since 0.168.4).
Previously it was 50.

```cpp
#include <jsoncons/json_type_traits.hpp>

JSONCONS_N_MEMBER_TRAITS(class_name,num_mandatory,
                         member0,member1,...) (1)

JSONCONS_ALL_MEMBER_TRAITS(class_name,
                           member0,member1,...) (2)

JSONCONS_TPL_N_MEMBER_TRAITS(num_template_params,
                             class_name,num_mandatory,
                             member0,member1,...) (3)  

JSONCONS_TPL_ALL_MEMBER_TRAITS(num_template_params,
                               class_name,
                               member0,member1,...) (4)

JSONCONS_N_MEMBER_NAME_TRAITS(class_name,num_mandatory,
                              (member0,serialized_name0[,mode0,match0,into0,from0]),
                              (member1,serialized_name1[,mode1,match1,into1,from1])...) (5)

JSONCONS_ALL_MEMBER_NAME_TRAITS(class_name,
                                (member0,serialized_name0[,mode0,match0,into0,from0]),
                                (member1,serialized_name1[,mode1,match1,into1,from1])...) (6)

JSONCONS_TPL_N_MEMBER_NAME_TRAITS(num_template_params,
                                  class_name,num_mandatory,
                                  (member0,serialized_name0[,mode0,match0,into0,from0]),
                                  (member1,serialized_name1[,mode1,match1,into1,from1])...) (7)

JSONCONS_TPL_ALL_MEMBER_NAME_TRAITS(num_template_params,
                                    class_name,
                                    (member0,serialized_name0[,mode0,match0,into0,from0]),
                                    (member1,serialized_name1[,mode1,match1,into1,from1])...) (8)

JSONCONS_ENUM_TRAITS(enum_name,enumerator0,enumerator1,...) (9)

JSONCONS_ENUM_NAME_TRAITS(enum_name,
                          (enumerator0,serialized_name0),
                          (enumerator1,serialized_name1)...) (10)

JSONCONS_N_CTOR_GETTER_TRAITS(class_name,num_mandatory,
                              getter0,
                              getter1,...) (11)

JSONCONS_ALL_CTOR_GETTER_TRAITS(class_name,
                                getter0,getter1,...) (12)

JSONCONS_TPL_N_CTOR_GETTER_TRAITS(num_template_params,
                                  class_name,num_mandatory,
                                  getter0,getter1,...) (13)

JSONCONS_TPL_ALL_CTOR_GETTER_TRAITS(num_template_params,
                                    class_name,
                                    getter0,getter1,...) (14)

JSONCONS_N_CTOR_GETTER_NAME_TRAITS(class_name,num_mandatory,
                                   (getter0,serialized_name0[,mode0,match0,into0,from0]),
                                   (getter1,serialized_name1[,mode1,match1,into1,from1])...) (15)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(class_name,
                                     (getter0,serialized_name0[,mode0,match0,into0,from0]),
                                     (getter1,serialized_name1[,mode1,match1,into1,from1])...) (16)

JSONCONS_TPL_N_CTOR_GETTER_NAME_TRAITS(num_template_params,
                                       class_name,num_mandatory,
                                       (getter0,serialized_name0[,mode0,match0,into0,from0]),
                                       (getter1,serialized_name1[,mode1,match1,into1,from1])...) (17)

JSONCONS_TPL_ALL_CTOR_GETTER_NAME_TRAITS(num_template_params,
                                         class_name,
                                         (getter0,serialized_name0[,mode0,match0,into0,from0]),
                                         (getter1,serialized_name1[,mode1,match1,into1,from1])...) (18)

JSONCONS_N_GETTER_SETTER_TRAITS(class_name,get_prefix,set_prefix,num_mandatory,
                                property0,property1,...) (19)

JSONCONS_ALL_GETTER_SETTER_TRAITS(class_name,get_prefix,set_prefix,
                                  property0,property1,...) (20)

JSONCONS_TPL_N_GETTER_SETTER_TRAITS(num_template_params,
                                    class_name,get_prefix,set_prefix,num_mandatory,
                                    property0,property1,...) (21)  

JSONCONS_TPL_ALL_GETTER_SETTER_TRAITS(num_template_params,
                                      class_name,get_prefix,set_prefix,
                                      property0,property1,...) (22)

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(class_name,num_mandatory,
                                     (getter0,setter0,serialized_name0[,mode0,match0,into0,from0]),
                                     (getter1,setter1,serialized_name1[,mode1,match1,into1,from1])...) (23)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(class_name,
                                       (getter0,setter0,serialized_name0[,mode0,match0,into0,from0]),
                                       (getter1,setter1,serialized_name1[,mode1,match1,into1,from1])...) (24)

JSONCONS_TPL_N_GETTER_SETTER_NAME_TRAITS(num_template_params,
                                         class_name,num_mandatory,
                                         (getter0,setter0,serialized_name0[,mode0,match0,into0,from0]),
                                         (getter1,setter1,serialized_name1[,mode1,match1,into1,from1])...) (25)

JSONCONS_TPL_ALL_GETTER_SETTER_NAME_TRAITS(num_template_params,
                                           class_name,
                                           (getter0,setter0,serialized_name0[,mode0,match0,into0,from0]),
                                           (getter1,setter1,serialized_name1[,mode1,match1,into1,from1])...) (26)

JSONCONS_POLYMORPHIC_TRAITS(base_class_name,derived_class_name0,derived_class_name1,...) (27)
```

(1)-(4) generate the code to specialize `json_type_traits` for a class from member data. 
The serialized names are the stringified member names. 
When decoding to a C++ data structure, 
(1) and (3) require that the first `num_mandatory` member names be present in the JSON,
the rest can have default values. (2) and (4)
require that all member names be present in the JSON. The class must have a default constructor.
If the member data or default constructor are private, the macro `JSONCONS_TYPE_TRAITS_FRIEND`
will make them accessible to `json_type_traits`, used so
 
```cpp
class MyClass
{
    JSONCONS_TYPE_TRAITS_FRIEND
...
};
```

(3)-(4) generate the code to specialize `json_type_traits` for a class template from member data. 

(5)-(8) generate the code to specialize `json_type_traits` for a class from member data.
The serialized names are the provided names. The sequence of `(memberN,serialized_nameN)`
pairs declares the member name and provided name for each of the class members
that are part of the sequence.
When decoding to a C++ data structure, 
(5) and (7) require that the first `num_mandatory` member names be present in the JSON,
the rest can have default values. (6) and (8) 
require that all member names be present in the JSON. The class must have a default constructor.
If the member data or default constructor are private, the macro `JSONCONS_TYPE_TRAITS_FRIEND`
will make them accessible to `json_type_traits`.
(7)-(8) generate the code to specialize `json_type_traits` for a class template from member data. 

(9) generates the code to specialize `json_type_traits` for an enumerated type from its enumerators.
The serialized name is the stringified enumerator name. 

(10) generates the code to specialize `json_type_traits` for an enumerated type from its enumerators.
The serialized name is the provided name. The sequence of `(enumeratorN,serialized_nameN)`
pairs declares the named constant and provided name for each of the enumerators
that are part of the sequence.

(11)-(14) generate the code to specialize `json_type_traits` for a class from a constructor and get functions. 
The serialized names are the stringified field names. 
When decoding to a C++ data structure, 
(11) and (13) require that the first `num_mandatory` member names be present in the JSON,
the rest can have default values. (12) and (14) 
require that all member names be present in the JSON. The class must have a constructor such that the return types 
of the get functions are convertible to its parameters, taken in order. 
(13)-(14) generate the code to specialize `json_type_traits` for a class template from a constructor and get functions.  

(15)-(18) generate the code to specialize `json_type_traits` for a class from a constructor and get functions.
The serialized names are the provided names. The sequence of `(getterN,serialized_nameN)`
pairs declares the get function and provided name for each of the class members
that are part of the sequence. 
When decoding to a C++ data structure, 
(15) and (17) require that the first `num_mandatory` member names be present in the JSON,
the rest can have default values. (16) and (18) 
require that all member names be present in the JSON. The class must have a constructor such that the return types 
of the get functions are convertible to its parameters, taken in order. 
(17)-(18) generate the code to specialize `json_type_traits` for a class template from a constructor and get functions.  

(19)-(22) generate the code to specialize `json_type_traits` for a class from get and set functions.
The serialized names are the stringified field names. The get and set function names are
formed from the concatenation of `get_prefix` and `set_prefix` with field name.
(19) and (21) require that the first `num_mandatory` member names be present in the JSON,
the rest can have default values. (20) and (22) 
require that all member names be present in the JSON. (21)-(22) generate the code to specialize `json_type_traits` 
for a class template from get and set functions.

(23)-(26) generate the code to specialize `json_type_traits` for a class from get and set functions.
The serialized names are the provided names. The sequence of `(getterN,setterN,serialized_nameN)`
triples declares the get and set functions and provided name for each of the class members
that are part of the sequence. When decoding to a C++ data structure, 
(23) and (25) require that the first `num_mandatory` member names be present in the JSON,
the rest can have default values. (24) and (26) 
require that all member names be present in the JSON. The class must have a default constructor. 
(25)-(26) generate the code to specialize `json_type_traits` for a class template from get and set functions.

(27) generates the code to specialize `json_type_traits` for `std::shared_ptr<base_class>` and `std::unique_ptr<base_class>`.
Each derived class must have a `json_type_traits<Json,derived_class_name>` specialization.
The type selection strategy is based on `json_type_traits<Json,derived_class_name>::is(const Json& j)`.
In the case that `json_type_traits<Json,derived_class_name>` has been generated by one of the
conveniences macros (1)-(26), the type selection strategy is based on the presence of members
in the derived classes.

#### Parameters

<table border="0">
  <tr>
    <td><code>class_name</code></td>
    <td>The name of a class or struct.</td> 
  </tr>
  <tr>
    <td><code>enum_name</code></td>
    <td>The name of an enum type or enum class type.</td> 
  </tr>
  <tr>
    <td><code>num_mandatory</code></td>
    <td>The number of mandatory class data members or accessors.</td> 
  </tr>
  <tr>
    <td><code>num_template_params</code></td>
    <td>For a class template, the number of template parameters.</td> 
  </tr>
  <tr>
    <td><code>memberN</code></td>
    <td>The name of a class data member. Class data members are normally modifiable, but may be <code>const</code> or <code>static const</code>.
        Data members that are <code>const</code> or <code>static const</code> are one-way serialized.</td> 
  </tr>
  <tr>
    <td><code>propertyN</code></td>
    <td>The base name of a class getter or setter, with any get or set prefix stripped out.</td> 
  </tr>
  <tr>
    <td><code>getterN</code></td>
    <td>The getter for a class data member.</td> 
  </tr>
  <tr>
    <td><code>setterN</code></td>
    <td>The setter for a class data member.</td> 
  </tr>
  <tr>
    <td><code>enumeratorN</code></td>
    <td>An enumerator.</td> 
  </tr>
  <tr>
    <td><code>serialized_nameN</code></td>
    <td>Serialized name.</td> 
  </tr>
  <tr>
    <td><code>modeN</code></td>
    <td>Indicates whether a data member is read-write (<code>JSONCONS_RDWR</code>) or read-only (<code>JSONCONS_RDONLY</code>).
Read-only data members are serialized but not de-serialized. (since 0.157.0)</td> 
  </tr>
  <tr>
    <td><code>matchN</code></td>
    <td>A function object that checks if a type that has <code>json_type_traits</code> specialization
can be converted into a user value. 
It must have function call signature equivalent to
<br/><br/><code>
bool fun(const Type& a);
</code><br/><br/>
where <code>Type</code> matches the return type of function object <code>intoN</code>, if provided,
and if not, the type of <code>memberN</code> (<code>_MEMBER_</code> traits) 
or the return type of an accessor (<code>_GETTER_ traits</code>).
It returns <code>true</code> if the argument provided matches an allowed value,
<code>false</code> otherwise. (since 0.157.0)</td> 
  </tr>
  <tr>
    <td><code>intoN</code></td>
    <td>A function object that converts a user value into a type that has <code>json_type_traits</code> specialization. 
It must have function call signature equivalent to
<br/><br/><code>
Ret fun(const Type& a);
</code><br/><br/>
where <code>Type</code> matches the type of <code>memberN</code> (<code>_MEMBER_</code> traits) or the return type of an accessor (<code>_GETTER_ traits</code>), and <code>Ret</code> is the parameter type of function object <code>fromN</code> (if provided)
or <code>Type</code> (if not).
It can be a free function, a struct object with <code>operator()</code> defined, or a variable containing a lambda expression,
but because it is used in an unevaluated context, it cannot be a lambda expression (at least until C++20).
(since 0.157.0)</td> 
  </tr>
  <tr>
    <td><code>fromN</code></td>
    <td>A function object that gets a user value from a type that has <code>json_type_traits</code> specialization. 
It must have function call signature equivalent to
<br/><br/><code>
Ret fun(const Type& a);
</code><br/><br/>
where <code>Type</code> is the return type of the function object <code>intoN</code>, 
and <code>Ret</code> is the  is the type of <code>memberN</code> (<code>_MEMBER_</code> traits) or the return type of an accessor (<code>_GETTER_ traits</code>).
Only used if <code>modeN</code> is <code>JSONCONS_RDWR</code>. (since 0.157.0)</td> 
  </tr>
  <tr>
    <td><code>base_class_name</code></td>
    <td>The name of a base class.</td> 
  </tr>
  <tr>
    <td><code>derived_class_nameN</code></td>
    <td>A class that is derived from the base class, and that has a <code>json_type_traits<Json,derived_class_nameN></code> specialization.</td> 
  </tr>
</table>

These macro declarations must be placed at global scope, outside any namespace blocks, and `class_name`, 
`base_class_name` and `derived_class_nameN` must be a fully namespace qualified names.

All of the `json_type_traits` specializations for type `T` generated by the convenience macros include a specialization of
`is_json_type_traits_declared<T>` with member constant `value` equal `true`.

### Examples

[Specialize json_type_traits to support a book class](#A1)  
[Using JSONCONS_ALL_CTOR_GETTER_TRAITS to generate the json_type_traits](#A2)  
[Example with std::shared_ptr, std::unique_ptr and std::optional](#A3)  
[Serialize a polymorphic type based on the presence of members](#A4)  
[Ensuring type selection is possible](#A5)  
[Decode to a polymorphic type based on a type marker (since 0.157.0)](#A6)  
[An example with std::variant](#A7)  
[Type selection and std::variant](#A8)  
[Decode to a std::variant based on a type marker (since 0.158.0)](#A9)  
[Transform data member (since 0.157.0)](#A10)  
[Tidy data member (since 0.158.0)](#A11)

<div id="A1"/> 

#### Specialize json_type_traits to support a book class.

```cpp
#include <iostream>
#include <jsoncons/json.hpp>
#include <vector>
#include <string>

namespace ns {
    struct book
    {
        std::string author;
        std::string title;
        double price;
    };
} // namespace ns

JSONCONS_ALL_MEMBER_TRAITS(ns::book, author, title, price)

using namespace jsoncons; // for convenience

int main()
{
    const std::string s = R"(
    [
        {
            "author" : "Haruki Murakami",
            "title" : "Kafka on the Shore",
            "price" : 25.17
        },
        {
            "author" : "Charles Bukowski",
            "title" : "Pulp",
            "price" : 22.48
        }
    ]
    )";

    std::vector<ns::book> book_list = decode_json<std::vector<ns::book>>(s);

    std::cout << "(1)\n";
    for (const auto& item : book_list)
    {
        std::cout << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }

    std::cout << "\n(2)\n";
    encode_json(book_list, std::cout, indenting::indent);
    std::cout << "\n\n";
}
```
Output:
```
(1)
Haruki Murakami, Kafka on the Shore, 25.17
Charles Bukowski, Pulp, 22.48

(2)
[
    {
        "author": "Haruki Murakami",
        "price": 25.17,
        "title": "Kafka on the Shore"
    },
    {
        "author": "Charles Bukowski",
        "price": 22.48,
        "title": "Pulp"
    }
]
```

<div id="A2"/> 

#### Using JSONCONS_ALL_CTOR_GETTER_TRAITS to generate the json_type_traits 

The macro `JSONCONS_ALL_CTOR_GETTER_TRAITS` will generate the `json_type_traits` boilerplate
for your own types from a constructor and getter functions.

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>

namespace ns {
    enum class hiking_experience {beginner,intermediate,advanced};

    class hiking_reputon
    {
        std::string rater_;
        hiking_experience assertion_;
        std::string rated_;
        double rating_;
        std::optional<std::chrono::seconds> generated_; // use std::optional if C++17
        std::optional<std::chrono::seconds> expires_;
    public:
        hiking_reputon(const std::string& rater,
                       hiking_experience assertion,
                       const std::string& rated,
                       double rating,
                       const std::optional<std::chrono::seconds>& generated = std::optional<std::chrono::seconds>(),
                       const std::optional<std::chrono::seconds>& expires = std::optional<std::chrono::seconds>())
            : rater_(rater), assertion_(assertion), rated_(rated), rating_(rating),
              generated_(generated), expires_(expires)
        {
        }

        const std::string& rater() const {return rater_;}
        hiking_experience assertion() const {return assertion_;}
        const std::string& rated() const {return rated_;}
        double rating() const {return rating_;}
        std::optional<std::chrono::seconds> generated() const {return generated_;}
        std::optional<std::chrono::seconds> expires() const {return expires_;}

        friend bool operator==(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return lhs.rater_ == rhs.rater_ && lhs.assertion_ == rhs.assertion_ && 
                   lhs.rated_ == rhs.rated_ && lhs.rating_ == rhs.rating_ &&
                   lhs.confidence_ == rhs.confidence_ && lhs.expires_ == rhs.expires_;
        }

        friend bool operator!=(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return !(lhs == rhs);
        };
    };

    class hiking_reputation
    {
        std::string application_;
        std::vector<hiking_reputon> reputons_;
    public:
        hiking_reputation(const std::string& application, 
                          const std::vector<hiking_reputon>& reputons)
            : application_(application), 
              reputons_(reputons)
        {}

        const std::string& application() const { return application_;}
        const std::vector<hiking_reputon>& reputons() const { return reputons_;}
    };

} // namespace ns

// Declare the traits. Specify which data members need to be serialized.

JSONCONS_ENUM_TRAITS(ns::hiking_experience, beginner, intermediate, advanced)
// First four members listed are mandatory, generated and expires are optional
JSONCONS_N_CTOR_GETTER_TRAITS(ns::hiking_reputon, 4, rater, assertion, rated, rating, 
                              generated, expires)

// All members are mandatory
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::hiking_reputation, application, reputons)

int main()
{
    // Decode the string of data into a c++ structure
    ns::hiking_reputation v = decode_json<ns::hiking_reputation>(data);

    // Iterate over reputons array value
    std::cout << "(1)\n";
    for (const auto& item : v.reputons())
    {
        std::cout << item.rated() << ", " << item.rating();
        if (item.generated())
        {
            std::cout << ", " << (*item.generated()).count();
        }
        std::cout << "\n";
    }

    // Encode the c++ structure into a string
    std::string s;
    encode_json(v, s, indenting::indent);
    std::cout << "(2)\n";
    std::cout << s << "\n";
}
```
Output:
```
(1)
Marilyn C, 0.9, 1514862245
(2)
{
    "application": "hiking",
    "reputons": [
        {
            "assertion": "advanced",
            "generated": 1514862245,
            "rated": "Marilyn C",
            "rater": "HikingAsylum",
            "rating": 0.9
        }
    ]
}
```

<div id="A3"/> 

#### Example with std::shared_ptr, std::unique_ptr and std::optional

This example assumes C++17 language support for `std::optional`.
Lacking that, you can use `jsoncons::optional`.

```cpp
#include <cassert>
#include <iostream>
#include <jsoncons/json.hpp>

namespace ns {
    struct smart_pointer_and_optional_test
    {
        std::shared_ptr<std::string> field1;
        std::unique_ptr<std::string> field2;
        std::optional<std::string> field3;
        std::shared_ptr<std::string> field4;
        std::unique_ptr<std::string> field5;
        std::optional<std::string> field6;
        std::shared_ptr<std::string> field7;
        std::unique_ptr<std::string> field8;
        std::optional<std::string> field9;
        std::shared_ptr<std::string> field10;
        std::unique_ptr<std::string> field11;
        std::optional<std::string> field12;
    };

} // namespace ns

// Declare the traits, first 6 members mandatory, last 6 non-mandatory
JSONCONS_N_MEMBER_TRAITS(ns::smart_pointer_and_optional_test,6,
                         field1,field2,field3,field4,field5,field6,
                         field7,field8,field9,field10,field11,field12)

using namespace jsoncons; // for convenience

int main()
{
    ns::smart_pointer_and_optional_test val;
    val.field1 = std::make_shared<std::string>("Field 1"); 
    val.field2 = jsoncons::make_unique<std::string>("Field 2"); 
    val.field3 = "Field 3";
    val.field4 = std::shared_ptr<std::string>(nullptr);
    val.field5 = std::unique_ptr<std::string>(nullptr);
    val.field6 = std::optional<std::string>();
    val.field7 = std::make_shared<std::string>("Field 7"); 
    val.field8 = jsoncons::make_unique<std::string>("Field 8"); 
    val.field9 = "Field 9";
    val.field10 = std::shared_ptr<std::string>(nullptr);
    val.field11 = std::unique_ptr<std::string>(nullptr);
    val.field12 = std::optional<std::string>();

    std::string buf;
    encode_json(val, buf, indenting::indent);
    std::cout << buf << "\n";

    auto other = decode_json<ns::smart_pointer_and_optional_test>(buf);

    assert(*other.field1 == *val.field1);
    assert(*other.field2 == *val.field2);
    assert(*other.field3 == *val.field3);
    assert(!other.field4);
    assert(!other.field5);
    assert(!other.field6);
    assert(*other.field7 == *val.field7);
    assert(*other.field8 == *val.field8);
    assert(*other.field9 == *val.field9);
    assert(!other.field10);
    assert(!other.field11);
    assert(!other.field12);
}
```
Output:
```
{
    "field1": "Field 1",
    "field2": "Field 2",
    "field3": "Field 3",
    "field4": null,
    "field5": null,
    "field6": null,
    "field7": "Field 7",
    "field8": "Field 8",
    "field9": "Field 9"
}
```

<div id="A4"/> 

#### Serialize a polymorphic type based on the presence of members

This example uses the convenience macro `JSONCONS_N_CTOR_GETTER_TRAITS`
to generate the `json_type_traits` boilerplate for the `HourlyEmployee` and `CommissionedEmployee` 
derived classes, and `JSONCONS_POLYMORPHIC_TRAITS` to generate the `json_type_traits` boilerplate
for `std::shared_ptr<Employee>` and `std::unique_ptr<Employee>`. The type selection strategy is based
on the presence of mandatory members, in particular, to the `firstName`, `lastName`, and `wage` members of an
`HourlyEmployee`, and to the `firstName`, `lastName`, `baseSalary`, and `commission` members of a `CommissionedEmployee`.
Non-mandatory members are not considered for the purpose of type selection.

```cpp
#include <cassert>
#include <iostream>
#include <vector>
#include <jsoncons/json.hpp>

using namespace jsoncons;

namespace ns {

class Employee
{
    std::string firstName_;
    std::string lastName_;
public:
    Employee(const std::string& firstName, const std::string& lastName)
        : firstName_(firstName), lastName_(lastName)
    {
    }
    virtual ~Employee() noexcept = default;

    virtual double calculatePay() const = 0;

    const std::string& firstName() const {return firstName_;}
    const std::string& lastName() const {return lastName_;}
};

class HourlyEmployee : public Employee
{
    double wage_;
    unsigned hours_;
public:
    HourlyEmployee(const std::string& firstName, const std::string& lastName, 
                   double wage, unsigned hours)
        : Employee(firstName, lastName), 
          wage_(wage), hours_(hours)
    {
    }

    double wage() const {return wage_;}

    unsigned hours() const {return hours_;}

    double calculatePay() const override
    {
        return wage_*hours_;
    }
};

class CommissionedEmployee : public Employee
{
    double baseSalary_;
    double commission_;
    unsigned sales_;
public:
    CommissionedEmployee(const std::string& firstName, const std::string& lastName, 
                         double baseSalary, double commission, unsigned sales)
        : Employee(firstName, lastName), 
          baseSalary_(baseSalary), commission_(commission), sales_(sales)
    {
    }

    double baseSalary() const
    {
        return baseSalary_;
    }

    double commission() const
    {
        return commission_;
    }

    unsigned sales() const
    {
        return sales_;
    }

    double calculatePay() const override
    {
        return baseSalary_ + commission_*sales_;
    }
};

} // ns

JSONCONS_N_CTOR_GETTER_TRAITS(ns::HourlyEmployee, 3, firstName, lastName, wage, hours)
JSONCONS_N_CTOR_GETTER_TRAITS(ns::CommissionedEmployee, 4, firstName, lastName, baseSalary, commission, sales)
JSONCONS_POLYMORPHIC_TRAITS(ns::Employee, ns::HourlyEmployee, ns::CommissionedEmployee)

int main()
{
    std::string input = R"(
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000
    }
]
    )"; 

    auto v = decode_json<std::vector<std::unique_ptr<ns::Employee>>>(input);

    std::cout << "(1)\n";
    for (const auto& p : v)
    {
        std::cout << p->firstName() << " " << p->lastName() << ", " << p->calculatePay() << "\n";
    }

    std::cout << "\n(2)\n";
    encode_json(v, std::cout, indenting::indent);

    std::cout << "\n\n(3)\n";
    json j(v);
    std::cout << pretty_print(j) << "\n\n";
}
```
Output:
```
(1)
John Smith, 40000
Jane Doe, 30250

(2)
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000
    }
]

(3)
[
    {
        "firstName": "John",
        "hours": 1000,
        "lastName": "Smith",
        "wage": 40.0
    },
    {
        "baseSalary": 30000.0,
        "commission": 0.25,
        "firstName": "Jane",
        "lastName": "Doe",
        "sales": 1000
    }
]
```

<div id="A5"/>

#### Ensuring type selection is possible

When deserializing a polymorphic type, jsoncons needs to know how
to convert a json value to the proper derived class. In the Employee
example above, the type selection strategy is based
on the presence of members in the derived classes. If
derived classes cannot be distinguished in this way, 
you can introduce extra members. The convenience
macros `JSONCONS_N_MEMBER_TRAITS`, `JSONCONS_ALL_MEMBER_TRAITS`,
`JSONCONS_TPL_N_MEMBER_TRAITS`, `JSONCONS_TPL_ALL_MEMBER_TRAITS`,
`JSONCONS_N_MEMBER_NAME_TRAITS`, `JSONCONS_ALL_MEMBER_NAME_TRAITS`,
`JSONCONS_TPL_N_MEMBER_NAME_TRAITS`, and `JSONCONS_TPL_ALL_MEMBER_NAME_TRAITS`
allow you to have `const` or `static const` data members that are serialized and that 
particpate in the type selection strategy during deserialization. 

```cpp
namespace ns {

class Foo
{
public:
    virtual ~Foo() noexcept = default;
};

class Bar : public Foo
{
    static const bool bar = true;
    JSONCONS_TYPE_TRAITS_FRIEND
};

class Baz : public Foo 
{
    static const bool baz = true;
    JSONCONS_TYPE_TRAITS_FRIEND
};

} // ns

JSONCONS_N_MEMBER_TRAITS(ns::Bar,1,bar)
JSONCONS_N_MEMBER_TRAITS(ns::Baz,1,baz)
JSONCONS_POLYMORPHIC_TRAITS(ns::Foo, ns::Bar, ns::Baz)

int main()
{
    std::vector<std::unique_ptr<ns::Foo>> u;
    u.emplace_back(new ns::Bar());
    u.emplace_back(new ns::Baz());

    std::string buffer;
    encode_json(u, buffer);
    std::cout << "(1)\n" << buffer << "\n\n";

    auto v = decode_json<std::vector<std::unique_ptr<ns::Foo>>>(buffer);

    std::cout << "(2)\n";
    for (const auto& ptr : v)
    {
        if (dynamic_cast<ns::Bar*>(ptr.get()))
        {
            std::cout << "A bar\n";
        }
        else if (dynamic_cast<ns::Baz*>(ptr.get()))
        {
            std::cout << "A baz\n";
        } 
    }
}
```

Output:
```
(1)
[{"bar":true},{"baz":true}]

(2)
A bar
A baz
```

<div id="A6"/>

#### Decode to a polymorphic type based on a type marker (since 0.157.0)

```cpp
namespace ns {

    class Shape
    {
    public:
        virtual ~Shape() = default;
        virtual double area() const = 0;
    };
      
    class Rectangle : public Shape
    {
        double height_;
        double width_;
    public:
        Rectangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "rectangle"; 
            return type_;
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle : public Shape
    { 
        double height_;
        double width_;

    public:
        Triangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "triangle"; 
            return type_;
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }
    };                 

    class Circle : public Shape
    { 
        double radius_;

    public:
        Circle(double radius)
            : radius_(radius)
        {
        }

        const std::string& type() const
        {
            static const std::string type_ = "circle"; 
            return type_;
        }

        double radius() const
        {
            return radius_;
        }

        double area() const override
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }
    };                 

} // ns

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle,
    (type,"type",JSONCONS_RDONLY,[](const std::string& type) noexcept{return type == "rectangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (radius, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape,ns::Rectangle,ns::Triangle,ns::Circle)

int main()
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 4.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    auto shapes = jsoncons::decode_json<std::vector<std::unique_ptr<ns::Shape>>>(input);

    std::cout << "(1)\n";
    for (const auto& shape : shapes)
    {
        std::cout << typeid(*shape.get()).name() << " area: " << shape->area() << "\n";
    }

    std::string output;

    jsoncons::encode_json(shapes, output, indenting::indent);
    std::cout << "\n(2)\n" << output << "\n";
}
```

Output:
```
(1)
class `ns::Rectangle area: 3.0000000
class `ns::Triangle area: 4.0000000
class `ns::Circle area: 3.1415927

(2)
[
    {
        "height": 1.5,
        "type": "rectangle",
        "width": 2.0
    },
    {
        "height": 2.0,
        "type": "triangle",
        "width": 4.0
    },
    {
        "radius": 1.0,
        "type": "circle"
    }
]
```

This example maps a `type()` getter to a "type" data member in the JSON.
However, we can also achieve this without using a `type()` getter at all. 
Compare with the very similar example [decode to a std::variant based on a type marker](#A9)

<div id="A7"/>

#### An example with std::variant

This example assumes C++17 language support and jsoncons v0.154.0 or later.

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    enum class Color {yellow, red, green, blue};

    inline
    std::ostream& operator<<(std::ostream& os, Color val)
    {
        switch (val)
        {
            case Color::yellow: os << "yellow"; break;
            case Color::red: os << "red"; break;
            case Color::green: os << "green"; break;
            case Color::blue: os << "blue"; break;
        }
        return os;
    }

    class Fruit 
    {
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string name_;
        Color color_;
    public:
        friend std::ostream& operator<<(std::ostream& os, const Fruit& val)
        {
            os << "name: " << val.name_ << ", color: " << val.color_ << "\n";
            return os;
        }
    };

    class Fabric 
    {
    private:
      JSONCONS_TYPE_TRAITS_FRIEND
      int size_;
      std::string material_;
    public:
        friend std::ostream& operator<<(std::ostream& os, const Fabric& val)
        {
            os << "size: " << val.size_ << ", material: " << val.material_ << "\n";
            return os;
        }
    };

    class Basket {
     private:
      JSONCONS_TYPE_TRAITS_FRIEND
      std::string owner_;
      std::vector<std::variant<Fruit, Fabric>> items_;

    public:
        std::string owner() const
        {
            return owner_;
        }

        std::vector<std::variant<Fruit, Fabric>> items() const
        {
            return items_;
        }
    };

} // ns
} // namespace

JSONCONS_ENUM_NAME_TRAITS(ns::Color, (yellow, "YELLOW"), (red, "RED"), (green, "GREEN"), (blue, "BLUE"))

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fruit,
                                (name_, "name"),
                                (color_, "color"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Fabric,
                                (size_, "size"),
                                (material_, "material"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Basket,
                                (owner_, "owner"),
                                (items_, "items"))

int main()
{
    std::string input = R"(
{
  "owner": "Rodrigo",
  "items": [
    {
      "name": "banana",
      "color": "YELLOW"
    },
    {
      "size": 40,
      "material": "wool"
    },
    {
      "name": "apple",
      "color": "RED"
    },
    {
      "size": 40,
      "material": "cotton"
    }
  ]
}
    )";

    ns::Basket basket = jsoncons::decode_json<ns::Basket>(input);
    std::cout << basket.owner() << "\n\n";

    std::cout << "(1)\n";
    for (const auto& var : basket.items()) 
    {
        std::visit([](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, ns::Fruit>)
                std::cout << "Fruit " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Fabric>)
                std::cout << "Fabric " << arg << '\n';
        }, var);
    }

    std::string output;
    jsoncons::encode_json(basket, output, indenting::indent);
    std::cout << "(2)\n" << output << "\n\n";
}
```
Output:
```
Rodrigo

(1)
Fruit name: banana, color: yellow

Fabric size: 28, material: wool

Fruit name: apple, color: red

Fabric size: 28, material: cotton

(2)
{
    "items": [
        {
            "color": "YELLOW",
            "name": "banana"
        },
        {
            "material": "wool",
            "size": 40
        },
        {
            "color": "RED",
            "name": "apple"
        },
        {
            "material": "cotton",
            "size": 40
        }
    ],
    "owner": "Rodrigo"
}
```

<div id="A8"/>

#### Type selection and std::variant

For classes supported through the convenience macros, e.g. `Fruit` and `Fabric` from the previous example, 
the type selection strategy is the same as for polymorphic types, and is based 
on the presence of mandatory members in the classes. More generally, 
the type selection strategy is based on the `json_type_traits<Json,T>::is(const Json& j)` 
function, checking each type in the variant from left to right, and stopping when 
`json_type_traits<Json,T>::is(j)` returns `true`. 

Now consider 

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    enum class Color {yellow, red, green, blue};

} // ns

JSONCONS_ENUM_NAME_TRAITS(ns::Color, (yellow, "YELLOW"), (red, "RED"), (green, "GREEN"), (blue, "BLUE"))

int main()
{
    using variant_type  = std::variant<int, double, bool, std::string, ns::Color>;

    std::vector<variant_type> vars = {100, 10.1, false, std::string("Hello World"), ns::Color::yellow};

    std::string buffer;
    jsoncons::encode_json(vars, buffer, indenting::indent);

    std::cout << "(1)\n" << buffer << "\n\n";

    auto vars2 = jsoncons::decode_json<std::vector<variant_type>>(buffer);

    auto visitor = [](auto&& arg) {
            using T = std::decay_t<decltype(arg)>;
            if constexpr (std::is_same_v<T, int>)
                std::cout << "int " << arg << '\n';
            else if constexpr (std::is_same_v<T, double>)
                std::cout << "double " << arg << '\n';
            else if constexpr (std::is_same_v<T, bool>)
                std::cout << "bool " << arg << '\n';
            else if constexpr (std::is_same_v<T, std::string>)
                std::cout << "std::string " << arg << '\n';
            else if constexpr (std::is_same_v<T, ns::Color>)
                std::cout << "ns::Color " << arg << '\n';
        };

    std::cout << "(2)\n";
    for (const auto& item : vars2)
    {
        std::visit(visitor, item);
    }
    std::cout << "\n";
}
```
Output:
```
(1)
[
    100,
    10.1,
    false,
    "Hello World",
    "YELLOW"
]

(2)
int 100
double 10.1
bool false
std::string Hello World
std::string YELLOW
```

Encode is fine. But when decoding, jsoncons checks if the JSON string "YELLOW" is a `std::string` 
before it checks whether it is an `ns::Color`, and since the answer is yes, 
it is stored in the variant as a `std::string`.

But if we switch the order of `ns::Color` and `std::string` in the variant definition, viz.

```cpp
 using variant_type  = std::variant<int, double, bool, ns::Color, std::string>;
```
strings containing  the text "YELLOW", "RED", "GREEN", or "BLUE" are detected to be `ns::Color`, and the others `std::string`.  

And the output becomes
```
(1)
[
    100,
    10.1,
    false,
    "Hello World",
    "YELLOW"
]

(2)
int 100
double 10.1
bool false
std::string Hello World
ns::Color yellow
```

So: types that are more constrained should appear to the left of types that are less constrained.

<div id="A9"/>

#### Decode to a std::variant based on a type marker (since 0.158.0)

This example is very similar to [decode to a polymorphic type based on a type marker](#A6),
and in fact the json traits defined for that example would do for `std::variant` as well.
But here we add a wrinkle by omitting the `type()` function in the `Rectangle`, `Triangle` and
`Circle` classes. More generally, we show how to augment the JSON output with name/value pairs 
that are not present in the class definitions, and to perform type selection with them.

```cpp
#include <jsoncons/json.hpp>

namespace ns {

    class Rectangle
    {
        double height_;
        double width_;
    public:
        Rectangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const
        {
            return height_ * width_;
        }
    };

    class Triangle
    { 
        double height_;
        double width_;

    public:
        Triangle(double height, double width)
            : height_(height), width_(width)
        {
        }

        double height() const
        {
            return height_;
        }

        double width() const
        {
            return width_;
        }

        double area() const
        {
            return (height_ * width_)/2.0;
        }
    };                 

    class Circle
    { 
        double radius_;

    public:
        Circle(double radius)
            : radius_(radius)
        {
        }

        double radius() const
        {
            return radius_;
        }

        double area() const
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }
    };                 

    inline constexpr auto rectangle_marker = [](double) noexcept {return "rectangle"; };
    inline constexpr auto triangle_marker = [](double) noexcept {return "triangle";};
    inline constexpr auto circle_marker = [](double) noexcept {return "circle";};

} // namespace ns

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle,
    (height,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle,
    (height,"type", JSONCONS_RDONLY, 
     [](const std::string& type) noexcept {return type == "triangle";},
     ns::triangle_marker),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle,
    (radius,"type", JSONCONS_RDONLY, 
     [](const std::string& type) noexcept {return type == "circle";},
     ns::circle_marker),
    (radius, "radius")
)

int main()
{
    using shapes_t = std::variant<ns::Rectangle,ns::Triangle,ns::Circle>;

    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 4.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    auto shapes = jsoncons::decode_json<std::vector<shapes_t>>(input);

    auto visitor = [](auto&& shape) {
        using T = std::decay_t<decltype(shape)>;
        if constexpr (std::is_same_v<T, ns::Rectangle>)
            std::cout << "rectangle area: " << shape.area() << '\n';
        else if constexpr (std::is_same_v<T, ns::Triangle>)
            std::cout << "triangle area: " << shape.area() << '\n';
        else if constexpr (std::is_same_v<T, ns::Circle>)
            std::cout << "circle area: " << shape.area() << '\n';
    };

    std::cout << "(1)\n";
    for (const auto& shape : shapes)
    {
        std::visit(visitor, shape);
    }

    std::string output;
    jsoncons::encode_json(shapes, output, indenting::indent);
    std::cout << "\n(2)\n" << output << "\n";
}
```
Output:
```
(1)
rectangle area: 3.0000000
triangle area: 4.0000000
circle area: 3.1415927

(2)
[
    {
        "height": 1.5,
        "type": "rectangle",
        "width": 2.0
    },
    {
        "height": 2.0,
        "type": "triangle",
        "width": 4.0
    },
    {
        "radius": 1.0,
        "type": "circle"
    }
]
```

Note the mapping to the "type" member, in particular, for the rectangle,

```cpp
(height,"type",JSONCONS_RDONLY,
 [](const std::string& type) noexcept{return type == "rectangle";},
 ns::rectangle_marker),
```

There are two things to observe. First, the class member being mapped,
here `height`, can be any member, we don't actually use it. Instead,  
we use the function object `ns::rectangle_marker` to ouput the value
"rectangle" with the key "type". Second, the function argument in
this position cannot be a lambda expression (at least until C++20), 
because jsoncons uses it in an unevaluated context, so it is
provided as a variable containing a lambda expression instead.

<div id="A10"/>

#### Transform data member (since 0.158.0)

```cpp
#include <cassert>
#include <jsoncons/json.hpp>

namespace ns {

    class Employee
    {
        std::string name_;
        std::string surname_;
    public:
        Employee() = default;

        Employee(const std::string& name, const std::string& surname)
            : name_(name), surname_(surname)
        {
        }

        std::string getName() const
        {
            return name_;
        }
        void setName(const std::string& name)
        {
            name_ = name;
        }
        std::string getSurname()const
        {
            return surname_;
        }
        void setSurname(const std::string& surname)
        {
            surname_ = surname;
        }

        friend bool operator<(const Employee& lhs, const Employee& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company 
    {
        std::string name_;
        std::vector<uint64_t> employeeIds_;
    public:
        std::string getName() const
        {
            return name_;
        }
        void setName(const std::string& name)
        {
            name_ = name;
        }
        const std::vector<uint64_t> getIds() const
        {
            return employeeIds_;
        }
        void setIds(const std::vector<uint64_t>& employeeIds)
        {
            employeeIds_ = employeeIds;
        }
    };

    std::vector<uint64_t> fromEmployeesToIds(const std::vector<Employee>& employees)
    {
        static std::map<Employee, uint64_t> employee_id_map = {{Employee("John", "Smith"), 1},{Employee("Jane", "Doe"), 2}};

        std::vector<uint64_t> ids;
        for (auto employee : employees)
        {
            ids.push_back(employee_id_map.at(employee));
        }
        return ids;
    }

    std::vector<Employee> toEmployeesFromIds(const std::vector<uint64_t>& ids)
    {
        static std::map<uint64_t, Employee> id_employee_map = {{1, Employee("John", "Smith")},{2, Employee("Jane", "Doe")}};

        std::vector<Employee> employees;
        for (auto id : ids)
        {
            employees.push_back(id_employee_map.at(id));
        }
        return employees;
    }

} // namespace ns

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Employee,
    (getName, setName, "employee_name"),
    (getSurname, setSurname, "employee_surname")
)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Company,
    (getName, setName, "company"),
    (getIds, setIds, "resources", 
     JSONCONS_RDWR, jsoncons::always_true(), 
     ns::toEmployeesFromIds, ns::fromEmployeesToIds)
)

int main()
{
    std::string input = R"(
{
    "company": "ExampleInc",
    "resources": [
        {
            "employee_name": "John",
            "employee_surname": "Smith"
        },
        {
            "employee_name": "Jane",
            "employee_surname": "Doe"
        }
    ]
}
    )";

    auto company = decode_json<ns::Company>(input);

    std::cout << "(1)\n" << company.getName() << "\n";
    for (auto id : company.getIds())
    {
        std::cout << id << "\n";
    }
    std::cout << "\n";

    std::string output;
    encode_json(company, output, indenting::indent);
    std::cout << "(2)\n" << output << "\n\n";
}
```

Output:

```
(1)
ExampleInc
1
2

(2)
{
    "company": "ExampleInc",
    "resources": [
        {
            "employee_name": "John",
            "employee_surname": "Smith"
        },
        {
            "employee_name": "Jane",
            "employee_surname": "Doe"
        }
    ]
}
```

<div id="A11"/>

#### Tidy data member (since 0.158.0)

```cpp
#include <jsoncons/json.hpp>

namespace ns {

   class Person 
   {
         std::string name_;
         std::optional<std::string> socialSecurityNumber_;
     public:
         Person(const std::string& name, const std::optional<std::string>& socialSecurityNumber)
           : name_(name), socialSecurityNumber_(socialSecurityNumber)
         {
         }
         std::string getName() const
         {
             return name_;
         }
         std::optional<std::string> getSsn() const
         {
             return socialSecurityNumber_;
         }
   };

} // namespace ns

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Person, 
  (getName, "name"),
  (getSsn, "social_security_number",
      jsoncons::always_true(),
      jsoncons::identity(), // or std::identity() if C++20
      [] (const std::optional<std::string>& unvalidated) {
          if (!unvalidated)
          {
              return unvalidated;
          }
          std::regex myRegex(("^(\\d{9})$"));
          if (!std::regex_match(*unvalidated, myRegex) ) {
              return std::optional<std::string>();
          }
          return unvalidated;
      }
   )
)

int main()
{
        std::string input = R"(
[
    {
        "name": "John Smith",
        "social_security_number": "123456789"
    },
    {
        "name": "Jane Doe",
        "social_security_number": "12345678"
    }
]
    )";

    auto persons = jsoncons::decode_json<std::vector<ns::Person>>(input);

    std::cout << "(1)\n";
    for (const auto& person : persons)
    {
        std::cout << person.getName() << ", " 
                  << (person.getSsn() ? *person.getSsn() : "n/a") << "\n";
    }
    std::cout << "\n";

    std::string output;
    jsoncons::encode_json(persons, output, indenting::indent);
    std::cout << "(2)\n" << output << "\n";
}
```
Output:
```
(1)
John Smith, 123456789
Jane Doe, n/a

(2)
[
    {
        "name": "John Smith",
        "social_security_number": "123456789"
    },
    {
        "name": "Jane Doe",
        "social_security_number": null
    }
]
```
