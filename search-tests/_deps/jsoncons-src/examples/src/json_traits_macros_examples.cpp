// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <cassert>
#include <string>
#include <vector>
#include <list>
#include <iomanip>
#include <jsoncons/json.hpp>

namespace { 
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

    enum class BookCategory {fiction,biography};

    inline
    std::ostream& operator<<(std::ostream& os, const BookCategory& category)
    {
        switch (category)
        {
            case BookCategory::fiction: os << "fiction, "; break;
            case BookCategory::biography: os << "biography, "; break;
        }
        return os;
    }

    // #1 Class with public member data and default constructor   
    struct Book1
    {
        BookCategory category;
        std::string author;
        std::string title;
        double price;
    };

    // #2 Class with private member data and default constructor   
    class Book2
    {
        BookCategory category;
        std::string author;
        std::string title;
        double price;
        Book2() = default;

        JSONCONS_TYPE_TRAITS_FRIEND
    public:
        BookCategory get_category() const {return category;}

        const std::string& get_author() const {return author;}

        const std::string& get_title() const{return title;}

        double get_price() const{return price;}
    };

    // #3 Class with getters and initializing constructor
    class Book3
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
    public:
        Book3(BookCategory category,
              const std::string& author,
              const std::string& title,
              double price)
            : category_(category), author_(author), title_(title), price_(price)
        {
        }

        BookCategory category() const {return category_;}

        const std::string& author() const{return author_;}

        const std::string& title() const{return title_;}

        double price() const{return price_;}
    };

    // #4 Class with getters and setters
    class Book4
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
    public:
        Book4()
            : category_(), price_(0)
        {
        }

        Book4(BookCategory category,
              const std::string& author,
              const std::string& title,
              double price)
            : category_(category), author_(author), title_(title), price_(price)
        {
        }

        BookCategory get_category() const
        {
            return category_;
        }

        void set_category(BookCategory value)
        {
            category_ = value;
        }

        const std::string& get_author() const
        {
            return author_;
        }

        void set_author(const std::string& value)
        {
            author_ = value;
        }

        const std::string& get_title() const
        {
            return title_;
        }

        void set_title(const std::string& value)
        {
            title_ = value;
        }

        double get_price() const
        {
            return price_;
        }

        void set_price(double value)
        {
            price_ = value;
        }
    };

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

    struct smart_pointer_test
    {
        std::shared_ptr<std::string> field1;
        std::unique_ptr<std::string> field2;
        std::shared_ptr<std::string> field3;
        std::unique_ptr<std::string> field4;
        std::shared_ptr<std::string> field5;
        std::unique_ptr<std::string> field6;
        std::shared_ptr<std::string> field7;
        std::unique_ptr<std::string> field8;
    };

#if defined(JSONCONS_HAS_STD_OPTIONAL)
    class MetaDataReplyTest 
    {
    public:
        MetaDataReplyTest()
            : description()
        {
        }
        const std::string& GetStatus() const 
        {
            return status;
        }
        const std::string& GetPayload() const 
        {
            return payload;
        }
        const std::optional<std::string>& GetDescription() const 
        {
            return description;
        }
    private:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::string status;
        std::string payload;
        std::optional<std::string> description;
    };

#endif

} // namespace ns
} // namespace 

// Declare the traits at global scope
JSONCONS_ENUM_TRAITS(ns::BookCategory,fiction,biography)

JSONCONS_ALL_MEMBER_TRAITS(ns::Book1,category,author,title,price)
JSONCONS_ALL_MEMBER_TRAITS(ns::Book2,category,author,title,price)
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::Book3,category,author,title,price)
JSONCONS_ALL_GETTER_SETTER_TRAITS(ns::Book4,get_,set_,category,author,title,price)

JSONCONS_N_CTOR_GETTER_TRAITS(ns::HourlyEmployee, 3, firstName, lastName, wage, hours)
JSONCONS_N_CTOR_GETTER_TRAITS(ns::CommissionedEmployee, 4, firstName, lastName, baseSalary, commission, sales)
JSONCONS_POLYMORPHIC_TRAITS(ns::Employee, ns::HourlyEmployee, ns::CommissionedEmployee)

JSONCONS_N_MEMBER_TRAITS(ns::Bar,1,bar)
JSONCONS_N_MEMBER_TRAITS(ns::Baz,1,baz)
JSONCONS_POLYMORPHIC_TRAITS(ns::Foo, ns::Bar, ns::Baz)

#if defined(JSONCONS_HAS_STD_OPTIONAL)
JSONCONS_N_MEMBER_TRAITS(ns::MetaDataReplyTest, 2, status, payload, description)
#endif

// Declare the traits, first 4 members mandatory, last 4 non-mandatory
JSONCONS_N_MEMBER_TRAITS(ns::smart_pointer_test,4,field1,field2,field3,field4,field5,field6,field7,field8)

using namespace jsoncons;
    
#if defined(JSONCONS_HAS_STD_OPTIONAL)

void json_type_traits_optional_examples()
{
    std::string input1 = R"({
      "status": "OK",
      "payload": "Modified",
      "description": "TEST"
    })";
    std::string input2 = R"({
      "status": "OK",
      "payload": "Modified"
    })";

    auto val1 = decode_json<ns::MetaDataReplyTest>(input1);
    assert(val1.GetStatus() == "OK");
    assert(val1.GetPayload() == "Modified");
    assert(val1.GetDescription());
    assert(val1.GetDescription() == "TEST");

    auto val2 = decode_json<ns::MetaDataReplyTest>(input2);
    assert(val2.GetStatus() == "OK");
    assert(val2.GetPayload() == "Modified");
    assert(!val2.GetDescription());

    std::string output1;
    std::string output2;

    encode_json(val2, output2, indenting::indent);
    encode_json(val1, output1, indenting::indent);

    std::cout << "(1)\n";
    std::cout << output1 << "\n\n";

    std::cout << "(2)\n";
    std::cout << output2 << "\n\n";
}

#endif

void smart_pointer_traits_test()
{
    ns::smart_pointer_test val;
    val.field1 = std::make_shared<std::string>("Field 1"); 
    val.field2 = jsoncons::make_unique<std::string>("Field 2"); 
    val.field3 = std::shared_ptr<std::string>(nullptr);
    val.field4 = std::unique_ptr<std::string>(nullptr);
    val.field5 = std::make_shared<std::string>("Field 5"); 
    val.field6 = jsoncons::make_unique<std::string>("Field 6"); 
    val.field7 = std::shared_ptr<std::string>(nullptr);
    val.field8 = std::unique_ptr<std::string>(nullptr);

    std::string buf;
    encode_json(val, buf, indenting::indent);

    std::cout << buf << "\n";

    auto other = decode_json<ns::smart_pointer_test>(buf);

    assert(*other.field1 == *val.field1);
    assert(*other.field2 == *val.field2);
    assert(!other.field3);
    assert(!other.field4);
    assert(*other.field5 == *val.field5);
    assert(*other.field6 == *val.field6);
    assert(!other.field7);
    assert(!other.field8);
}

void json_type_traits_book_examples()
{
    const std::string input = R"(
    [
        {
            "category" : "fiction",
            "author" : "Haruki Murakami",
            "title" : "Kafka on the Shore",
            "price" : 25.17
        },
        {
            "category" : "biography",
            "author" : "Robert A. Caro",
            "title" : "The Path to Power: The Years of Lyndon Johnson I",
            "price" : 16.99
        }
    ]
    )";

    std::cout << "(1)\n\n";
    auto books1 = decode_json<std::vector<ns::Book1>>(input);
    for (const auto& item : books1)
    {
        std::cout << item.category << ", "
                  << item.author << ", " 
                  << item.title << ", " 
                  << item.price << "\n";
    }
    std::cout << "\n";
    encode_json(books1, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(2)\n\n";
    auto books2 = decode_json<std::vector<ns::Book2>>(input);
    for (const auto& item : books2)
    {
        std::cout << item.get_category() << ", "
                  << item.get_author() << ", " 
                  << item.get_title() << ", " 
                  << item.get_price() << "\n";
    }
    std::cout << "\n";
    encode_json(books2, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(3)\n\n";
    auto books3 = decode_json<std::vector<ns::Book3>>(input);
    for (const auto& item : books3)
    {
        std::cout << item.category() << ", "
                  << item.author() << ", " 
                  << item.title() << ", " 
                  << item.price() << "\n";
    }
    std::cout << "\n";
    encode_json(books3, std::cout, indenting::indent);
    std::cout << "\n\n";

    std::cout << "(4)\n\n";
    auto books4 = decode_json<std::vector<ns::Book4>>(input);
    for (const auto& item : books4)
    {
        std::cout << item.get_category() << ", "
                  << item.get_author() << ", " 
                  << item.get_title() << ", " 
                  << item.get_price() << "\n";
    }
    std::cout << "\n";
    encode_json(books4, std::cout, indenting::indent);
    std::cout << "\n\n";
}

void employee_polymorphic_example()
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

void foo_bar_baz_example()
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

int main()
{
    std::cout << "\njson_type_traits macro examples\n\n";

    std::cout << std::setprecision(6);

    json_type_traits_book_examples();
    employee_polymorphic_example();
    foo_bar_baz_example();

#if defined(JSONCONS_HAS_STD_OPTIONAL)
    json_type_traits_optional_examples();
#endif
    smart_pointer_traits_test();

    std::cout << '\n';
}

