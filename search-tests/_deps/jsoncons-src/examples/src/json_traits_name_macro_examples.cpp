// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <cassert>
#include <regex>
#include <jsoncons/json.hpp>

namespace {
namespace ns {

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
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;
        Book2() = default;

        JSONCONS_TYPE_TRAITS_FRIEND
    public:
        BookCategory category() const {return category_;}

        const std::string& author() const {return author_;}

        const std::string& title() const{return title_;}

        double price() const{return price_;}
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

    // #4 Class with getters, setters and default constructor
    class Book4
    {
        BookCategory category_;
        std::string author_;
        std::string title_;
        double price_;

    public:
        BookCategory getCategory() const {return category_;}
        void setCategory(const BookCategory& value) {category_ = value;}

        const std::string& getAuthor() const {return author_;}
        void setAuthor(const std::string& value) {author_ = value;}

        const std::string& getTitle() const {return title_;}
        void setTitle(const std::string& value) {title_ = value;}

        double getPrice() const {return price_;}
        void setPrice(double value) {price_ = value;}
    };


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

    class Person 
    {
          std::string name_;
          jsoncons::optional<std::string> socialSecurityNumber_;
      public:
          Person(const std::string& name, const jsoncons::optional<std::string>& socialSecurityNumber)
            : name_(name), socialSecurityNumber_(socialSecurityNumber)
          {
          }
          std::string getName() const
          {
              return name_;
          }
          jsoncons::optional<std::string> getSsn() const
          {
              return socialSecurityNumber_;
          }
    };

} // namespace ns
} // namespace

// Declare the traits at global scope
JSONCONS_ENUM_NAME_TRAITS(ns::BookCategory,(fiction,"Fiction"),(biography,"Biography"))

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Book1,(category,"Category"),(author,"Author"),
                                          (title,"Title"),(price,"Price"))
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Book2,(category_,"Category"),(author_,"Author"),
                                          (title_,"Title"),(price_,"Price"))
JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Book3,(category,"Category"),(author,"Author"),
                                               (title,"Title"),(price,"Price"))
JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Book4,(getCategory,setCategory,"Category"),
                                                 (getAuthor,setAuthor,"Author"),
                                                 (getTitle,setTitle,"Title"),
                                                 (getPrice,setPrice,"Price"))

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

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Person, 
  (getName, "name"),
  (getSsn, "social_security_number", 
      JSONCONS_RDWR, jsoncons::always_true(),
      jsoncons::identity(),
      [] (const jsoncons::optional<std::string>& unvalidated) {
          if (!unvalidated)
          {
              return unvalidated;
          }
          std::regex myRegex(("^(\\d{9})$"));
          if (!std::regex_match(*unvalidated, myRegex) ) {
              return jsoncons::optional<std::string>();
          }
          return unvalidated;
      }
   )
)

using namespace jsoncons;

void json_type_traits_book_examples()
{
    const std::string input = R"(
    [
        {
            "Category" : "Fiction",
            "Author" : "Haruki Murakami",
            "Title" : "Kafka on the Shore",
            "Price" : 25.17
        },
        {
            "Category" : "Biography",
            "Author" : "Robert A. Caro",
            "Title" : "The Path to Power: The Years of Lyndon Johnson I",
            "Price" : 16.99
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
        std::cout << item.category() << ", "
                  << item.author() << ", " 
                  << item.title() << ", " 
                  << item.price() << "\n";
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
        std::cout << item.getCategory() << ", "
                  << item.getAuthor() << ", " 
                  << item.getTitle() << ", " 
                  << item.getPrice() << "\n";
    }
    std::cout << "\n";
    encode_json(books4, std::cout, indenting::indent);
    std::cout << "\n\n";
}

void translate_ids_from_to_employees()
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

void tidy_member()
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

    auto persons = decode_json<std::vector<ns::Person>>(input);

    std::cout << "(1)\n";
    for (const auto& person : persons)
    {
        std::cout << person.getName() << ", " 
                  << (person.getSsn() ? *person.getSsn() : "n/a") << "\n";
    }
    std::cout << "\n";

    std::string output;
    encode_json(persons, output, indenting::indent);
    std::cout << "(2)\n" << output << "\n";

}

int main()
{
    std::cout << "\njson_type_traits macro named examples\n\n";

    std::cout << std::setprecision(6);

    json_type_traits_book_examples();
    translate_ids_from_to_employees();
    tidy_member();

    std::cout << '\n';
}
