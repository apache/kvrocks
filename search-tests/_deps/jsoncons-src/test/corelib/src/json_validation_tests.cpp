// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <map>
#include <catch/catch.hpp>

namespace ns {

    struct employee_AMN
    {
        std::string name;
        uint64_t id;
        int age;
    };

    struct employee_NMN
    {
        std::string name;
        uint64_t id;
        int age;
    };

    class employee_ACGN
    {
        std::string name_;
        uint64_t id_;
        int age_;
    public:
        employee_ACGN(std::string name, uint64_t id, int age)
            : name_(name), id_(id), age_(age)
        {
        }

        const std::string& name() const {return name_;}
        uint64_t id() const {return id_;}
        int age() const {return age_;}
    };

    class employee_NCGN
    {
        std::string name_;
        uint64_t id_;
        int age_;
    public:
        employee_NCGN(std::string name, uint64_t id, int age)
            : name_(name), id_(id), age_(age)
        {
        }

        const std::string& name() const {return name_;}
        uint64_t id() const {return id_;}
        int age() const {return age_;}
    };

    class employee_AGSN
    {
        std::string name_;
        uint64_t id_;
        int age_;
    public:
        const std::string& name() const {return name_;}
        uint64_t id() const {return id_;}
        int age() const {return age_;}

        void name(const std::string& value) {name_ = value;}
        void id(uint64_t value) {id_ = value;}
        void age(int value) {age_ = value;}
    };

    class employee_NGSN
    {
        std::string name_;
        uint64_t id_;
        int age_;
    public:
        const std::string& name() const {return name_;}
        uint64_t id() const {return id_;}
        int age() const {return age_;}

        void name(const std::string& value) {name_ = value;}
        void id(uint64_t value) {id_ = value;}
        void age(int value) {age_ = value;}
    };

} // namespace jsoncons

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::employee_AMN,
    (name,"Name"),
    (id,"Id"),
    (age,"Age",JSONCONS_RDWR,
     [](int age) noexcept
     {
         return age >= 16 && age <= 68;
     }
    )
)

JSONCONS_N_MEMBER_NAME_TRAITS(ns::employee_NMN,3,
    (name,"Name"),
    (id,"Id"),
    (age,"Age",JSONCONS_RDWR,
     [](int age) noexcept
     {
         return age >= 16 && age <= 68;
     }
    )
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::employee_ACGN,
    (name,"Name"),
    (id,"Id"),
    (age,"Age",JSONCONS_RDWR,
     [](int age) noexcept
     {
         return age >= 16 && age <= 68;
     }
    )
)

JSONCONS_N_CTOR_GETTER_NAME_TRAITS(ns::employee_NCGN, 3,
    (name,"Name"),
    (id,"Id"),
    (age,"Age",JSONCONS_RDWR,
     [](int age) noexcept
     {
         return age >= 16 && age <= 68;
     }
    )
)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::employee_AGSN,
    (name,name,"Name"),
    (id,id,"Id"),
    (age,age,"Age",JSONCONS_RDWR,
     [](int age) noexcept
     {
         return age >= 16 && age <= 68;
     }
    )
)

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(ns::employee_NGSN, 3,
    (name,name,"Name"),
    (id,id,"Id"),
    (age,age,"Age",JSONCONS_RDWR,
     [](int age) noexcept
     {
         return age >= 16 && age <= 68;
     }
    )
)

TEST_CASE("json validator tests")
{
    const std::string input = R"(
    [
      {
        "Name" : "John Smith",
        "Id" : 22,
        "Age" : 345
      },
      {
        "Name" : "",
        "Id" : 23,
        "Age" : 36
      },
      {
        "Name" : "Jane Doe",
        "Id" : 24,
        "Age" : 34
      }
    ]
    )";

    SECTION("employee_AMN test")
    {
        jsoncons::json_string_cursor cursor(input);

        auto view = jsoncons::staj_array<ns::employee_AMN>(cursor);

        for (auto it = view.begin(); it != view.end(); ++it)
        {
            if (it.has_value())
            {
                auto val = *it;
            }
            else
            {
                REQUIRE_THROWS_WITH(*it, "Not a ns::employee_AMN: Unable to convert into the provided type");
            }
        }
    }

    SECTION("employee_NMN test")
    {
        jsoncons::json_string_cursor cursor(input);

        auto view = jsoncons::staj_array<ns::employee_NMN>(cursor);

        for (auto it = view.begin(); it != view.end(); ++it)
        {
            if (it.has_value())
            {
                auto val = *it;
            }
            else
            {
                REQUIRE_THROWS_WITH(*it, "Not a ns::employee_NMN: Unable to convert into the provided type");
            }
        }
    }

    SECTION("employee_ACGN test")
    {
        jsoncons::json_string_cursor cursor(input);

        auto view = jsoncons::staj_array<ns::employee_ACGN>(cursor);

        for (auto it = view.begin(); it != view.end(); ++it)
        {
            if (it.has_value())
            {
                auto val = *it;
            }
            else
            {
                REQUIRE_THROWS_WITH(*it, "Not a ns::employee_ACGN: Unable to convert into the provided type");
            }
        }
    }

    SECTION("employee_NCGN test")
    {
        jsoncons::json_string_cursor cursor(input);

        auto view = jsoncons::staj_array<ns::employee_NCGN>(cursor);

        for (auto it = view.begin(); it != view.end(); ++it)
        {
            if (it.has_value())
            {
                auto val = *it;
            }
            else
            {
                REQUIRE_THROWS_WITH(*it, "Not a ns::employee_NCGN: Unable to convert into the provided type");
            }
        }
    }

    SECTION("employee_AGSN test")
    {
        jsoncons::json_string_cursor cursor(input);

        auto view = jsoncons::staj_array<ns::employee_AGSN>(cursor);

        for (auto it = view.begin(); it != view.end(); ++it)
        {
            if (it.has_value())
            {
                auto val = *it;
            }
            else
            {
                REQUIRE_THROWS_WITH(*it,  "Not a ns::employee_AGSN: Unable to convert into the provided type");
            }
        }
    }

    SECTION("employee_NGSN test")
    {
        jsoncons::json_string_cursor cursor(input);

        auto view = jsoncons::staj_array<ns::employee_NGSN>(cursor);

        for (auto it = view.begin(); it != view.end(); ++it)
        {
            if (it.has_value())
            {
                auto val = *it;
            }
            else
            {
                REQUIRE_THROWS_WITH(*it,  "Not a ns::employee_NGSN: Unable to convert into the provided type");
            }
        }
    }
}

