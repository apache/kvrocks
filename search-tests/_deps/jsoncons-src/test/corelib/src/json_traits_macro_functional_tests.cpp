// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#if defined(_MSC_VER)
#include "windows.h" // test no inadvertant macro expansions
#endif
#include <catch/catch.hpp>
#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cstdint>
#include <regex>
#include <jsoncons/json.hpp>
#include <functional>

namespace {
namespace ns {
 
    class Person_NCGN   
    {
          std::string name_;
          jsoncons::optional<std::string> socialSecurityNumber_;
          jsoncons::optional<std::string> birthDate_;
      public:
          Person_NCGN(const std::string& name, const jsoncons::optional<std::string>& socialSecurityNumber, 
                      const jsoncons::optional<std::string>& birthDate = jsoncons::optional<std::string>())
            : name_(name), socialSecurityNumber_(socialSecurityNumber), birthDate_(birthDate)
          {
          }
          std::string getName() const
          {
              return name_;
          }
          jsoncons::optional<std::string> getSocialSecurityNumber() const
          {
              return socialSecurityNumber_;
          }
          jsoncons::optional<std::string> getBirthDate() const
          {
              return birthDate_;
          }
    };

    class Person_ACGN 
    {
          std::string name_;
          jsoncons::optional<std::string> socialSecurityNumber_;
      public:
          Person_ACGN(const std::string& name, const jsoncons::optional<std::string>& socialSecurityNumber)
            : name_(name), socialSecurityNumber_(socialSecurityNumber)
          {
          }
          std::string getName() const
          {
              return name_;
          }
          jsoncons::optional<std::string> getSocialSecurityNumber() const
          {
              return socialSecurityNumber_;
          }
    };

    class Employee_NMN
    {
    public:
        std::string name_;
        std::string surname_;

        Employee_NMN() = default;

        Employee_NMN(const std::string& name, const std::string& surname)
            : name_(name), surname_(surname)
        {
        }

        friend bool operator<(const Employee_NMN& lhs, const Employee_NMN& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company_NMN 
    {
    public:
        std::string name_;
        std::vector<uint64_t> employeeIds_;
        jsoncons::optional<double> rating_;

        Company_NMN() = default;

        Company_NMN(const std::string& name, const std::vector<uint64_t>& employeeIds)
            : name_(name), employeeIds_(employeeIds), rating_()
        {
        }
    };

    class Employee_AMN
    {
    public:
        std::string name_;
        std::string surname_;

        Employee_AMN() = default;

        Employee_AMN(const std::string& name, const std::string& surname)
            : name_(name), surname_(surname)
        {
        }

        friend bool operator<(const Employee_AMN& lhs, const Employee_AMN& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company_AMN 
    {
    public:
        std::string name_;
        std::vector<uint64_t> employeeIds_;

        Company_AMN() = default;

        Company_AMN(const std::string& name, const std::vector<uint64_t>& employeeIds)
            : name_(name), employeeIds_(employeeIds)
        {
        }
    };

    class Employee_NGSN
    {
        std::string name_;
        std::string surname_;
    public:
        Employee_NGSN() = default;

        Employee_NGSN(const std::string& name, const std::string& surname)
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

        friend bool operator<(const Employee_NGSN& lhs, const Employee_NGSN& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company_NGSN 
    {
        std::string name_;
        std::vector<uint64_t> employeeIds_;
        jsoncons::optional<double> rating_;
    public:
        Company_NGSN() = default;

        Company_NGSN(const std::string& name, const std::vector<uint64_t>& employeeIds)
            : name_(name), employeeIds_(employeeIds), rating_()
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
        const std::vector<uint64_t> getIds() const
        {
            return employeeIds_;
        }
        void setIds(const std::vector<uint64_t>& employeeIds)
        {
            employeeIds_ = employeeIds;
        }

        const jsoncons::optional<double>& getRating() const
        {
            return rating_;
        }
        void setRating(const jsoncons::optional<double>& rating)
        {
            rating_ = rating;
        }    
    };

    class Employee_AGSN
    {
        std::string name_;
        std::string surname_;
    public:
        Employee_AGSN() = default;

        Employee_AGSN(const std::string& name, const std::string& surname)
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

        friend bool operator<(const Employee_AGSN& lhs, const Employee_AGSN& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company_AGSN 
    {
        std::string name_;
        std::vector<uint64_t> employeeIds_;
    public:
        Company_AGSN() = default;

        Company_AGSN(const std::string& name, const std::vector<uint64_t>& employeeIds)
            : name_(name), employeeIds_(employeeIds)
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
        const std::vector<uint64_t> getIds() const
        {
            return employeeIds_;
        }
        void setIds(const std::vector<uint64_t>& employeeIds)
        {
            employeeIds_ = employeeIds;
        }
    };

    class Employee_NCGN
    {
        std::string name_;
        std::string surname_;
    public:
        Employee_NCGN(const std::string& name, const std::string& surname)
            : name_(name), surname_(surname)
        {
        }

        std::string getName() const
        {
            return name_;
        }
        std::string getSurname()const
        {
            return surname_;
        }

        friend bool operator<(const Employee_NCGN& lhs, const Employee_NCGN& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company_NCGN 
    {
        std::string name_;
        std::vector<uint64_t> employeeIds_;
        jsoncons::optional<double> rating_;
    public:
        Company_NCGN(const std::string& name, const std::vector<uint64_t>& employeeIds,
                     const jsoncons::optional<double>& rating = jsoncons::optional<double>())
            : name_(name), employeeIds_(employeeIds), rating_(rating)
        {
        }

        std::string getName() const
        {
            return name_;
        }
        const std::vector<uint64_t> getIds() const
        {
            return employeeIds_;
        }

        const jsoncons::optional<double>& getRating() const
        {
            return rating_;
        }
    };

    class Employee_ACGN
    {
        std::string name_;
        std::string surname_;
    public:
        Employee_ACGN(const std::string& name, const std::string& surname)
            : name_(name), surname_(surname)
        {
        }

        std::string getName() const
        {
            return name_;
        }
        std::string getSurname() const
        {
            return surname_;
        }

        friend bool operator<(const Employee_ACGN& lhs, const Employee_ACGN& rhs)
        {
            if (lhs.surname_ < rhs.surname_)
                return true;
            return lhs.name_ < rhs.name_;
        }
    };

    class Company_ACGN 
    {
        std::string name_;
        std::vector<uint64_t> employeeIds_;
    public:
        Company_ACGN(const std::string& name, const std::vector<uint64_t>& employeeIds)
            : name_(name), employeeIds_(employeeIds)
        {
        }

        std::string getName() const
        {
            return name_;
        }
        const std::vector<uint64_t> getIds() const
        {
            return employeeIds_;
        }
    };

    template <typename Employee>
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

    template <typename Employee>
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

    class Shape_ACGN
    {
    public:
        virtual ~Shape_ACGN() = default;
        virtual double area() const = 0;
    };
      
    class Rectangle_ACGN : public Shape_ACGN
    {
        double height_;
        double width_;
    public:
        Rectangle_ACGN(double height, double width)
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

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle_ACGN : public Shape_ACGN
    { 
        double height_;
        double width_;

    public:
        Triangle_ACGN(double height, double width)
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

        double area() const override
        {
            return (height_ * width_)/2.0;
        }

        const std::string& type() const
        {
            static const std::string type_ = "triangle"; 
            return type_;
        }
    };                 

    class Circle_ACGN : public Shape_ACGN
    { 
        double radius_;

    public:
        Circle_ACGN(double radius)
            : radius_(radius)
        {
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

        const std::string& type() const
        {
            static const std::string type_ = "circle"; 
            return type_;
        }
    };                 

    class Shape_AGSN
    {
    public:
        virtual ~Shape_AGSN() = default;
        virtual double area() const = 0;
    };

    class Rectangle_AGSN : public Shape_AGSN
    {
        double height_;
        double width_;
    public:
        Rectangle_AGSN()
            : height_(0), width_(0)
        {
        }

        double getHeight() const
        {
            return height_;
        }

        void setHeight(double value)
        {
            height_ = value;
        }

        double getWidth() const
        {
            return width_;
        }

        void setWidth(double value)
        {
            width_ = value;
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle_AGSN : public Shape_AGSN
    { 
        double height_;
        double width_;

    public:
        Triangle_AGSN()
            : height_(0), width_(0)
        {
        }

        double getHeight() const
        {
            return height_;
        }

        void setHeight(double value)
        {
            height_ = value;
        }

        double getWidth() const
        {
            return width_;
        }

        void setWidth(double value)
        {
            width_ = value;
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }

        const std::string& getType() const
        {
            static const std::string type_ = "triangle"; 
            return type_;
        }
    };                 

    class Circle_AGSN : public Shape_AGSN
    { 
        double radius_;

    public:
        Circle_AGSN()
            : radius_(0)
        {
        }

        double getRadius() const
        {
            return radius_;
        }

        void setRadius(double value)
        {
            radius_ = value;
        }

        double area() const override
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }

        const std::string& getType() const
        {
            static const std::string type_ = "circle"; 
            return type_;
        }
    };                 

    class Shape_NGSN
    {
    public:
        virtual ~Shape_NGSN() = default;
        virtual double area() const = 0;
    };

    class Rectangle_NGSN : public Shape_NGSN
    {
        double height_;
        double width_;
    public:
        Rectangle_NGSN()
            : height_(0), width_(0)
        {
        }

        double getHeight() const
        {
            return height_;
        }

        void setHeight(double value)
        {
            height_ = value;
        }

        double getWidth() const
        {
            return width_;
        }

        void setWidth(double value)
        {
            width_ = value;
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle_NGSN : public Shape_NGSN
    { 
        double height_;
        double width_;

    public:
        Triangle_NGSN()
            : height_(0), width_(0)
        {
        }

        double getHeight() const
        {
            return height_;
        }

        void setHeight(double value)
        {
            height_ = value;
        }

        double getWidth() const
        {
            return width_;
        }

        void setWidth(double value)
        {
            width_ = value;
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }

        const std::string& getType() const
        {
            static const std::string type_ = "triangle"; 
            return type_;
        }
    };                 

    class Circle_NGSN : public Shape_NGSN
    { 
        double radius_;

    public:
        Circle_NGSN()
            : radius_(0)
        {
        }

        double getRadius() const
        {
            return radius_;
        }

        void setRadius(double value)
        {
            radius_ = value;
        }

        double area() const override
        {
            constexpr double pi = 3.14159265358979323846;
            return pi*radius_*radius_;
        }

        const std::string& getType() const
        {
            static const std::string type_ = "circle"; 
            return type_;
        }
    };                 

    class Shape_AMN
    {
    public:
        virtual ~Shape_AMN() = default;
        virtual double area() const = 0;
    };

    class Rectangle_AMN : public Shape_AMN
    {
        JSONCONS_TYPE_TRAITS_FRIEND
        double height_;
        double width_;
    public:
        Rectangle_AMN()
            : height_(0), width_(0)
        {
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle_AMN : public Shape_AMN
    { 
        JSONCONS_TYPE_TRAITS_FRIEND
        static const std::string type_;
        double height_;
        double width_;

    public:
        Triangle_AMN()
            : height_(0), width_(0)
        {
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }
    };                 

    const std::string Triangle_AMN::type_ = "triangle";

    class Circle_AMN : public Shape_AMN
    { 
        JSONCONS_TYPE_TRAITS_FRIEND
        static const std::string type_;
        double radius_;

    public:
        Circle_AMN()
            : radius_(0)
        {
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

    const std::string Circle_AMN::type_ = "circle";

    class Shape_NMN
    {
    public:
        virtual ~Shape_NMN() = default;
        virtual double area() const = 0;
    };

    class Rectangle_NMN : public Shape_NMN
    {
        JSONCONS_TYPE_TRAITS_FRIEND
        double height_;
        double width_;
    public:
        Rectangle_NMN()
            : height_(0), width_(0)
        {
        }

        double area() const override
        {
            return height_ * width_;
        }
    };

    class Triangle_NMN : public Shape_NMN
    { 
        JSONCONS_TYPE_TRAITS_FRIEND
        static const std::string type_;
        double height_;
        double width_;

    public:
        Triangle_NMN()
            : height_(0), width_(0)
        {
        }

        double area() const override
        {
            return (height_ * width_)/2.0;
        }
    };                 

    const std::string Triangle_NMN::type_ = "triangle";

    class Circle_NMN : public Shape_NMN
    { 
        JSONCONS_TYPE_TRAITS_FRIEND
        static const std::string type_;
        double radius_;

    public:
        Circle_NMN()
            : radius_(0)
        {
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

    const std::string Circle_NMN::type_ = "circle";

    const auto rectangle_marker = [](double) noexcept {return "rectangle"; };
    //const auto triangle_marker = [](double) noexcept {return "triangle";};
    //const auto circle_marker = [](double) noexcept {return "circle";};
          
} // namespace
} // ns

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Rectangle_ACGN,
    (height,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (height, "height", JSONCONS_RDWR),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Triangle_ACGN,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (height, "height"),
    (width, "width")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Circle_ACGN,
    (type,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (radius, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape_ACGN,ns::Rectangle_ACGN,ns::Triangle_ACGN,ns::Circle_ACGN)


JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Rectangle_AGSN,
    (getHeight, ,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (getHeight, setHeight, "height"),
    (getWidth, setWidth, "width")
)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Triangle_AGSN,
    (getType,,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (getHeight, setHeight, "height"),
    (getWidth, setWidth, "width")
)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Circle_AGSN,
    (getType,,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (getRadius, setRadius, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape_AGSN,ns::Rectangle_AGSN,ns::Triangle_AGSN,ns::Circle_AGSN)

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(ns::Rectangle_NGSN, 3,
    (getHeight, ,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (getHeight, setHeight, "height"),
    (getWidth, setWidth, "width")
)

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(ns::Triangle_NGSN, 3,
    (getType,,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (getHeight, setHeight, "height"),
    (getWidth, setWidth, "width")
)

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(ns::Circle_NGSN, 2,
    (getType,,"type", JSONCONS_RDONLY, [](const std::string& type) {return type == "circle";}),
    (getRadius, setRadius, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape_NGSN,ns::Rectangle_NGSN,ns::Triangle_NGSN,ns::Circle_NGSN)

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Rectangle_AMN,
    (height_,"type",JSONCONS_RDONLY,
        [](const std::string& type) noexcept {return type == "rectangle"; }, 
        ns::rectangle_marker),
    (height_, "height"),
    (width_, "width")
)

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Triangle_AMN,
    (type_,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (height_, "height"),
    (width_, "width")
)

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Circle_AMN,
    (type_,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (radius_, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape_AMN,ns::Rectangle_AMN,ns::Triangle_AMN,ns::Circle_AMN)

JSONCONS_N_MEMBER_NAME_TRAITS(ns::Rectangle_NMN, 3,
    (height_,"type",JSONCONS_RDONLY,
     [](const std::string& type) noexcept{return type == "rectangle";},
     ns::rectangle_marker),
    (height_, "height"),
    (width_, "width")
) 
JSONCONS_N_MEMBER_NAME_TRAITS(ns::Triangle_NMN, 3,
    (type_,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "triangle";}),
    (height_, "height"),
    (width_, "width")
)

JSONCONS_N_MEMBER_NAME_TRAITS(ns::Circle_NMN, 2,
    (type_,"type", JSONCONS_RDONLY, [](const std::string& type) noexcept {return type == "circle";}),
    (radius_, "radius")
)

JSONCONS_POLYMORPHIC_TRAITS(ns::Shape_NMN,ns::Rectangle_NMN,ns::Triangle_NMN,ns::Circle_NMN)

JSONCONS_N_MEMBER_NAME_TRAITS(ns::Employee_NMN, 2,
    (name_, "employee_name"),
    (surname_, "employee_surname")
)

JSONCONS_N_MEMBER_NAME_TRAITS(ns::Company_NMN, 2,
    (name_, "company"),
    (employeeIds_, "resources", JSONCONS_RDWR, jsoncons::always_true(), 
     ns::toEmployeesFromIds<ns::Employee_NMN>, ns::fromEmployeesToIds<ns::Employee_NMN>),
    (rating_, "rating")
)

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Employee_AMN,
    (name_, "employee_name"),
    (surname_, "employee_surname")
)

JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::Company_AMN,
    (name_, "company"),
    (employeeIds_, "resources", JSONCONS_RDWR, jsoncons::always_true(), 
     ns::toEmployeesFromIds<ns::Employee_AMN>, ns::fromEmployeesToIds<ns::Employee_AMN>)
)

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(ns::Employee_NGSN, 2,
                                      (getName, setName, "employee_name"),
                                      (getSurname, setSurname, "employee_surname")
  )

JSONCONS_N_GETTER_SETTER_NAME_TRAITS(ns::Company_NGSN, 2,
  (getName, setName, "company"),
  (getIds, setIds, "resources", JSONCONS_RDWR, jsoncons::always_true(), 
   ns::toEmployeesFromIds<ns::Employee_NGSN>, ns::fromEmployeesToIds<ns::Employee_NGSN>),
  (getRating, setRating, "rating")
)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Employee_AGSN,
    (getName, setName, "employee_name"),
    (getSurname, setSurname, "employee_surname")
)

JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS(ns::Company_AGSN,
    (getName, setName, "company"),
    (getIds, setIds, "resources", JSONCONS_RDWR, jsoncons::always_true(), 
     ns::toEmployeesFromIds<ns::Employee_AGSN>, ns::fromEmployeesToIds<ns::Employee_AGSN>)
)

JSONCONS_N_CTOR_GETTER_NAME_TRAITS(ns::Employee_NCGN, 2,
                                      (getName, "employee_name"),
                                      (getSurname, "employee_surname")
  )

JSONCONS_N_CTOR_GETTER_NAME_TRAITS(ns::Company_NCGN, 2,
  (getName, "company"),
  (getIds, "resources", JSONCONS_RDWR, jsoncons::always_true(), 
   ns::toEmployeesFromIds<ns::Employee_NCGN>, ns::fromEmployeesToIds<ns::Employee_NCGN>),
  (getRating, "rating")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Employee_ACGN,
    (getName, "employee_name"),
    (getSurname, "employee_surname")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Company_ACGN,
    (getName, "company"),
    (getIds, "resources", JSONCONS_RDWR, jsoncons::always_true{}, 
     ns::toEmployeesFromIds<ns::Employee_ACGN>, ns::fromEmployeesToIds<ns::Employee_ACGN>)
)

JSONCONS_N_CTOR_GETTER_NAME_TRAITS(ns::Person_NCGN, 2,
    (getName, "name"),
    (getSocialSecurityNumber, "social_security_number", 
      JSONCONS_RDWR, jsoncons::always_true{},
      jsoncons::identity(),
      [] (const jsoncons::optional<std::string>& unvalidated) {
          if (!unvalidated)
          {
              return unvalidated;
          }
          std::regex myRegex(("^(\\d{9})$"));
          if (!std::regex_match(*unvalidated, myRegex) ) {
              throw std::runtime_error("Invalid social security number");
          }
          return jsoncons::optional<std::string>(unvalidated);
      }
   ),
   (getBirthDate, "birth_date")
)

JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS(ns::Person_ACGN, 
  (getName, "name"),
  (getSocialSecurityNumber, "social_security_number", 
      JSONCONS_RDWR, 
      [] (const jsoncons::optional<std::string>& unvalidated) {
          if (!unvalidated)
          {
              return false;
          }
          std::regex myRegex("^(\\d{9})$");
          return std::regex_match(*unvalidated, myRegex);
      },
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

TEST_CASE("JSONCONS_N_GETTER_SETTER_NAME_TRAITS transform tests")
{
    SECTION("test 1")
    {
        std::vector<uint64_t> ids = {1,2};

        ns::Company_NGSN company("Example", ids);

        std::string output1;
        encode_json(company, output1);
        //std::cout << output1 << "\n\n";
        auto company2 = decode_json<ns::Company_NGSN>(output1);
        std::string output2;
        encode_json(company, output2);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<ns::Company_NGSN>());
        CHECK(j.is<ns::Company_AGSN>());
    }
} 

TEST_CASE("JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS transform tests")
{
    SECTION("test 1")
    {
        std::vector<uint64_t> ids = {1,2};
        
        ns::Company_AGSN company("Example", ids);

        std::string output1;
        encode_json(company, output1);
        //std::cout << output1 << "\n\n";
        auto company2 = decode_json<ns::Company_AGSN>(output1);
        std::string output2;
        encode_json(company, output2);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<ns::Company_NGSN>());
        CHECK(j.is<ns::Company_AGSN>());
    }
} 

TEST_CASE("JSONCONS_N_CTOR_GETTER_NAME_TRAITS transform tests")
{
    SECTION("test 1")
    {
        std::vector<ns::Employee_NCGN> employees = {ns::Employee_NCGN("John", "Smith"), ns::Employee_NCGN("Jane", "Doe")};    

        std::string output1;
        encode_json(employees, output1, indenting::indent);
        auto employees2 = decode_json<std::vector<ns::Employee_NCGN>>(output1);
        std::string output2;
        encode_json(employees2, output2, indenting::indent);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<std::vector<ns::Employee_NCGN>>());
    }
    SECTION("test 2")
    {
        std::vector<uint64_t> ids = {1,2};

        ns::Company_NCGN company("Example", ids);

        std::string output1;
        encode_json(company, output1);
        //std::cout << output1 << "\n\n";
        auto company2 = decode_json<ns::Company_NCGN>(output1);
        std::string output2;
        encode_json(company, output2);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<ns::Company_NCGN>());
        CHECK(j.is<ns::Company_ACGN>());
    }
} 

TEST_CASE("JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS transform tests")
{
    SECTION("test 1")
    {
        std::vector<uint64_t> ids = {1,2};
        
        ns::Company_ACGN company("Example", ids);

        std::string output1;
        encode_json(company, output1);
        //std::cout << output1 << "\n\n";
        auto company2 = decode_json<ns::Company_ACGN>(output1);
        std::string output2;
        encode_json(company, output2);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<ns::Company_NCGN>());
        CHECK(j.is<ns::Company_ACGN>());
    }
} 

TEST_CASE("JSONCONS_N_MEMBER_NAME_TRAITS transform tests")
{
    SECTION("test 1")
    {
        std::vector<uint64_t> ids = {1,2};

        ns::Company_NMN company("Example", ids);

        std::string output1;
        encode_json(company, output1);
        //std::cout << output1 << "\n\n";
        auto company2 = decode_json<ns::Company_NMN>(output1);
        std::string output2;
        encode_json(company, output2);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<ns::Company_NMN>());
        CHECK(j.is<ns::Company_AMN>());
    }
} 

TEST_CASE("JSONCONS_ALL_MEMBER_NAME_TRAITS transform tests")
{
    SECTION("test 1")
    {
        std::vector<uint64_t> ids = {1,2};
        
        ns::Company_AMN company("Example", ids);

        std::string output1;
        encode_json(company, output1);
        //std::cout << output1 << "\n\n";
        auto company2 = decode_json<ns::Company_AMN>(output1);
        std::string output2;
        encode_json(company, output2);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<ns::Company_NMN>());
        CHECK(j.is<ns::Company_AMN>());
    }
} 

#if defined(JSONCONS_HAS_STD_REGEX)
TEST_CASE("JSONCONS_N_CTOR_GETTER_NAME_TRAITS validation tests")
{
    SECTION("test 1")
    {
        std::vector<ns::Person_NCGN> persons = {ns::Person_NCGN("John Smith", "123456789"), ns::Person_NCGN("Jane Doe", "234567890")};    

        std::string output1;
        encode_json(persons, output1, indenting::indent);
        auto persons2 = decode_json<std::vector<ns::Person_NCGN>>(output1);
        std::string output2;
        encode_json(persons2, output2, indenting::indent);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<std::vector<ns::Person_NCGN>>());
    }
} 

TEST_CASE("JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS validation tests")
{
    SECTION("success")
    {
        std::vector<ns::Person_ACGN> persons = {ns::Person_ACGN("John Smith", "123456789"), ns::Person_ACGN("Jane Doe", "123456789")};    

        std::string output1;
        encode_json(persons, output1, indenting::indent);
        auto persons2 = decode_json<std::vector<ns::Person_ACGN>>(output1);
        std::string output2;
        encode_json(persons2, output2, indenting::indent);
        CHECK(output2 == output1);

        auto j = decode_json<json>(output2);
        CHECK(j.is<std::vector<ns::Person_ACGN>>());    
    }
    SECTION("failure")
    {
        std::vector<ns::Person_ACGN> persons1 = {ns::Person_ACGN("John Smith", "123456789"), ns::Person_ACGN("Jane Doe", "12345678")};    

        std::string output1;
        encode_json(persons1, output1, indenting::indent);
        CHECK_THROWS(decode_json<std::vector<ns::Person_ACGN>>(output1));
    }
} 
#endif

TEST_CASE("JSONCONS_ALL_CTOR_GETTER_NAME_TRAITS polymorphic and variant tests")
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 3.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    SECTION("polymorphic test")
    {
        auto shapes = decode_json<std::vector<std::unique_ptr<ns::Shape_ACGN>>>(input);
        REQUIRE(shapes.size() == 3);
        std::string output;

        encode_json(shapes, output, indenting::indent);

        auto j = decode_json<json>(input);
        REQUIRE((j.is_array() && j.size() == 3));
        CHECK(j[0].is<ns::Rectangle_ACGN>());
        CHECK_FALSE(j[0].is<ns::Triangle_ACGN>());
        CHECK_FALSE(j[0].is<ns::Circle_ACGN>());
        CHECK(j[1].is<ns::Triangle_ACGN>());
        CHECK_FALSE(j[1].is<ns::Rectangle_ACGN>());
        CHECK_FALSE(j[1].is<ns::Circle_ACGN>());
        CHECK(j[2].is<ns::Circle_ACGN>());
        CHECK_FALSE(j[2].is<ns::Rectangle_ACGN>());
        CHECK_FALSE(j[2].is<ns::Triangle_ACGN>());

        auto j2 = decode_json<json>(output);
        CHECK(j2 == j);
    }

#if defined(JSONCONS_HAS_STD_VARIANT)
    SECTION("variant test")
    {
        using shapes_t = std::variant<ns::Rectangle_ACGN,ns::Triangle_ACGN,ns::Circle_ACGN>;
        auto shapes = decode_json<std::vector<shapes_t>>(input);
        REQUIRE(shapes.size() == 3);

        /*auto visitor = [](auto&& shape) {
            using T = std::decay_t<decltype(shape)>;
            if constexpr (std::is_same_v<T, ns::Rectangle_ACGN>)
                std::cout << "rectangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Triangle_ACGN>)
                std::cout << "triangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Circle_ACGN>)
                std::cout << "circle area: " << shape.area() << '\n';
        };
        for (const auto& shape : shapes)
        {
            std::visit(visitor, shape);
        }*/

        std::string output;
        encode_json(shapes, output, indenting::indent);
        //std::cout << output << "\n";

    }
#endif
} 

TEST_CASE("JSONCONS_ALL_GETTER_SETTER_NAME_TRAITS polymorphic and variant tests")
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 3.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    SECTION("polymorphic test")
    {
        auto shapes = decode_json<std::vector<std::unique_ptr<ns::Shape_AGSN>>>(input);
        REQUIRE(shapes.size() == 3);
        std::string output;

        encode_json(shapes, output, indenting::indent);

        auto j = decode_json<json>(input);
        REQUIRE((j.is_array() && j.size() == 3));
        CHECK(j[0].is<ns::Rectangle_AGSN>());
        CHECK_FALSE(j[0].is<ns::Triangle_AGSN>());
        CHECK_FALSE(j[0].is<ns::Circle_AGSN>());
        CHECK(j[1].is<ns::Triangle_AGSN>());
        CHECK_FALSE(j[1].is<ns::Rectangle_AGSN>());
        CHECK_FALSE(j[1].is<ns::Circle_AGSN>());
        CHECK(j[2].is<ns::Circle_AGSN>());
        CHECK_FALSE(j[2].is<ns::Rectangle_AGSN>());
        CHECK_FALSE(j[2].is<ns::Triangle_AGSN>());

        auto j2 = decode_json<json>(output);
        CHECK(j2 == j);
    }

#if defined(JSONCONS_HAS_STD_VARIANT)
    SECTION("variant test")
    {
        using shapes_t = std::variant<ns::Rectangle_AGSN,ns::Triangle_AGSN,ns::Circle_AGSN>;
        auto shapes = decode_json<std::vector<shapes_t>>(input);
        REQUIRE(shapes.size() == 3);

        /*auto visitor = [](auto&& shape) {
            using T = std::decay_t<decltype(shape)>;
            if constexpr (std::is_same_v<T, ns::Rectangle_AGSN>)
                std::cout << "rectangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Triangle_AGSN>)
                std::cout << "triangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Circle_AGSN>)
                std::cout << "circle area: " << shape.area() << '\n';
        };
        for (const auto& shape : shapes)
        {
            std::visit(visitor, shape);
        }*/

        std::string output;
        encode_json(shapes, output, indenting::indent);
        //std::cout << output << "\n";

    }
#endif
} 

TEST_CASE("JSONCONS_N_GETTER_SETTER_NAME_TRAITS polymorphic and variant tests")
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 3.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    SECTION("polymorphic test")
    {
        auto shapes = decode_json<std::vector<std::unique_ptr<ns::Shape_NGSN>>>(input);
        REQUIRE(shapes.size() == 3);
        std::string output;

        encode_json(shapes, output, indenting::indent);

        auto j = decode_json<json>(input);
        REQUIRE((j.is_array() && j.size() == 3));
        CHECK(j[0].is<ns::Rectangle_NGSN>());
        CHECK_FALSE(j[0].is<ns::Triangle_NGSN>());
        CHECK_FALSE(j[0].is<ns::Circle_NGSN>());
        CHECK(j[1].is<ns::Triangle_NGSN>());
        CHECK_FALSE(j[1].is<ns::Rectangle_NGSN>());
        CHECK_FALSE(j[1].is<ns::Circle_NGSN>());
        CHECK(j[2].is<ns::Circle_NGSN>());
        CHECK_FALSE(j[2].is<ns::Rectangle_NGSN>());
        CHECK_FALSE(j[2].is<ns::Triangle_NGSN>());

        auto j2 = decode_json<json>(output);
        CHECK(j2 == j);
    }

#if defined(JSONCONS_HAS_STD_VARIANT)
    SECTION("variant test")
    {
        using shapes_t = std::variant<ns::Rectangle_NGSN,ns::Triangle_NGSN,ns::Circle_NGSN>;
        auto shapes = decode_json<std::vector<shapes_t>>(input);
        REQUIRE(shapes.size() == 3);

        /*auto visitor = [](auto&& shape) {
            using T = std::decay_t<decltype(shape)>;
            if constexpr (std::is_same_v<T, ns::Rectangle_NGSN>)
                std::cout << "rectangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Triangle_NGSN>)
                std::cout << "triangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Circle_NGSN>)
                std::cout << "circle area: " << shape.area() << '\n';
        };
        for (const auto& shape : shapes)
        {
            std::visit(visitor, shape);
        }*/

        std::string output;
        encode_json(shapes, output, indenting::indent);
        //std::cout << output << "\n";

    }
#endif
} 

TEST_CASE("JSONCONS_ALL_MEMBER_NAME_TRAITS polymorphic and variant tests")
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 3.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    SECTION("polymorphic test")
    {
        auto shapes = decode_json<std::vector<std::unique_ptr<ns::Shape_AMN>>>(input);
        REQUIRE(shapes.size() == 3);
        std::string output;

        encode_json(shapes, output, indenting::indent);

        auto j = decode_json<json>(input);
        REQUIRE((j.is_array() && j.size() == 3));
        CHECK(j[0].is<ns::Rectangle_AMN>());
        CHECK_FALSE(j[0].is<ns::Triangle_AMN>());
        CHECK_FALSE(j[0].is<ns::Circle_AMN>());
        CHECK(j[1].is<ns::Triangle_AMN>());
        CHECK_FALSE(j[1].is<ns::Rectangle_AMN>());
        CHECK_FALSE(j[1].is<ns::Circle_AMN>());
        CHECK(j[2].is<ns::Circle_AMN>());
        CHECK_FALSE(j[2].is<ns::Rectangle_AMN>());
        CHECK_FALSE(j[2].is<ns::Triangle_AMN>());

        auto j2 = decode_json<json>(output);
        CHECK(j2 == j);
    }

#if defined(JSONCONS_HAS_STD_VARIANT)
    SECTION("variant test")
    {
        using shapes_t = std::variant<ns::Rectangle_AMN,ns::Triangle_AMN,ns::Circle_AMN>;
        auto shapes = decode_json<std::vector<shapes_t>>(input);
        REQUIRE(shapes.size() == 3);

        /*auto visitor = [](auto&& shape) {
            using T = std::decay_t<decltype(shape)>;
            if constexpr (std::is_same_v<T, ns::Rectangle_AMN>)
                std::cout << "rectangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Triangle_AMN>)
                std::cout << "triangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Circle_AMN>)
                std::cout << "circle area: " << shape.area() << '\n';
        };
        for (const auto& shape : shapes)
        {
            std::visit(visitor, shape);
        }*/

        std::string output;
        encode_json(shapes, output, indenting::indent);
        //std::cout << output << "\n";

    }
#endif
} 

TEST_CASE("JSONCONS_N_MEMBER_NAME_TRAITS polymorphic and variant tests")
{
    std::string input = R"(
[
    {"type" : "rectangle", "width" : 2.0, "height" : 1.5 },
    {"type" : "triangle", "width" : 3.0, "height" : 2.0 },
    {"type" : "circle", "radius" : 1.0 }
]
    )";

    SECTION("polymorphic test")
    {
        auto shapes = decode_json<std::vector<std::unique_ptr<ns::Shape_NMN>>>(input);
        REQUIRE(shapes.size() == 3);
        std::string output;

        encode_json(shapes, output, indenting::indent);

        auto j = decode_json<json>(input);
        REQUIRE((j.is_array() && j.size() == 3));
        CHECK(j[0].is<ns::Rectangle_NMN>());
        CHECK_FALSE(j[0].is<ns::Triangle_NMN>());
        CHECK_FALSE(j[0].is<ns::Circle_NMN>());
        CHECK(j[1].is<ns::Triangle_NMN>());
        CHECK_FALSE(j[1].is<ns::Rectangle_NMN>());
        CHECK_FALSE(j[1].is<ns::Circle_NMN>());
        CHECK(j[2].is<ns::Circle_NMN>());
        CHECK_FALSE(j[2].is<ns::Rectangle_NMN>());
        CHECK_FALSE(j[2].is<ns::Triangle_NMN>());

        auto j2 = decode_json<json>(output);
        CHECK(j2 == j);
    }

#if defined(JSONCONS_HAS_STD_VARIANT)
    SECTION("variant test")
    {
        using shapes_t = std::variant<ns::Rectangle_NMN,ns::Triangle_NMN,ns::Circle_NMN>;
        auto shapes = decode_json<std::vector<shapes_t>>(input);
        REQUIRE(shapes.size() == 3);

        /*auto visitor = [](auto&& shape) {
            using T = std::decay_t<decltype(shape)>;
            if constexpr (std::is_same_v<T, ns::Rectangle_NMN>)
                std::cout << "rectangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Triangle_NMN>)
                std::cout << "triangle area: " << shape.area() << '\n';
            else if constexpr (std::is_same_v<T, ns::Circle_NMN>)
                std::cout << "circle area: " << shape.area() << '\n';
        };
        for (const auto& shape : shapes)
        {
            std::visit(visitor, shape);
        }*/

        std::string output;
        encode_json(shapes, output, indenting::indent);
        //std::cout << output << "\n";

    }
#endif
} 

