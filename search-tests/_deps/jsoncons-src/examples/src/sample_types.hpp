#ifndef EXAMPLE_TYPES
#define EXAMPLE_TYPES

#include <string>
#include <vector>
#include <jsoncons/json.hpp>

// book example

namespace ns {

    struct bond
    {
        double principal;
        std::string maturity;
        double coupon;
        std::string period;
    };

    struct employee
    {
        std::string employeeNo;
        std::string name;
        std::string title;
    };

    class fixing
    {
        std::string index_id_;
        std::string observation_date_;
        double rate_;
    public:
        fixing(const std::string& index_id, const std::string& observation_date, double rate)
            : index_id_(index_id), observation_date_(observation_date), rate_(rate)
        {
        }

        const std::string& index_id() const {return  index_id_;}

        const std::string& observation_date() const {return  observation_date_;}

        double rate() const {return rate_;}
    };

    struct book
    {
        std::string author;
        std::string title;
        double price;
    };

    class Person
    {
    public:
        Person(const std::string& name, const std::string& surname,
               const std::string& ssn, unsigned int age)
           : name(name), surname(surname), ssn(ssn), age(age) { }

        bool operator==(const Person& rhs) const
        {
            return name == rhs.name && surname == rhs.surname && ssn == rhs.ssn &&
                   age == rhs.age;
        }
        bool operator!=(const Person& rhs) const { return !(rhs == *this); }

    private:
        // Make json_type_traits specializations friends to give accesses to private members
        JSONCONS_TYPE_TRAITS_FRIEND

        Person() : age(0) {}

        std::string name;
        std::string surname;
        std::string ssn;
        unsigned int age;
    };

    enum class hiking_experience {beginner,intermediate,advanced};

    class hiking_reputon
    {
        std::string rater_;
        hiking_experience assertion_;
        std::string rated_;
        double rating_;
        jsoncons::optional<std::chrono::seconds> generated_; // use std::optional if C++17
        jsoncons::optional<std::chrono::seconds> expires_;
    public:
        hiking_reputon(const std::string& rater,
                       hiking_experience assertion,
                       const std::string& rated,
                       double rating,
                       const jsoncons::optional<std::chrono::seconds>& generated = jsoncons::optional<std::chrono::seconds>(),
                       const jsoncons::optional<std::chrono::seconds>& expires = jsoncons::optional<std::chrono::seconds>())
            : rater_(rater), assertion_(assertion), rated_(rated), rating_(rating),
              generated_(generated), expires_(expires)
        {
        }

        const std::string& rater() const {return rater_;}
        hiking_experience assertion() const {return assertion_;}
        const std::string& rated() const {return rated_;}
        double rating() const {return rating_;}
        jsoncons::optional<std::chrono::seconds> generated() const {return generated_;}
        jsoncons::optional<std::chrono::seconds> expires() const {return expires_;}

        friend bool operator==(const hiking_reputon& lhs, const hiking_reputon& rhs)
        {
            return lhs.rater_ == rhs.rater_ && lhs.assertion_ == rhs.assertion_ && 
                   lhs.rated_ == rhs.rated_ && lhs.rating_ == rhs.rating_ &&
                   lhs.generated_ == rhs.generated_ && lhs.expires_ == rhs.expires_;
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

        friend bool operator==(const hiking_reputation& lhs, const hiking_reputation& rhs)
        {
            return (lhs.application_ == rhs.application_) && (lhs.reputons_ == rhs.reputons_);
        }

        friend bool operator!=(const hiking_reputation& lhs, const hiking_reputation& rhs)
        {
            return !(lhs == rhs);
        };
    };

    template <typename T1,typename T2>
    struct TemplatedStruct
    {
          T1 aT1;
          T2 aT2;

          friend bool operator==(const TemplatedStruct& lhs, const TemplatedStruct& rhs)
          {
              return lhs.aT1 == rhs.aT1 && lhs.aT2 == rhs.aT2;  
          }

          friend bool operator!=(const TemplatedStruct& lhs, const TemplatedStruct& rhs)
          {
              return !(lhs == rhs);
          }
    };

} // namespace ns

namespace jsoncons {

    template <typename Json>
    struct json_type_traits<Json, ns::book>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& j) noexcept
        {
            return j.is_object() && j.contains("author") && 
                   j.contains("title") && j.contains("price");
        }
        static ns::book as(const Json& j)
        {
            ns::book val;
            val.author = j.at("author").template as<std::string>();
            val.title = j.at("title").template as<std::string>();
            val.price = j.at("price").template as<double>();
            return val;
        }
        static Json to_json(const ns::book& val, 
                            allocator_type alloc=allocator_type())
        {
            Json j(json_object_arg, semantic_tag::none, alloc);
            j.try_emplace("author", val.author);
            j.try_emplace("title", val.title);
            j.try_emplace("price", val.price);
            return j;
        }
    };
} // namespace jsoncons

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_ALL_MEMBER_NAME_TRAITS(ns::bond, (principal,"notional"), (maturity,"maturityDate"), (coupon,"couponRate"), (period,"frequency"))

JSONCONS_ENUM_TRAITS(ns::hiking_experience, beginner, intermediate, advanced)
// First four members listed are mandatory, generated and expires are optional
JSONCONS_N_CTOR_GETTER_TRAITS(ns::hiking_reputon, 4, rater, assertion, rated, rating, 
                              generated, expires)
// All members are mandatory
JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::hiking_reputation, application, reputons)

JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::fixing, index_id, observation_date, rate)
JSONCONS_ALL_MEMBER_TRAITS(ns::employee, employeeNo, name, title)

// Declare the traits. Specify which data members need to be serialized.
JSONCONS_ALL_MEMBER_TRAITS(ns::Person, name, surname, ssn, age)

JSONCONS_TPL_ALL_MEMBER_TRAITS(2,ns::TemplatedStruct,aT1,aT2)

#endif
