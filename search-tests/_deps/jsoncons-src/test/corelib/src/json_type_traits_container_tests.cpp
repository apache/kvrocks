// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons/json.hpp>
#include <jsoncons/json_encoder.hpp>
#include <array>
#include <cstdint>
#include <ctime>
#include <deque>
#include <forward_list>
#include <list>
#include <map>
#include <set>
#include <sstream>
#include <unordered_map>
#include <unordered_set>
#include <utility>
#include <valarray>
#include <vector>
#include <catch/catch.hpp>

using namespace jsoncons;

TEST_CASE("test_json_as_pair")
{
    json j(json_array_arg, {false,1});
    auto t = j.as<std::pair<bool,int>>();
    CHECK(std::get<0>(t) == false);
    CHECK(std::get<1>(t) == 1);
}

TEST_CASE("test_tuple_to_json")
{
    auto t = std::make_tuple(false,1,"foo");
    json j(t);

    REQUIRE(j.is_array());
    REQUIRE(j.size() == 3);
    CHECK(false == j[0].as<bool>());
    CHECK(1 == j[1].as<int>());
    CHECK(std::string("foo") == j[2].as<std::string>());
}

TEST_CASE("test_json_as_tuple")
{
    json j(json_array_arg, {false,1,"foo"});

    auto t = j.as<std::tuple<bool,int,std::string>>();

    CHECK(std::get<0>(t) == false);
    CHECK(std::get<1>(t) == 1);
    CHECK(std::get<2>(t) == std::string("foo"));
}

TEST_CASE("test_characters")
{
    const json a = "short";
    const json b = "a long string";
 
    CHECK(true == a.is<std::string>());
    CHECK(true == b.is<std::string>());

    std::string s2 = a.as<std::string>();
    std::string t2 = b.as<std::string>();

    json c(json_array_arg, { "short","a long string" });
    auto u = c.as<std::vector<std::string>>();
}

// vector

TEST_CASE("test_is_json_vector")
{
    json a(json_array_arg, {0,1,2,3,4}); 

    CHECK(true == a.is<std::vector<uint64_t>>());
}

TEST_CASE("test_as_vector")
{
    json a(json_array_arg, {0,1,2,3,4}); 
    std::vector<int> v = a.as<std::vector<int>>();

    CHECK(v[0] == 0);
    CHECK(v[1] == 1);
    CHECK(v[2] == 2);
    CHECK(v[3] == 3);
    CHECK(v[4] == 4);
}

TEST_CASE("test_assign_vector")
{
    std::vector<int> v {0,1,2,3,4}; 
    json a = v;

    CHECK(a[0] == json(0));
    CHECK(a[1] == json(1));
    CHECK(a[2] == json(2));
    CHECK(a[3] == json(3));
    CHECK(a[4] == json(4));
}

TEST_CASE("test_as_vector_of_bool")
{
    json a = json::parse("[true,false,true]");

    std::vector<bool> v = a.as<std::vector<bool>>();

    CHECK(v[0] == true);
    CHECK(v[1] == false);
    CHECK(v[2] == true);
}

TEST_CASE("test_assign_vector_of_bool")
{
    std::vector<bool> v = {true,false,true};
    json a = v;

    CHECK(a[0] == json(true));
    CHECK(a[1] == json(false));
    CHECK(a[2] == json(true));

    json b;
    b = v;

    CHECK(b[0] == json(true));
    CHECK(b[1] == json(false));
    CHECK(b[2] == json(true));
}

TEST_CASE("test_construct_vector_of_bool")
{
    std::vector<bool> v = {true,false,true};
    json a = v;

    CHECK(a[0] == json(true));
    CHECK(a[1] == json(false));
    CHECK(a[2] == json(true));
}

TEST_CASE("test_construct_const_vector_of_bool")
{
    const std::vector<bool> v = {true,false,true};
    json a = v;

    CHECK(a[0] == json(true));
    CHECK(a[1] == json(false));
    CHECK(a[2] == json(true));
}

// valarray

TEST_CASE("test_is_json_valarray")
{
    json a(json_array_arg, {0,1,2,3,4}); 

    CHECK(true == a.is<std::valarray<uint64_t>>());
}

TEST_CASE("test_as_valarray")
{
    json a(json_array_arg, {0,1,2,3,4}); 
    std::valarray<int> v = a.as<std::valarray<int>>();

    CHECK(v[0] == 0);
    CHECK(v[1] == 1);
    CHECK(v[2] == 2);
    CHECK(v[3] == 3);
    CHECK(v[4] == 4);
}

TEST_CASE("test_assign_valarray")
{
    std::valarray<int> v {0,1,2,3,4}; 
    json a = v;

    CHECK(a[0] == json(0));
    CHECK(a[1] == json(1));
    CHECK(a[2] == json(2));
    CHECK(a[3] == json(3));
    CHECK(a[4] == json(4));
}

TEST_CASE("test_is_json_map")
{
    json a;
    a["a"] = 0; 
    a["b"] = 1; 
    a["c"] = 2; 

    CHECK(true == (a.is<std::map<std::string,int> >()));
}

TEST_CASE("test_is_json_map2")
{
    json a;
    a["a"] = "0"; 
    a["b"] = "1"; 
    a["c"] = "2"; 

    CHECK(true == (a["a"].is_string()));

    json b("0");
    CHECK(true == (b.is<std::string>()));

    CHECK(true == (a["a"].is<std::string>()));

    CHECK(true == (a.is<std::map<std::string,std::string> >()));
}

TEST_CASE("test_as_map")
{
    json o;
    o["first"] = "first";
    o["second"] = "second";

    auto m = o.as<std::map<std::string,std::string>>();
    CHECK(std::string("first") == m.at("first"));
    CHECK(std::string("second") == m.at("second"));

    json o2(m);
    CHECK(o == o2);

    json o3;
    o3 = m;
    CHECK(o == o3);
}

TEST_CASE("test_as_map2")
{
    json o;
    o["first"] = 1;
    o["second"] = true;
    o["third"] = jsoncons::null_type();

    auto m = o.as<std::map<std::string,std::string>>();
    CHECK(std::string("1") == m.at("first"));
    CHECK(std::string("true") == m.at("second"));
    CHECK(std::string("null") == m.at("third"));

    json o2(m);
    CHECK(std::string("1") == o2["first"].as<std::string>());
}

TEST_CASE("byte string container")
{
    SECTION("as<std::vector>")
    {
        std::vector<uint8_t> bytes = {'H', 'e', 'l', 'l', 'o'};
        json j(byte_string_arg, bytes);

        auto v = j.as<std::vector<uint8_t>>();

        json other(byte_string_arg, v);
    }
}

TEST_CASE("test_from_stl_container")
{
    std::vector<int> a_vector{1, 2, 3, 4};
    json j_vector(a_vector);
    CHECK((1 == j_vector[0].as<int>()));
    CHECK(2 == j_vector[1].as<int>());
    CHECK(3 == j_vector[2].as<int>());
    CHECK(4 == j_vector[3].as<int>());

    std::vector<unsigned long> a_vector2{1ul, 2ul, 3ul, 4ul};
    json j_vec2(a_vector2);
    CHECK(1 == j_vec2[0].as<int>());
    CHECK(2 == j_vec2[1].as<int>());
    CHECK(3 == j_vec2[2].as<int>());
    CHECK(4 == j_vec2[3].as<int>());

    std::deque<double> a_deque{1.123, 2.234, 3.456, 4.567};
    json j_deque(a_deque);
    CHECK(1.123 == j_deque[0].as<double>());
    CHECK(2.234 == j_deque[1].as<double>());
    CHECK(3.456 == j_deque[2].as<double>());
    CHECK(4.567 == j_deque[3].as<double>());

    std::list<bool> a_list{true, true, false, true};
    json j_list(a_list);
    CHECK(true == j_list[0].as<bool>());
    CHECK(true == j_list[1].as<bool>());
    CHECK(false == j_list[2].as<bool>());
    CHECK(true == j_list[3].as<bool>());

    std::forward_list<int64_t>a_flist {12345678909876, 23456789098765, 34567890987654, 45678909876543};
    json j_flist(a_flist);
    CHECK(static_cast<int64_t>(12345678909876) == j_flist[0].as<int64_t>());
    CHECK(static_cast<int64_t>(23456789098765) == j_flist[1].as<int64_t>());
    CHECK(static_cast<int64_t>(34567890987654) == j_flist[2].as<int64_t>());
    CHECK(static_cast<int64_t>(45678909876543) == j_flist[3].as<int64_t>());

    std::array<unsigned long, 4> a_array {{1, 2, 3, 4}};
    json j_array(a_array);
    CHECK(1 == j_array[0].as<int>());
    CHECK(2 == j_array[1].as<int>());
    CHECK(3 == j_array[2].as<int>());
    CHECK(4 == j_array[3].as<int>());

    std::set<std::string> a_set{"one", "two", "three", "four"};
    json j_set(a_set); 
    // ["four", "one", "three", "two"]

    std::unordered_set<std::string> a_uset{"one", "two", "three", "four"};
    json j_uset(a_uset); 
    // maybe ["two", "three", "four", "one"]

    std::multiset<std::string> a_mset{"one", "two", "one", "four"};
    json j_mset(a_mset); // only one entry for "one" is used
    // maybe ["one", "two", "four"]

    std::unordered_multiset<std::string> a_umset {"one", "two", "one", "four"};
    json j_umset(a_umset); // both entries for "one" are used
    // maybe ["one", "two", "one", "four"]

    std::map<std::string, int> a_map{ {"one", 1}, {"two", 2}, {"three", 3} };
    json j_map(a_map);
    CHECK(1 == j_map["one"].as<int>());
    CHECK(2 == j_map["two"].as<int>());
    CHECK(3 == j_map["three"].as<int>());

    std::unordered_map<std::string, double> a_umap{ {"one", 1.2}, {"two", 2.3}, {"three", 3.4} };
    json j_umap(a_umap);
    CHECK(1.2 == j_umap["one"].as<double>());
    CHECK(2.3 == j_umap["two"].as<double>());
    CHECK(3.4 == j_umap["three"].as<double>());

    std::multimap<std::string, bool> a_mmap{ {"one", true}, {"two", true}, {"three", false}, {"three", true} };
    json j_mmap(a_mmap); // one entry for key "three"
    CHECK(true == j_mmap.find("one")->value().as<bool>());
    CHECK(true == j_mmap.find("two")->value().as<bool>());
    CHECK(false == j_mmap.find("three")->value().as<bool>());

    std::unordered_multimap<std::string, bool> a_ummap { {"one", true}, {"two", true}, /*{"three", false},*/ {"three", true} };
    json j_ummap(a_ummap); // two entries for key "three"
    CHECK(true == j_ummap.find("one")->value().as<bool>());
    CHECK(true == j_ummap.find("two")->value().as<bool>());
    CHECK(true == j_ummap.find("three")->value().as<bool>());
}

//own vector will always be of an even length 
struct own_vector : std::vector<int64_t> { using  std::vector<int64_t>::vector; };

namespace jsoncons {
template <typename Json>
struct json_type_traits<Json, own_vector> {
    static bool is(const Json& j) noexcept
    { 
        return j.is_object() && j.size() % 2 == 0;
    }
    static own_vector as(const Json& j)
    {   
        own_vector v;
        for (auto& item : j.object_range())
        {
            std::string s(item.key());
            v.push_back(std::strtol(s.c_str(),nullptr,10));
            v.push_back(item.value().template as<int64_t>());
        }
        return v;
    }
    static Json to_json(const own_vector& val){
        Json j;
        for (std::size_t i=0;i<val.size();i+=2){
            j[std::to_string(val[i])] = val[i + 1];
        }
        return j;
    }
};

template <> 
struct is_json_type_traits_declared<own_vector> : public std::true_type 
{}; 
} // namespace jsoncons

TEST_CASE("own_vector json_type_traits")
{
    json j(json_object_arg,{ {"1",2},{"3",4} });
    REQUIRE(j.is<own_vector>());
    auto v = j.as<own_vector>();
    REQUIRE(v.size() == 4);
    json j2 = v;
    CHECK(j2 == j);
}

TEST_CASE("map with integer key")
{
    SECTION("test 1")
    {
        std::map<uint32_t, int> val{ {1,1,},{2,2} };

        json j{ val };
        //CHECK(j.is<std::map<uint32_t, int>>());
        //CHECK(j.is<std::map<uint64_t, int>>());
        CHECK_FALSE(!j.is<std::map<std::string, int>>());

        REQUIRE(j.is_object());
        REQUIRE(j.size() == 2);
        CHECK(j["1"] == 1);
        CHECK(j["2"] == 2);

        std::map<uint64_t, int> other = j.as<std::map<uint64_t, int>>();
        REQUIRE(other.size() == 2);
        CHECK(other[1] == 1);
        CHECK(other[2] == 2);

        json other_j{ other };
        REQUIRE(other_j.is_object());
        REQUIRE(other_j.size() == 2);
        CHECK(other_j["1"] == 1);
        CHECK(other_j["2"] == 2);
    }
}

namespace {
namespace ns {

    enum class MyCriterionType{
        First,
        Presentation = First,
        MessageType,
        Version,
        Release,
        Last
    };

    template <typename E>
    class TCriterion
    {
    public:
        TCriterion() = default;
        TCriterion(E iType, std::string iVal) : _type(iType), _value(iVal) {}

        const std::string& getValue() const {
            return _value;
        }

        const E getType() const {
            return _type;
        }

    protected:
        JSONCONS_TYPE_TRAITS_FRIEND
        E _type;
        std::string _value;
    };

    struct EnumClassHash {
        template <typename T>
        std::size_t operator()(T t) const noexcept {
            return static_cast<std::size_t>(t);
        }
    };//To be able to use the enum class as key

    template <typename E>
    class TCriteria
    {
    public:
        TCriteria() = default;

        const std::unordered_map <E, TCriterion<E>>& getCriteriaMap() const {
            return _criteriaMap;
        }

    //protected:
        JSONCONS_TYPE_TRAITS_FRIEND
        std::unordered_map<E, TCriterion<E>, EnumClassHash> _criteriaMap;
    };

} // ns
} // namespace

JSONCONS_TPL_ALL_MEMBER_NAME_TRAITS(1,ns::TCriterion, (_type,"name"), (_value,"value"))
JSONCONS_TPL_ALL_MEMBER_NAME_TRAITS(1, ns::TCriteria, (_criteriaMap, "criteriaList"))
JSONCONS_ENUM_NAME_TRAITS(ns::MyCriterionType,(First,"first"),(MessageType,"message-type"),(Version,"version"),(Release,"release"),(Last,"last"))

TEST_CASE("map with enum key")
{
    SECTION("test 1")
    {
        ns::TCriterion<ns::MyCriterionType> criterion(ns::MyCriterionType::MessageType,"foo");

        std::string buffer;
        encode_json(criterion, buffer, indenting::indent);
        //std::cout << buffer << "\n";

        auto val = decode_json<ns::TCriterion<ns::MyCriterionType>>(buffer);

        CHECK(val.getValue() == std::string("foo"));
        CHECK(val.getType() == ns::MyCriterionType::MessageType);
    }
    SECTION("test 2")
    {
        ns::TCriterion<ns::MyCriterionType> criterion(ns::MyCriterionType::MessageType, "foo");
            
        ns::TCriteria<ns::MyCriterionType> criteria;
        criteria._criteriaMap.emplace(ns::MyCriterionType::MessageType, criterion);

        std::string buffer = R"(
{
    "criteriaList" : 
    {
        "message-type" : {
            "name": "message-type",
            "value": "foo"
        }
    }
}
        )";
        encode_json(criteria, buffer, indenting::indent);
        //std::cout << buffer << "\n";

        auto val = decode_json<ns::TCriteria<ns::MyCriterionType>>(buffer);

        auto x = val._criteriaMap.at(ns::MyCriterionType::MessageType);

        CHECK(x.getValue() == std::string("foo"));
        CHECK(x.getType() == ns::MyCriterionType::MessageType);
    }
}

namespace {
namespace ns {

    struct Value {
        std::string  name;
        std::int32_t value;

        friend bool operator==(const Value& lhs, const Value& rhs)
        {
            return lhs.name == rhs.name && lhs.value == rhs.value;
        }

        // Required for fedora24-x86_64 g++ 6.3.1
        friend bool operator!=(const Value& lhs, const Value& rhs)
        {
            return !(lhs == rhs);
        }
    };

    struct ValueHash {
        std::size_t operator()(const Value& val) const noexcept
        {
            std::size_t h1 = std::hash<std::string>{}(val.name);
            std::size_t h2 = std::hash<std::int32_t>{}(val.value);
            return h1 ^ (h2 << 1);
        }
    };

    inline bool operator<(const Value &l, const Value &r) noexcept {
      return l.name < r.name;
    }

    template <typename Container>
    struct Project {
        std::int32_t         version = 1;
        std::string          name;
        std::string          author;
        std::string          notes;
        bool                 showNotes;
        Container values;

        friend bool operator==(const Project& lhs, const Project& rhs)
        {
            return (lhs.version == rhs.version && 
                lhs.name == rhs.name &&
                lhs.author == rhs.author &&
                lhs.notes == rhs.notes &&
                lhs.showNotes == rhs.showNotes &&
                lhs.values == rhs.values);
        }
        friend bool operator!=(const Project& lhs, const Project& rhs)
        {
            return !(lhs == rhs);
        }
    };

} // namespace ns
} // namespace

JSONCONS_N_MEMBER_TRAITS(ns::Value, 0,
    name,
    value
)

JSONCONS_TPL_N_MEMBER_TRAITS(1, ns::Project, 0,
    version,
    name,
    author,
    notes,
    showNotes,
    values
)

TEST_CASE("encode/decode set traits")
{
    using project_type = ns::Project<std::set<ns::Value>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.insert({ "foo",1 });
    project.values.insert({ "bar",2 });
    project.values.insert({ "foo",1 });
    project.values.insert({ "baz",1 });

    SECTION("Value comparator tests")
    {
        ns::Value a{"foo", 1};
        ns::Value b{ "foo",2 };

        CHECK(a == a);
        CHECK(a != b);
    }

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
        CHECK_FALSE(project2 != project);
        CHECK_FALSE(project2.values != project.values);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}

TEST_CASE("encode/decode multiset traits")
{
    using project_type = ns::Project<std::multiset<ns::Value>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.insert({ "foo",1 });
    project.values.insert({ "bar",2 });
    project.values.insert({ "foo",1 });
    project.values.insert({ "baz",1 });

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}

TEST_CASE("encode/decode unordered_set traits")
{
    using project_type = ns::Project<std::unordered_set<ns::Value,ns::ValueHash>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.insert({ "foo",1 });
    project.values.insert({ "bar",2 });
    project.values.insert({ "foo",1 });
    project.values.insert({ "baz",1 });

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}

TEST_CASE("encode/decode unordered_multiset traits")
{
    using project_type = ns::Project<std::unordered_set<ns::Value,ns::ValueHash>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.insert({ "foo",1 });
    project.values.insert({ "bar",2 });
    project.values.insert({ "foo",1 });
    project.values.insert({ "baz",1 });

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}

TEST_CASE("encode/decode std::forward_list traits")
{
    using project_type = ns::Project<std::forward_list<ns::Value>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.push_front({ "foo",1 });
    project.values.push_front({ "bar",2 });
    project.values.push_front({ "foo",1 });
    project.values.push_front({ "baz",1 });

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}

TEST_CASE("encode/decode std::deque traits")
{
    using project_type = ns::Project<std::deque<ns::Value>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.push_back({ "foo",1 });
    project.values.push_back({ "bar",2 });
    project.values.push_back({ "foo",1 });
    project.values.push_back({ "baz",1 });

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}

TEST_CASE("encode/decode std::vector traits")
{
    using project_type = ns::Project<std::vector<ns::Value>>;

    project_type project;
    project.name = "xyz";
    project.author = "Jane Doe";
    project.showNotes = true;
    project.values.push_back({ "foo",1 });
    project.values.push_back({ "bar",2 });
    project.values.push_back({ "foo",1 });
    project.values.push_back({ "baz",1 });

    SECTION("encode/decode")
    {
        std::string s;
        encode_json(project, s, indenting::indent);

        auto project2 = decode_json<project_type>(s);

        CHECK(project2 == project);
    }

    SECTION("json_type_traits")
    {
        json j{project};

        auto project2 = j.as<project_type>();
        CHECK(project2 == project);
    }
}
