// Copyright 2013-2025 Daniel Parker
// Distributed under Boost license

#include <jsoncons_ext/csv/csv.hpp>
#include <jsoncons/json.hpp>

#include <sstream>
#include <vector>
#include <utility>
#include <ctime>
#include <cassert>
#include <boost/date_time/gregorian/gregorian.hpp>
#include <boost/numeric/ublas/matrix.hpp>
#include <boost/multiprecision/cpp_dec_float.hpp>

namespace jsoncons {

    template <typename Json>
    struct json_type_traits<Json,boost::gregorian::date>
    {
        static bool is(const Json& val) noexcept
        {
            if (!val.is_string())
                return false;
            try
            {
                std::string s = val.template as<std::string>();
                boost::gregorian::from_simple_string(s);
                return true;
            }
            catch (...)
            {
                return false;
            }
        }

        static boost::gregorian::date as(const Json& val)
        {
            std::string s = val.template as<std::string>();
            return boost::gregorian::from_simple_string(s);
        }

        static Json to_json(boost::gregorian::date val, 
                            typename Json::allocator_type alloc = Json::allocator_type())
        {
            return Json(to_iso_extended_string(val), alloc);
        }
    };

    template <typename Json,typename Backend>
    struct json_type_traits<Json,boost::multiprecision::number<Backend>>
    {
        using multiprecision_type = boost::multiprecision::number<Backend>;

        static bool is(const Json& val) noexcept
        {
            if (!(val.is_string() && val.tag() == semantic_tag::bigdec))
            {
                return false;
            }
            else
            {
                return true;
            }
        }

        static multiprecision_type as(const Json& val)
        {
            return multiprecision_type(val.template as<std::string>());
        }

        static Json to_json(multiprecision_type val)
        {
            return Json(val.str(), semantic_tag::bigdec);
        }
    };

    template <typename Json,typename T>
    struct json_type_traits<Json,boost::numeric::ublas::matrix<T>>
    {
        using allocator_type = typename Json::allocator_type;

        static bool is(const Json& val) noexcept
        {
            if (!val.is_array())
            {
                return false;
            }
            if (val.size() > 0)
            {
                std::size_t n = val[0].size();
                for (const auto& a: val.array_range())
                {
                    if (!(a.is_array() && a.size() == n))
                    {
                        return false;
                    }
                    for (auto x: a.array_range())
                    {
                        if (!x.template is<T>())
                        {
                            return false;
                        }
                    }
                }
            }
            return true;
        }

        static boost::numeric::ublas::matrix<T> as(const Json& val)
        {
            if (val.is_array() && val.size() > 0)
            {
                std::size_t m = val.size();
                std::size_t n = 0;
                for (const auto& a : val.array_range())
                {
                    if (a.size() > n)
                    {
                        n = a.size();
                    }
                }

                boost::numeric::ublas::matrix<T> A(m,n,T());
                for (std::size_t i = 0; i < m; ++i)
                {
                    const auto& a = val[i];
                    for (std::size_t j = 0; j < a.size(); ++j)
                    {
                        A(i,j) = a[j].template as<T>();
                    }
                }
                return A;
            }
            else
            {
                boost::numeric::ublas::matrix<T> A;
                return A;
            }
        }

        static Json to_json(const boost::numeric::ublas::matrix<T>& val,
                            allocator_type alloc = allocator_type())
        {
            Json a = Json::template make_array<2>(val.size1(), val.size2(), T());
            for (std::size_t i = 0; i < val.size1(); ++i)
            {
                for (std::size_t j = 0; j < val.size1(); ++j)
                {
                    a[i][j] = val(i,j);
                }
            }
            return a;
        }
    };
} // namespace jsoncons

namespace ns {
    class fixing
    {
        std::string index_id_;
        boost::gregorian::date observation_date_;
        double rate_;
    public:
        fixing(const std::string& index_id, boost::gregorian::date observation_date, double rate)
            : index_id_(index_id), observation_date_(observation_date), rate_(rate)
        {
        }

        const std::string& index_id() const {return  index_id_;}

        boost::gregorian::date observation_date() const {return  observation_date_;}

        double rate() const {return rate_;}
    };
} namespace ns

JSONCONS_ALL_CTOR_GETTER_TRAITS(ns::fixing, index_id, observation_date, rate)

using namespace jsoncons;
using boost::numeric::ublas::matrix;

void boost_date_conversions()
{
    using boost::gregorian::date;

    json deal;
    deal["maturity"] = date(2014,10,14);

    json observation_dates(json_array_arg);
    observation_dates.push_back(date(2014,2,14));
    observation_dates.push_back(date(2014,2,21));

    deal["observationDates"] = std::move(observation_dates);

    date maturity = deal["maturity"].as<date>();

    assert(deal["maturity"].as<date>() == date(2014,10,14));
    assert(deal["observationDates"].is_array());
    assert(deal["observationDates"].size() == 2);
    assert(deal["observationDates"][0].as<date>() == date(2014,2,14));
    assert(deal["observationDates"][1].as<date>() == date(2014,2,21));

    std::cout << pretty_print(deal) << '\n';
}

void boost_matrix_conversions()
{
    matrix<double> A(2, 2);
    A(0, 0) = 1.1;
    A(0, 1) = 2.1;
    A(1, 0) = 3.1;
    A(1, 1) = 4.1;

    json a = A;

    assert(a.is<matrix<double>>());
    assert(!a.is<matrix<int>>());

    assert(a[0][0].as<double>()==A(0,0));
    assert(a[0][1].as<double>()==A(0,1));
    assert(a[1][0].as<double>()==A(1,0));
    assert(a[1][1].as<double>()==A(1,1));

    matrix<double> B = a.as<matrix<double>>();

    assert(B.size1() ==a.size());
    assert(B.size2() ==a[0].size());

    assert(a[0][0].as<double>()==B(0,0));
    assert(a[0][1].as<double>()==B(0,1));
    assert(a[1][0].as<double>()==B(1,0));
    assert(a[1][1].as<double>()==B(1,1));
}


void boost_multiprecison_conversions()
{
    typedef boost::multiprecision::number<boost::multiprecision::cpp_dec_float_50> multiprecision_type;

    std::string s = "[100000000000000000000000000000000.1234]";
    auto options = json_options{}
        .lossless_number(true);
    json j = json::parse(s, options);

    multiprecision_type x = j[0].as<multiprecision_type>();

    std::cout << "(1) " << std::setprecision(std::numeric_limits<multiprecision_type>::max_digits10)
        << x << "\n";

    json j2(json_array_arg,{x});
    std::cout << "(2) " << j2[0].as<std::string>() << "\n";
}

void csv_strongly_typed_example()
{
    const std::string data = R"(index_id,observation_date,rate
EUR_LIBOR_06M,2015-10-23,0.0000214
EUR_LIBOR_06M,2015-10-26,0.0000143
EUR_LIBOR_06M,2015-10-27,0.0000001
)";

    auto options = csv::csv_options{}
        .assume_header(true);

    // Decode the CSV data into a c++ structure
    std::vector<ns::fixing> v = csv::decode_csv<std::vector<ns::fixing>>(data, options);

    // Iterate over values
    std::cout << std::fixed << std::setprecision(7);
    std::cout << "(1)\n";
    for (const auto& item : v)
    {
        std::cout << item.index_id() << ", " << item.observation_date() << ", " << item.rate() << "\n";
    }

    // Encode the c++ structure into CSV data
    std::string s;
    csv::encode_csv(v, s);
    std::cout << "(2)\n";
    std::cout << s << "\n";

    //std::vector<json> v = csv::decode_csv<std::vector<json>>(data, options);
    //std::cout << v[0]["observation_date"].as<boost::gregorian::date>() << "\n";
}

void extensibility_examples()
{
    std::cout << "extensibility examples\n\n";

    boost_date_conversions();
    boost_matrix_conversions();
    boost_multiprecison_conversions();
    csv_strongly_typed_example();
}
