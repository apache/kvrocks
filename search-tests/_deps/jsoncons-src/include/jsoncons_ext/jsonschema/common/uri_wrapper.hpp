// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_URI_WRAPPER_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_URI_WRAPPER_HPP

#include <cstddef>
#include <memory>
#include <string>
#include <system_error>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/uri.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>

namespace jsoncons {
namespace jsonschema {

    class uri_wrapper
    {
        jsoncons::uri uri_;
        std::string identifier_;
        bool has_plain_name_fragment_;
    public:
        uri_wrapper()
            : has_plain_name_fragment_(false)
        {
        }

        explicit uri_wrapper(const std::string& uri)
        {
            uri_ = jsoncons::uri(uri);
            if (!uri_.encoded_fragment().empty())
            {
                identifier_ = uri_.fragment();
                std::error_code ec;
                jsonpointer::json_pointer::parse(identifier_, ec);
                has_plain_name_fragment_ = ec ? true : false;
            }
            else
            {
                has_plain_name_fragment_ = false;
            }
        }

        explicit uri_wrapper(const uri& uri)
            : uri_{uri}
        {
            uri_ = jsoncons::uri(uri);
            if (!uri_.encoded_fragment().empty())
            {
                identifier_ = uri_.fragment();
                std::error_code ec;
                jsonpointer::json_pointer::parse(identifier_, ec);
                has_plain_name_fragment_ = ec ? true : false;
            }
            else
            {
                has_plain_name_fragment_ = false;
            }
        }

        const jsoncons::uri& uri() const
        {
            return uri_;
        }

        bool has_fragment() const
        {
            return !uri_.encoded_fragment().empty();
        }

        bool has_plain_name_fragment() const
        {
            return has_plain_name_fragment_;
        }

        jsoncons::uri base() const
        {
            return uri_.base();
        }

        std::string path() const
        {
            return uri_.path();
        }

        bool is_absolute() const
        {
            return uri_.is_absolute();
        }

        std::string fragment() const
        {
            return identifier_;
        }

        int compare(const uri_wrapper& other) const
        {
            int result = uri_.compare(other.uri_);
            if (result != 0) 
            {
                return result;
            }
            return result; 
        }

        uri_wrapper append(const std::string& field) const
        {
            if (has_plain_name_fragment())
                return *this;

            jsoncons::jsonpointer::json_pointer pointer(std::string(uri_.fragment()));
            pointer /= field;

            jsoncons::uri new_uri(uri_, uri_fragment_part, pointer.to_string());

            return uri_wrapper(std::move(new_uri));
        }

        uri_wrapper append(std::size_t index) const
        {
            if (has_plain_name_fragment())
                return *this;

            jsoncons::jsonpointer::json_pointer pointer(std::string(uri_.encoded_fragment()));
            pointer /= index;

            jsoncons::uri new_uri(uri_, uri_fragment_part, pointer.to_string());

            return uri_wrapper(std::move(new_uri));
        }

        std::string string() const
        {
            std::string s = uri_.string();
            return s;
        }

        friend bool operator==(const uri_wrapper& lhs, const uri_wrapper& rhs)
        {
            return lhs.compare(rhs) == 0;
        }

        friend bool operator!=(const uri_wrapper& lhs, const uri_wrapper& rhs)
        {
            return lhs.compare(rhs) != 0;
        }

        friend bool operator<(const uri_wrapper& lhs, const uri_wrapper& rhs)
        {
            return lhs.compare(rhs) < 0;
        }

        friend bool operator<=(const uri_wrapper& lhs, const uri_wrapper& rhs)
        {
            return lhs.compare(rhs) <= 0;
        }

        friend bool operator>(const uri_wrapper& lhs, const uri_wrapper& rhs)
        {
            return lhs.compare(rhs) > 0;
        }

        friend bool operator>=(const uri_wrapper& lhs, const uri_wrapper& rhs)
        {
            return lhs.compare(rhs) >= 0;
        }
    private:
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_COMMON_URI_WRAPPER_HPP
