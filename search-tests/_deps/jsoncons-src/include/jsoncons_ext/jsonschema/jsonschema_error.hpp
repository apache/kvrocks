/// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version
 
#ifndef JSONCONS_EXT_JSONSCHEMA_JSONSCHEMA_ERROR_HPP
#define JSONCONS_EXT_JSONSCHEMA_JSONSCHEMA_ERROR_HPP

#include <string>
#include <stdexcept>
#include <system_error>

#include <jsoncons/json_exception.hpp>

namespace jsoncons {
namespace jsonschema {

    class schema_error : public std::runtime_error, public virtual json_exception
    {
    public:
        schema_error(const std::string& message)
            : std::runtime_error(message)
        {
        }

        const char* what() const noexcept override
        {
            return std::runtime_error::what();
        }  
    };

    class validation_error : public std::runtime_error, public virtual json_exception
    {
    public:
        validation_error(const std::string& message)
            : std::runtime_error(message)
        {
        }

        const char* what() const noexcept override
        {
            return std::runtime_error::what();
        }  
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_JSONSCHEMA_ERROR_HPP
