/// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONPATCH_JSONPATCH_ERROR_HPP
#define JSONCONS_EXT_JSONPATCH_JSONPATCH_ERROR_HPP

#include <string>
#include <system_error>
#include <type_traits>

#include <jsoncons/json_exception.hpp>

namespace jsoncons { namespace jsonpatch {

    enum class jsonpatch_errc 
    {
        success = 0,
        invalid_patch = 1,
        test_failed,
        add_failed,
        remove_failed,
        replace_failed,
        move_failed,
        copy_failed

    };

    class jsonpatch_error_category_impl
       : public std::error_category
    {
    public:
        const char* name() const noexcept override
        {
            return "jsoncons/jsonpatch";
        }
        std::string message(int ev) const override
        {
            switch (static_cast<jsonpatch_errc>(ev))
            {
                case jsonpatch_errc::invalid_patch:
                    return "Invalid JSON Patch document";
                case jsonpatch_errc::test_failed:
                    return "JSON Patch test operation failed";
                case jsonpatch_errc::add_failed:
                    return "JSON Patch add operation failed";
                case jsonpatch_errc::remove_failed:
                    return "JSON Patch remove operation failed";
                case jsonpatch_errc::replace_failed:
                    return "JSON Patch replace operation failed";
                case jsonpatch_errc::move_failed:
                    return "JSON Patch move operation failed";
                case jsonpatch_errc::copy_failed:
                    return "JSON Patch copy operation failed";
                default:
                    return "Unknown JSON Patch error";
            }
        }
    };

    inline
    const std::error_category& jsonpatch_error_category()
    {
      static jsonpatch_error_category_impl instance;
      return instance;
    }

    inline 
    std::error_code make_error_code(jsonpatch_errc result)
    {
        return std::error_code(static_cast<int>(result),jsonpatch_error_category());
    }

} // namespace jsonpatch
} // namespace jsoncons

namespace std {
    template<>
    struct is_error_code_enum<jsoncons::jsonpatch::jsonpatch_errc> : public true_type
    {
    };
} // namespace std

namespace jsoncons { namespace jsonpatch {

// allow to disable exceptions
#if !defined(JSONCONS_NO_EXCEPTIONS)
    #define JSONCONS_THROW(exception) throw exception
    #define JSONCONS_RETHROW throw
    #define JSONCONS_TRY try
    #define JSONCONS_CATCH(exception) catch(exception)
#else
    #define JSONCONS_THROW(exception) std::terminate()
    #define JSONCONS_RETHROW std::terminate()
    #define JSONCONS_TRY if (true)
    #define JSONCONS_CATCH(exception) if (false)
#endif

    class jsonpatch_error : public std::system_error, public virtual json_exception
    {
    public:
        jsonpatch_error(const std::error_code& ec)
            : std::system_error(ec)
        {
        }

        jsonpatch_error(const jsonpatch_error& other) = default;

        jsonpatch_error(jsonpatch_error&& other) = default;

        jsonpatch_error& operator=(const jsonpatch_error& e) = default;
        jsonpatch_error& operator=(jsonpatch_error&& e) = default;

        const char* what() const noexcept override
        {
            return std::system_error::what();
        }
    };
    
} // namespace jsonpatch
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONPATCH_JSONPATCH_ERROR_HPP
