// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_EVALUATION_OPTIONS_HPP
#define JSONCONS_EXT_JSONSCHEMA_EVALUATION_OPTIONS_HPP

#include <string>

namespace jsoncons {
namespace jsonschema {

    struct schema_version
    {
        static std::string draft4() 
        {
            static std::string s{"http://json-schema.org/draft-04/schema#"};
            return s;
        }
        static std::string draft6() 
        {
            static std::string s{"http://json-schema.org/draft-06/schema#"};
            return s;
        }
        static std::string draft7() 
        {
            static std::string s{"http://json-schema.org/draft-07/schema#"};
            return s;
        }
        static std::string draft201909() 
        {
            static std::string s{"https://json-schema.org/draft/2019-09/schema"};
            return s;
        }
        static std::string draft202012() 
        {
            static std::string s{"https://json-schema.org/draft/2020-12/schema"};
            return s;
        }
    };

    class evaluation_options
    {
        std::string default_version_;
        bool require_format_validation_{false};
        bool compatibility_mode_{false};
        std::string default_base_uri_;
        bool enable_custom_error_message_{false};
    public:
        evaluation_options()
            : default_version_{schema_version::draft202012()}, 
              default_base_uri_("https://jsoncons.com")
        {
        }
            
        evaluation_options(const evaluation_options& other) = default;

        evaluation_options& operator=(const evaluation_options& other) = default;

        bool require_format_validation() const
        {
            return require_format_validation_;
        }
        evaluation_options& require_format_validation(bool value) 
        {
            require_format_validation_ = value;
            return *this;
        }

        bool compatibility_mode() const
        {
            return compatibility_mode_;
        }
        evaluation_options& compatibility_mode(bool value) 
        {
            compatibility_mode_ = value;
            return *this;
        }

        const std::string& default_version() const
        {
            return default_version_;
        }
        evaluation_options& default_version(const std::string& version) 
        {
            default_version_ = version;
            return *this;
        }

        const std::string& default_base_uri() const
        {
            return default_base_uri_;
        }
        evaluation_options& default_base_uri(const std::string& base_uri) 
        {
            default_base_uri_ = base_uri;
            return *this;
        }

        bool enable_custom_error_message() const
        {
            return enable_custom_error_message_;
        }
        evaluation_options& enable_custom_error_message(bool value) 
        {
            enable_custom_error_message_ = value;
            return *this;
        }

        friend bool operator==(const evaluation_options& lhs, const evaluation_options& rhs) 
        {
            return lhs.default_version_ == rhs.default_version_
                && lhs.require_format_validation_ == rhs.require_format_validation_
                && lhs.compatibility_mode_ == rhs.compatibility_mode_
                && lhs.default_base_uri_ == rhs.default_base_uri_
                && lhs.enable_custom_error_message_ == rhs.enable_custom_error_message_;
        }
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_COMMON_SCHEMA_HPP
