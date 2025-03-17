// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_COMPILATION_CONTEXT_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_COMPILATION_CONTEXT_HPP

#include <random>
#include <vector>
#include <unordered_map>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/uri.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/common/uri_wrapper.hpp>
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator.hpp>

namespace jsoncons {
namespace jsonschema {

    template <typename Json>
    class compilation_context
    {
        using anchor_uri_map_type = std::unordered_map<std::string,uri_wrapper>;
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;

        uri_wrapper base_uri_;
        std::vector<uri_wrapper> uris_;
        jsoncons::optional<uri> id_;
        std::unordered_map<std::string,std::string> custom_messages_;
        std::string custom_message_;
    public:

        explicit compilation_context(const uri_wrapper& retrieval_uri,
            const std::unordered_map<std::string,std::string>& custom_messages = std::unordered_map<std::string,std::string>{})
            : base_uri_(retrieval_uri), 
              uris_(std::vector<uri_wrapper>{{retrieval_uri}}),
              custom_messages_{custom_messages}
        {
        }

        explicit compilation_context(const std::vector<uri_wrapper>& uris,
            const std::unordered_map<std::string,std::string>& custom_messages = std::unordered_map<std::string,std::string>{})
            : uris_(uris),
              custom_messages_{custom_messages}
        {
            if (uris_.empty())
            {
                uris_.push_back(uri_wrapper{"#"});
            }
            base_uri_ = uris_.back();
        }

        explicit compilation_context(const std::vector<uri_wrapper>& uris, const jsoncons::optional<uri>& id,
            const std::unordered_map<std::string,std::string>& custom_messages, const std::string& custom_message)
            : uris_(uris), 
              id_(id),
              custom_messages_{custom_messages},
              custom_message_(custom_message)
        {
            if (uris_.empty())
            {
                uris_.push_back(uri_wrapper{"#"});
            }
            base_uri_ = uris_.back();
        }
        
        std::string get_custom_message(const std::string& message_key) const
        {
            if (!custom_message_.empty())
            {
                return custom_message_;
            }
            auto it = custom_messages_.find(message_key);
            return it == custom_messages_.end() ? std::string{} : it->second;
        }
        
        const std::unordered_map<std::string,std::string>& custom_messages() const
        {
            return custom_messages_;
        }

        const std::vector<uri_wrapper>& uris() const {return uris_;}
        
        const jsoncons::optional<uri>& id() const
        {
            return id_;
        }

        uri get_base_uri() const
        {
            return base_uri_.uri();
        }

        jsoncons::uri make_schema_location(const std::string& keyword) const
        {
            for (auto it = uris_.rbegin(); it != uris_.rend(); ++it)
            {
                if (!(*it).has_plain_name_fragment())
                {
                    return (*it).append(keyword).uri();
                }
            }
            return uri{"#"};
        }
        
        static jsoncons::uri make_random_uri()
        {
            std::random_device dev;
            std::mt19937 gen{ dev() };
            std::uniform_int_distribution<std::mt19937::result_type> dist(1, 10000);

            std::string str = "https:://jsoncons.com/" + std::to_string(dist(gen));
            jsoncons::uri uri{str};
            return uri;
        }
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_COMPILATION_CONTEXT_HPP
