// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_EVAL_CONTEXT_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_EVAL_CONTEXT_HPP

#include <cstddef>
#include <string>
#include <vector>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/uri.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>

namespace jsoncons {
namespace jsonschema {

    template <typename Json>
    class schema_validator;
    
    enum class evaluation_flags : uint32_t {require_evaluated_properties=1, require_evaluated_items=2};

    inline evaluation_flags operator~(evaluation_flags a)
    {
        return static_cast<evaluation_flags>(~static_cast<unsigned int>(a));
    }

    inline evaluation_flags operator&(evaluation_flags a, evaluation_flags b)
    {
        return static_cast<evaluation_flags>(static_cast<unsigned int>(a) & static_cast<unsigned int>(b));
    }

    inline evaluation_flags operator^(evaluation_flags a, evaluation_flags b)
    {
        return static_cast<evaluation_flags>(static_cast<unsigned int>(a) ^ static_cast<unsigned int>(b));
    }

    inline evaluation_flags operator|(evaluation_flags a, evaluation_flags b)
    {
        return static_cast<evaluation_flags>(static_cast<unsigned int>(a) | static_cast<unsigned int>(b));
    }

    inline evaluation_flags operator&=(evaluation_flags& a, evaluation_flags b)
    {
        a = a & b;
        return a;
    }

    inline evaluation_flags operator^=(evaluation_flags& a, evaluation_flags b)
    {
        a = a ^ b;
        return a;
    }

    inline evaluation_flags operator|=(evaluation_flags& a, evaluation_flags b)
    {
        a = a | b;
        return a;
    }

    template <typename Json>
    class eval_context
    {
    private:
        std::vector<const schema_validator<Json>*> dynamic_scope_;
        jsonpointer::json_pointer eval_path_;
        evaluation_flags flags_;
    public:
        eval_context()
            : flags_{}
        {
        }

        eval_context(const eval_context& other)
            : dynamic_scope_ { other.dynamic_scope_}, eval_path_{other.eval_path_},
              flags_(other.flags_)
        {
        }

        eval_context(eval_context&& other) noexcept
            : dynamic_scope_{std::move(other.dynamic_scope_)},eval_path_{std::move(other.eval_path_)},
              flags_(other.flags_)
        {
        }

        eval_context(const eval_context& parent, const schema_validator<Json> *validator)
            : dynamic_scope_ { parent.dynamic_scope_ }, eval_path_{ parent.eval_path_ },
              flags_(parent.flags_)
        {
            if (validator->id() || dynamic_scope_.empty())
            {
                dynamic_scope_.push_back(validator);
            }
        }

        eval_context(const eval_context& parent, const schema_validator<Json> *validator,
            evaluation_flags flags)
            : dynamic_scope_ { parent.dynamic_scope_ }, eval_path_{ parent.eval_path_ },
              flags_(flags)
        {
            if (validator->id() || dynamic_scope_.empty())
            {
                dynamic_scope_.push_back(validator);
            }
        }

        eval_context(const eval_context& parent, const std::string& name)
            : dynamic_scope_{parent.dynamic_scope_}, eval_path_(parent.eval_path() / name),
              flags_(parent.flags_)
              
        {
        }

        eval_context(const eval_context& parent, const std::string& name,
            evaluation_flags flags)
            : dynamic_scope_{parent.dynamic_scope_}, eval_path_(parent.eval_path() / name),
              flags_(flags)
        {
        }

        eval_context(const eval_context& parent, std::size_t index)
            : dynamic_scope_{parent.dynamic_scope_}, eval_path_(parent.eval_path() / index),
              flags_(parent.flags_)
        {
        }

        eval_context(const eval_context& parent, std::size_t index,
            evaluation_flags flags)
            : dynamic_scope_{parent.dynamic_scope_}, eval_path_(parent.eval_path() / index),
              flags_(flags)
        {
        }

        const std::vector<const schema_validator<Json>*>& dynamic_scope() const
        {
            return dynamic_scope_;
        }

        const jsonpointer::json_pointer& eval_path() const
        {
            return eval_path_;
        }
        
        evaluation_flags eval_flags() const
        {
            return flags_;
        }
        
        bool require_evaluated_properties() const
        {
            return (flags_ & evaluation_flags::require_evaluated_properties) == evaluation_flags::require_evaluated_properties;
        }

        bool require_evaluated_items() const
        {
            return (flags_ & evaluation_flags::require_evaluated_items) == evaluation_flags::require_evaluated_items;
        }
    }; 

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_COMPILATION_CONTEXT_HPP
