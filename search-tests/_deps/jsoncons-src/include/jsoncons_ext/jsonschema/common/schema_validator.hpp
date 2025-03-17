// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_SCHEMA_VALIDATOR_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_SCHEMA_VALIDATOR_HPP

#include <cstddef>
#include <unordered_map>
#include <unordered_set>

#include <jsoncons/config/jsoncons_config.hpp>
#include <jsoncons/utility/uri.hpp>
#include <jsoncons_ext/jsonschema/common/eval_context.hpp>
#include <jsoncons_ext/jsonschema/common/keyword_validator.hpp>
#include <jsoncons_ext/jsonschema/jsonschema_error.hpp>

namespace jsoncons {
namespace jsonschema {

    template <typename Json>
    class schema_validator : public validator_base<Json>
    {
    public:
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;

    public:
        schema_validator()
        {}

        virtual jsoncons::optional<Json> get_default_value() const = 0;

        virtual bool recursive_anchor() const = 0;

        virtual const jsoncons::optional<jsoncons::uri>& id() const = 0;

        virtual const schema_validator<Json>* get_schema_for_dynamic_anchor(const std::string& anchor) const = 0;

        virtual const jsoncons::optional<jsoncons::uri>& dynamic_anchor() const = 0;
    };

    template <typename Json>
    class document_schema_validator : public schema_validator<Json>
    {
        using schema_validator_ptr_type = std::unique_ptr<schema_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        std::unique_ptr<Json> root_schema_;
        schema_validator_ptr_type schema_val_;
        std::vector<schema_validator_ptr_type> schemas_;
    public:
        document_schema_validator(std::unique_ptr<Json>&& root_schema, schema_validator_ptr_type&& schema_val, std::vector<schema_validator_ptr_type>&& schemas)
            : root_schema_(std::move(root_schema)), schema_val_(std::move(schema_val)), schemas_(std::move(schemas))
        {
            if (schema_val_ == nullptr)
                JSONCONS_THROW(schema_error("There is no schema to validate an instance against"));
        }

        document_schema_validator(const document_schema_validator&) = delete;
        document_schema_validator(document_schema_validator&&) = default;
        document_schema_validator& operator=(const document_schema_validator&) = delete;
        document_schema_validator& operator=(document_schema_validator&&) = default;      

        jsoncons::optional<Json> get_default_value() const final
        {
            return schema_val_->get_default_value();
        }

        const uri& schema_location() const final
        {
            return schema_val_->schema_location();
        }

        bool recursive_anchor() const final
        {
            return schema_val_->recursive_anchor();
        }

        const jsoncons::optional<jsoncons::uri>& id() const final
        {
            return schema_val_->id();
        }

        const jsoncons::optional<jsoncons::uri>& dynamic_anchor() const final
        {
            return schema_val_->dynamic_anchor();
        }

        const schema_validator<Json>* get_schema_for_dynamic_anchor(const std::string& anchor) const final
        {
            return schema_val_->get_schema_for_dynamic_anchor(anchor);
        }

        bool always_fails() const final
        {
            return schema_val_->always_fails();
        }

        bool always_succeeds() const final
        {
            return schema_val_->always_succeeds();
        }

        walk_result walk(const eval_context<Json>& context, 
            const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const 
        {
            return do_walk(context, instance, instance_location, reporter);
        }
        
    private:
        walk_result do_validate(const eval_context<Json>& context, 
            const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results,
            error_reporter& reporter, 
            Json& patch) const 
        {
            JSONCONS_ASSERT(schema_val_ != nullptr);
            return schema_val_->validate(context, instance, instance_location, results, reporter, patch);
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final
        {
            JSONCONS_ASSERT(schema_val_ != nullptr);
            return schema_val_->walk(context, instance, instance_location, reporter);
        }
    };

    template <typename Json>
    class boolean_schema_validator : public schema_validator<Json>
    {
    public:
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        uri schema_location_;
        bool value_;

        jsoncons::optional<jsoncons::uri> id_;

        jsoncons::optional<jsoncons::uri> dynamic_anchor_;

    public:
        boolean_schema_validator(const boolean_schema_validator&) = delete;
        boolean_schema_validator& operator=(const boolean_schema_validator&) = delete;
        boolean_schema_validator(boolean_schema_validator&&) = default;
        boolean_schema_validator& operator=(boolean_schema_validator&&) = default;
        boolean_schema_validator(const uri& schema_location, bool value)
            : schema_location_(schema_location), value_(value)
        {
        }

        jsoncons::optional<Json> get_default_value() const final
        {
            return jsoncons::optional<Json>{};
        }

        const uri& schema_location() const final
        {
            return schema_location_;
        }

        bool recursive_anchor() const final
        {
            return false;
        }

        const jsoncons::optional<jsoncons::uri>& id() const final
        {
            return id_;
        }

        const jsoncons::optional<jsoncons::uri>& dynamic_anchor() const final
        {
            return dynamic_anchor_;
        }

        const schema_validator<Json>* get_schema_for_dynamic_anchor(const std::string& /*anchor*/) const final
        {
            return nullptr;
        }

        bool always_fails() const final
        {
            return !value_;
        }

        bool always_succeeds() const final
        {
            return value_;
        }

    private:

        walk_result do_validate(const eval_context<Json>& context, const Json&, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& /*results*/, 
            error_reporter& reporter, 
            Json& /*patch*/) const final
        {
            if (!value_)
            {
                reporter.error(validation_message("false", 
                    context.eval_path(),
                    this->schema_location(), 
                    instance_location, 
                    "False schema always fails"));
            }
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& /*context*/, const Json& /*instance*/,
            const jsonpointer::json_pointer& /*instance_location*/, const walk_reporter_type& /*reporter*/) const final 
        {
            return walk_result::advance;
        }
    };
 
    template <typename Json>
    class object_schema_validator : public schema_validator<Json>
    {
    public:
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;
        using keyword_validator_ptr_type = typename std::unique_ptr<keyword_validator<Json>>;
        using anchor_schema_map_type = std::unordered_map<std::string,std::unique_ptr<ref_validator<Json>>>;
        using walk_reporter_type = typename json_schema_traits<Json>::walk_reporter_type;

        uri schema_location_;
        jsoncons::optional<jsoncons::uri> id_;
        std::vector<keyword_validator_ptr_type> validators_; 
        std::unique_ptr<unevaluated_properties_validator<Json>> unevaluated_properties_val_;
        std::unique_ptr<unevaluated_items_validator<Json>> unevaluated_items_val_;
        std::map<std::string,schema_validator_ptr_type> defs_;
        Json default_value_;
        bool recursive_anchor_;
        jsoncons::optional<jsoncons::uri> dynamic_anchor_;
        anchor_schema_map_type anchor_dict_;
        bool always_succeeds_;
        bool always_fails_;

    public:
        object_schema_validator(const object_schema_validator&) = delete;
        object_schema_validator& operator=(const object_schema_validator&) = delete;
        object_schema_validator(object_schema_validator&&) = default;
        object_schema_validator& operator=(object_schema_validator&&) = default;
        object_schema_validator(const uri& schema_location,
            const jsoncons::optional<jsoncons::uri>& id,
            std::vector<keyword_validator_ptr_type>&& validators, 
            std::map<std::string,schema_validator_ptr_type>&& defs,
            Json&& default_value)
            : schema_location_(schema_location),
              id_(id),
              validators_(std::move(validators)),
              defs_(std::move(defs)),
              default_value_(std::move(default_value)),
              recursive_anchor_(false),
              always_succeeds_(false), always_fails_(false) 
        {
            init();
        }
        object_schema_validator(const uri& schema_location, 
            const jsoncons::optional<jsoncons::uri>& id,
            std::vector<keyword_validator_ptr_type>&& validators,
            std::unique_ptr<unevaluated_properties_validator<Json>>&& unevaluated_properties_val, 
            std::unique_ptr<unevaluated_items_validator<Json>>&& unevaluated_items_val, 
            std::map<std::string,schema_validator_ptr_type>&& defs,
            Json&& default_value, bool recursive_anchor)
            : schema_location_(schema_location),
              id_(id),
              validators_(std::move(validators)),
              unevaluated_properties_val_(std::move(unevaluated_properties_val)),
              unevaluated_items_val_(std::move(unevaluated_items_val)),
              defs_(std::move(defs)),
              default_value_(std::move(default_value)),
              recursive_anchor_(recursive_anchor),
              always_succeeds_(false), always_fails_(false)
        {
            init();
        }
        object_schema_validator(const uri& schema_location, 
            const jsoncons::optional<jsoncons::uri>& id,
            std::vector<keyword_validator_ptr_type>&& validators, 
            std::unique_ptr<unevaluated_properties_validator<Json>>&& unevaluated_properties_val, 
            std::unique_ptr<unevaluated_items_validator<Json>>&& unevaluated_items_val, 
            std::map<std::string,schema_validator_ptr_type>&& defs,
            Json&& default_value,
            jsoncons::optional<jsoncons::uri>&& dynamic_anchor,
            anchor_schema_map_type&& anchor_dict)
            : schema_location_(schema_location),
              id_(std::move(id)),
              validators_(std::move(validators)),
              unevaluated_properties_val_(std::move(unevaluated_properties_val)),
              unevaluated_items_val_(std::move(unevaluated_items_val)),
              defs_(std::move(defs)),
              default_value_(std::move(default_value)),
              recursive_anchor_(false),
              dynamic_anchor_(std::move(dynamic_anchor)),
              anchor_dict_(std::move(anchor_dict)),
              always_succeeds_(false), always_fails_(false)
        {
            init();
        }

        jsoncons::optional<Json> get_default_value() const override
        {
            return default_value_;
        }

        const uri& schema_location() const override
        {
            return schema_location_;
        }

        bool recursive_anchor() const final
        {
            return recursive_anchor_;
        }

        const jsoncons::optional<jsoncons::uri>& id() const final
        {
            return id_;
        }

        const schema_validator<Json>* get_schema_for_dynamic_anchor(const std::string& anchor) const final
        {
            auto it = anchor_dict_.find(anchor);
            return (it == anchor_dict_.end()) ? nullptr : (*it).second->referred_schema();
        }

        const jsoncons::optional<jsoncons::uri>& dynamic_anchor() const final
        {
            return dynamic_anchor_;
        }

        bool always_fails() const final
        {
            return always_fails_;
        }

        bool always_succeeds() const final
        {
            return always_succeeds_;
        }

    private:

        void init()
        {
            if (!(unevaluated_properties_val_ || unevaluated_items_val_))
            {
                std::size_t always_fails_count = 0;
                std::size_t always_succeeds_count = 0;
                for (const auto& val : validators_)
                {
                    if (val->always_fails())
                    {
                        ++always_fails_count;
                    }
                    if (val->always_succeeds())
                    {
                        ++always_succeeds_count;
                    }
                }
                always_succeeds_ = always_succeeds_count == validators_.size(); // empty schema always succeeds
                always_fails_ = validators_.size() > 0 && (always_fails_count == validators_.size()); 
            }
        }

        walk_result do_validate(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location,
            evaluation_results& results, 
            error_reporter& reporter, 
            Json& patch) const final
        {
            //std::cout << "object_schema_validator begin[" << context.eval_path().string() << "," << this->schema_location().string() << "]";
            //std::cout << "results:\n";
            //for (const auto& s : results)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";
          
            evaluation_results local_results;

            evaluation_flags flags = context.eval_flags();
            if (unevaluated_properties_val_)
            {
                flags |= evaluation_flags::require_evaluated_properties;
            }
            if (unevaluated_items_val_)
            {
                flags |= evaluation_flags::require_evaluated_items;
            }

            eval_context<Json> this_context{context, this, flags};
            
            //std::cout << "validators:\n";
            for (auto& val : validators_)
            {               
                //std::cout << "    " << val->keyword_name() << "\n";
                walk_result result = val->validate(this_context, instance, instance_location, local_results, reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            
            if (unevaluated_properties_val_)
            {
                walk_result result = unevaluated_properties_val_->validate(this_context, instance, instance_location, local_results, reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (unevaluated_items_val_)
            {
                walk_result result = unevaluated_items_val_->validate(this_context, instance, instance_location, local_results, reporter, patch);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if ((context.eval_flags() & evaluation_flags::require_evaluated_properties)
                 == evaluation_flags::require_evaluated_properties)
            {
                results.merge(std::move(local_results.evaluated_properties));
            }
            if ((context.eval_flags() & evaluation_flags::require_evaluated_items)
                 == evaluation_flags::require_evaluated_items)
            {
                results.merge(std::move(local_results.evaluated_items));
            }
            
            //std::cout << "object_schema_validator end[" << context.eval_path().string() << "," << this->schema_location().string() << "]";
            //std::cout << "results:\n";
            //for (const auto& s : results)
            //{
            //    std::cout << "    " << s << "\n";
            //}
            //std::cout << "\n";
            return walk_result::advance;
        }

        walk_result do_walk(const eval_context<Json>& context, const Json& instance, 
            const jsonpointer::json_pointer& instance_location, const walk_reporter_type& reporter) const final 
        {           
            eval_context<Json> this_context{context, this};
            for (auto& val : validators_)
            {               
                //std::cout << "    " << val->keyword_name() << "\n";
                walk_result result = val->walk(this_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            if (unevaluated_properties_val_)
            {
                walk_result result = unevaluated_properties_val_->walk(this_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }

            if (unevaluated_items_val_)
            {
                walk_result result = unevaluated_items_val_->walk(this_context, instance, instance_location, reporter);
                if (result == walk_result::abort)
                {
                    return result;
                }
            }
            return walk_result::advance;
        }
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_KEYWORD_VALIDATOR_HPP
