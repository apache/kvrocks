// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_COMMON_SCHEMA_VALIDATOR_FACTORY_BASE_HPP
#define JSONCONS_EXT_JSONSCHEMA_COMMON_SCHEMA_VALIDATOR_FACTORY_BASE_HPP

#include <cstddef>
#include <functional>
#include <memory>
#include <string>
#include <unordered_map>
#include <utility>

#include <jsoncons/config/compiler_support.hpp>

#include <jsoncons_ext/jsonschema/common/compilation_context.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator.hpp>
#include <jsoncons_ext/jsonschema/evaluation_options.hpp>

namespace jsoncons {
namespace jsonschema {

    template <typename Json>
    using resolve_uri_type = std::function<Json(const jsoncons::uri & /*id*/)>;

    template <typename Json>
    class schema_validator_factory_base
    {
    public:
        using schema_store_type = std::map<jsoncons::uri, schema_validator<Json>*>;
        using validator_factory_factory_type = std::function<std::unique_ptr<schema_validator_factory_base<Json>>(const Json&,
            const evaluation_options&,schema_store_type*,const std::vector<resolve_uri_type<Json>>&,
            const std::unordered_map<std::string,bool>&)>;
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;
        using ref_validator_type = ref_validator<Json>;
        using ref_type = ref<Json>;
        using anchor_uri_map_type = std::unordered_map<std::string,uri_wrapper>;

    private:
        std::string spec_version_;
        std::unique_ptr<Json> root_schema_;
        validator_factory_factory_type factory_factory_;
        evaluation_options options_;
        schema_store_type* schema_store_ptr_;
        std::vector<resolve_uri_type<Json>> resolve_funcs_;
        std::unordered_map<std::string,bool> vocabulary_;

        schema_validator_ptr_type root_;       
        
        // Owns external schemas
        std::vector<schema_validator_ptr_type> schema_validators_;
    protected:
        std::vector<std::pair<jsoncons::uri, ref_type*>> unresolved_refs_; 
        std::map<jsoncons::uri, Json> unknown_keywords_;

    public:

        schema_validator_factory_base(const std::string& version, Json&& root_schema, const validator_factory_factory_type& factory_factory,
            evaluation_options options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs)
            : spec_version_(version), factory_factory_(factory_factory), options_(std::move(options)),
              schema_store_ptr_(schema_store_ptr), resolve_funcs_(resolve_funcs)
        {
            JSONCONS_ASSERT(schema_store_ptr != nullptr);
            root_schema_ = jsoncons::make_unique<Json>(std::move(root_schema));
        }

        schema_validator_factory_base(const std::string& version, Json&& root_schema, const validator_factory_factory_type& factory_factory,
            evaluation_options options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs,
            const std::unordered_map<std::string,bool>& vocabulary)
            : spec_version_(version), factory_factory_(factory_factory), options_(std::move(options)),
              schema_store_ptr_(schema_store_ptr), resolve_funcs_(resolve_funcs), vocabulary_(vocabulary)
        {
            JSONCONS_ASSERT(schema_store_ptr != nullptr);
            root_schema_ = jsoncons::make_unique<Json>(std::move(root_schema));
        }

        virtual ~schema_validator_factory_base() = default;

        const std::unordered_map<std::string,bool>& vocabulary() const {return vocabulary_;}
        
        void save_schema(schema_validator_ptr_type&& schema)
        {
            schema_validators_.emplace_back(std::move(schema));
        }

        const std::string& schema() const
        {
            return spec_version_;
        }
        
        void build_schema() 
        {
            anchor_uri_map_type anchor_dict;
            root_ = make_schema_validator(compilation_context<Json>(uri_wrapper(options_.default_base_uri())), *root_schema_, {}, anchor_dict);
        }

        void build_schema(const std::string& retrieval_uri) 
        {
            anchor_uri_map_type anchor_dict;
            root_ = make_schema_validator(compilation_context<Json>(uri_wrapper(retrieval_uri)), *root_schema_, {}, anchor_dict);
        }

        evaluation_options options() const
        {
            return options_;
        }
        
        schema_validator_ptr_type make_boolean_schema(const compilation_context<Json>& context, const Json& sch)
        {
            uri schema_location = context.get_base_uri();
            schema_validator_ptr_type schema_validator_ptr = jsoncons::make_unique<boolean_schema_validator<Json>>( 
                schema_location, sch.template as<bool>());

            return schema_validator_ptr;
        }
        
        std::unique_ptr<document_schema_validator<Json>> get_schema_validator()
        {                        
            //std::cout << "schema_store:\n";
            //for (auto& member : *schema_store_ptr_)
            //{
            //    std::cout << "    " << member.first.string() << "\n";
            //}

            // load all external schemas that have not already been loaded           
            // new unresolved refs may be added to the end as earlier ones are resolved
            for (std::size_t i = 0; i < unresolved_refs_.size(); ++i)
            {
                auto loc = unresolved_refs_[i].first;
                //std::cout << "unresolved: " << loc.string() << "\n";
                if (schema_store_ptr_->find(loc) == schema_store_ptr_->end()) // registry for this file is empty
                {
                    bool found = false;
                    for (auto it = resolve_funcs_.begin(); it != resolve_funcs_.end() && !found; ++it)
                    {
                        Json external_sch = (*it)(loc);

                        if (external_sch.is_object() || external_sch.is_bool())
                        {
                            anchor_uri_map_type anchor_dict2;
                            this->save_schema(make_cross_draft_schema_validator(compilation_context<Json>(uri_wrapper(loc.base())), 
                                std::move(external_sch), {}, anchor_dict2));
                            found = true;
                        }
                    }
                    if (found)
                    {
                        // Try resolving again
                        if (schema_store_ptr_->find(loc) == schema_store_ptr_->end()) 
                        {
                            JSONCONS_THROW(jsonschema::schema_error("Unresolved reference '" + loc.string() + "'"));
                        }
                    }
                    else
                    {
                        JSONCONS_THROW(jsonschema::schema_error("Don't know how to load JSON Schema '" + loc.base().string() + "'" ));
                    }
                }
            }

            resolve_references();

            return jsoncons::make_unique<document_schema_validator<Json>>(std::move(root_schema_), std::move(root_), std::move(schema_validators_));
        }

        void resolve_references()
        {
            for (auto& ref : unresolved_refs_)
            {
                auto it = schema_store_ptr_->find(ref.first);
                if (it == schema_store_ptr_->end())
                {
                    JSONCONS_THROW(schema_error("Undefined reference " + ref.first.string()));
                }
                if ((*it).second == nullptr)
                {
                    JSONCONS_THROW(schema_error("Null referred schema " + ref.first.string()));
                }
                ref.second->set_referred_schema((*it).second);
            }
        }

        void insert_schema(const uri_wrapper& identifier, schema_validator<Json>* s)
        {
            this->schema_store_ptr_->emplace(identifier.uri(), s);
        }

        void insert_unknown_keyword(const uri_wrapper& uri, 
                                    const std::string& key, 
                                    const Json& value)
        {
            auto new_u = uri.append(key);
            uri_wrapper new_uri(new_u);

            if (new_uri.has_fragment() && !new_uri.has_plain_name_fragment()) 
            {
                // is there a reference looking for this unknown-keyword, which is thus no longer a unknown keyword but a schema
                auto unresolved_refs = std::find_if(this->unresolved_refs_.begin(), this->unresolved_refs_.end(),
                    [new_uri](const std::pair<jsoncons::uri,ref<Json>*>& pr) {return pr.first == new_uri.uri();});
                if (unresolved_refs != this->unresolved_refs_.end())
                {
                    anchor_uri_map_type anchor_dict2;
                    this->save_schema(make_cross_draft_schema_validator(compilation_context<Json>(new_uri), value, {}, anchor_dict2));
                }
                else // no, nothing ref'd it, keep for later
                {
                    //file.unknown_keywords.emplace(fragment, value);
                    this->unknown_keywords_.emplace(new_uri.uri(), value);
                }

                // recursively add possible subschemas of unknown keywords
                if (value.type() == json_type::object_value)
                {
                    for (const auto& subsch : value.object_range())
                    {
                        insert_unknown_keyword(new_uri, subsch.key(), subsch.value());
                    }
                }
            }
        }

        std::unique_ptr<ref_validator<Json>> get_or_create_reference(const Json& schema, const uri_wrapper& identifier)
        {
            // a schema already exists
            auto it = this->schema_store_ptr_->find(identifier.uri());
            if (it != this->schema_store_ptr_->end())
            {
                return jsoncons::make_unique<ref_validator_type>(schema, identifier.uri(), (*it).second);
            }

            // referencing an unknown keyword, turn it into schema
            //
            // an unknown keyword can only be referenced by a JSONPointer,
            // not by a plain name identifier
            if (identifier.has_fragment() && !identifier.has_plain_name_fragment()) 
            {
                //std::string fragment = std::string(identifier.fragment());

                auto it2 = this->unknown_keywords_.find(identifier.uri());
                if (it2 != this->unknown_keywords_.end())
                {
                    auto& subsch = it2->second;
                    anchor_uri_map_type anchor_dict2;
                    auto s = make_cross_draft_schema_validator(compilation_context<Json>(identifier), subsch, {}, anchor_dict2);
                    this->unknown_keywords_.erase(it2);
                    auto orig = jsoncons::make_unique<ref_validator_type>(schema, identifier.uri(), s.get());
                    this->save_schema(std::move(s));
                    return orig;
                }
            }

            // get or create a ref_validator
            auto orig = jsoncons::make_unique<ref_validator_type>(schema, identifier.uri());

            this->unresolved_refs_.emplace_back(identifier.uri(), orig.get());
            return orig;
        }

        static bool validate_anchor(const std::string& s)
        {
            if (s.empty())
            {
                return false;
            }
            if (!((s[0] >= 'a' && s[0] <= 'z') || (s[0] >= 'A' && s[0] <= 'Z')))
            {
                return false;
            }

            for (std::size_t i = 1; i < s.size(); ++i)
            {
                switch (s[i])
                {
                    case '-':
                    case '_':
                    case ':':
                    case '.':
                        break;
                    default:
                        if (!((s[i] >= 'a' && s[i] <= 'z') || (s[i] >= 'A' && s[i] <= 'Z') || (s[i] >= '0' && s[i] <= '9')))
                        {
                            return false;
                        }
                        break;
                }
            }
            return true;
        }

        virtual compilation_context<Json> make_compilation_context(const compilation_context<Json>& parent,
            const Json& sch, jsoncons::span<const std::string> keys) const = 0;

        virtual schema_validator_ptr_type make_schema_validator(const compilation_context<Json>& context, 
            const Json& sch, jsoncons::span<const std::string> keys, anchor_uri_map_type& anchor_dict) = 0;

        schema_validator_ptr_type make_cross_draft_schema_validator(const compilation_context<Json>& context, 
            const Json& sch, jsoncons::span<const std::string> keys, anchor_uri_map_type& anchor_dict)
        {
            schema_validator_ptr_type schema_val = schema_validator_ptr_type{};
            switch (sch.type())
            {
                case json_type::object_value:
                {
                    auto it = sch.find("$schema");
                    if (it != sch.object_range().end())
                    {
                        if ((*it).value().as_string_view() == schema())
                        {
                            return make_schema_validator(context, std::move(sch), keys, anchor_dict);
                        }
                        else
                        {
                            auto schema_validator_factory_base = factory_factory_(std::move(sch), options_, schema_store_ptr_, resolve_funcs_, vocabulary_);
                            schema_validator_factory_base->build_schema(context.get_base_uri().string());
                            schema_val = schema_validator_factory_base->get_schema_validator();
                        }
                    }
                    else
                    {
                        return make_schema_validator(context, std::move(sch), keys, anchor_dict);
                    }
                    break;
                }
                case json_type::bool_value:
                {
                    return make_schema_validator(context, std::move(sch), keys, anchor_dict);
                }
                default:
                    JSONCONS_THROW(schema_error("Schema must be object or boolean"));
            }
            return schema_val;
        }
    };

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_COMMON_SCHEMA_HPP
