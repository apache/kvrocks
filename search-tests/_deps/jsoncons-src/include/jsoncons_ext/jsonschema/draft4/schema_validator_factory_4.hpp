// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_DRAFT4_SCHEMA_VALIDATOR_FACTORY_4_HPP
#define JSONCONS_EXT_JSONSCHEMA_DRAFT4_SCHEMA_VALIDATOR_FACTORY_4_HPP

#include <cassert>
#include <iostream>
#include <set>
#include <string>
#include <unordered_map>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/utility/uri.hpp>

#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>
#include <jsoncons_ext/jsonschema/common/compilation_context.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator_factory_base.hpp>
#include <jsoncons_ext/jsonschema/common/keyword_validator_factory.hpp>
#include <jsoncons_ext/jsonschema/common/schema_validator.hpp>
#include <jsoncons_ext/jsonschema/draft4/schema_draft4.hpp>
#include <jsoncons_ext/jsonschema/json_schema.hpp>

#if defined(JSONCONS_HAS_STD_REGEX)
#include <regex>
#endif

namespace jsoncons {
namespace jsonschema {
namespace draft4 {

    template <typename Json>
    class schema_validator_factory_4 : public schema_validator_factory_base<Json> 
    {
    public:
        using schema_store_type = typename schema_validator_factory_base<Json>::schema_store_type;
        using validator_factory_factory_type = typename schema_validator_factory_base<Json>::validator_factory_factory_type;
        using keyword_validator_ptr_type = typename std::unique_ptr<keyword_validator<Json>>;
        using schema_validator_ptr_type = typename std::unique_ptr<schema_validator<Json>>;
        using anchor_uri_map_type = std::unordered_map<std::string,uri_wrapper>;
    private:

        using keyword_factory_type = std::function<keyword_validator_ptr_type(const compilation_context<Json>& context, 
            const Json& sch, const Json& parent, anchor_uri_map_type&)>;

        std::unordered_map<std::string,keyword_factory_type> keyword_factory_map_;
        keyword_validator_factory<Json> factory_;

    public:
        schema_validator_factory_4(Json&& sch, const validator_factory_factory_type& factory_factory, 
            evaluation_options options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs) 
            : schema_validator_factory_base<Json>(schema_version::draft4(), std::move(sch), factory_factory, options, schema_store_ptr, resolve_funcs),
              factory_(this)
        {
            init();
        }

        schema_validator_factory_4(const schema_validator_factory_4&) = delete;
        schema_validator_factory_4& operator=(const schema_validator_factory_4&) = delete;
        schema_validator_factory_4(schema_validator_factory_4&&) = default;
        schema_validator_factory_4& operator=(schema_validator_factory_4&&) = default;

        void init()
        {
            keyword_factory_map_.emplace("type", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_type_validator(context, sch, parent);});
            keyword_factory_map_.emplace("contentEncoding", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_content_encoding_validator(context, sch, parent);});
            keyword_factory_map_.emplace("contentMediaType", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_content_media_type_validator(context, sch, parent);});
            if (this->options().require_format_validation())
            {
                keyword_factory_map_.emplace("format", 
                    [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_format_validator(context, sch, parent);});
            }
#if defined(JSONCONS_HAS_STD_REGEX)
            keyword_factory_map_.emplace("pattern", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_pattern_validator(context, sch, parent);});
#endif
            keyword_factory_map_.emplace("maxItems", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_max_items_validator(context, sch, parent);});
            keyword_factory_map_.emplace("minItems", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_min_items_validator(context, sch, parent);});
            keyword_factory_map_.emplace("maxProperties", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_max_properties_validator(context, sch, parent);});
            keyword_factory_map_.emplace("minProperties", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_min_properties_validator(context, sch, parent);});
            keyword_factory_map_.emplace("uniqueItems", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_unique_items_validator(context, sch, parent);});
            keyword_factory_map_.emplace("maxLength", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_max_length_validator(context, sch, parent);});
            keyword_factory_map_.emplace("minLength", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_min_length_validator(context, sch, parent);});
            keyword_factory_map_.emplace("not", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_not_validator(context, sch, parent, anchor_dict);});
            keyword_factory_map_.emplace("maximum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return make_maximum_validator_4(context, sch, parent);});
            keyword_factory_map_.emplace("minimum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return make_minimum_validator_4(context, sch, parent);});
            keyword_factory_map_.emplace("multipleOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_multiple_of_validator(context, sch, parent);});
            keyword_factory_map_.emplace("enum", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_enum_validator(context, sch, parent);});
            keyword_factory_map_.emplace("allOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_all_of_validator(context, sch, parent, anchor_dict);});
            keyword_factory_map_.emplace("anyOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_any_of_validator(context, sch, parent, anchor_dict);});
            keyword_factory_map_.emplace("oneOf", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_one_of_validator(context, sch, parent, anchor_dict);});
            keyword_factory_map_.emplace("dependencies", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type& anchor_dict){return factory_.make_dependencies_validator(context, sch, parent, anchor_dict);});
            keyword_factory_map_.emplace("required", 
                [&](const compilation_context<Json>& context, const Json& sch, const Json& parent, anchor_uri_map_type&){return factory_.make_required_validator(context, sch, parent);});
        }

        schema_validator_ptr_type make_schema_validator( 
            const compilation_context<Json>& context, const Json& sch, jsoncons::span<const std::string> keys, 
            anchor_uri_map_type& anchor_dict) override
        {
            auto new_context = make_compilation_context(context, sch, keys);
            //std::cout << "make_schema_validator " << context.get_base_uri().string() << ", " << new_context.get_base_uri().string() << "\n\n";

            schema_validator_ptr_type schema_validator_ptr;

            switch (sch.type())
            {
                case json_type::bool_value:
                {
                    schema_validator_ptr = this->make_boolean_schema(new_context, sch);
                    schema_validator<Json>* p = schema_validator_ptr.get();
                    for (const auto& uri : new_context.uris()) 
                    { 
                        this->insert_schema(uri, p);
                    }          
                    break;
                }
                case json_type::object_value:
                {
                    auto it = sch.find("$ref");
                    if (it != sch.object_range().end()) // this schema is a reference
                    {
                        std::vector<keyword_validator_ptr_type> validators;
                        std::map<std::string,schema_validator_ptr_type> defs;

                        auto it2 = sch.find("definitions");
                        if (it2 != sch.object_range().end()) 
                        {
                            for (const auto& def : it2->value().object_range())
                            {
                                std::string sub_keys[] = { "definitions", def.key() };
                                defs.emplace(def.key(), make_schema_validator(context, def.value(), sub_keys, anchor_dict));
                            }
                        }

                        Json default_value{ jsoncons::null_type() };
                        uri relative((*it).value().template as<std::string>()); 
                        auto id = context.get_base_uri().resolve(relative);
                        validators.push_back(this->get_or_create_reference(sch, uri_wrapper{id}));
                        schema_validator_ptr = jsoncons::make_unique<object_schema_validator<Json>>(
                            new_context.get_base_uri(), context.id(),
                            std::move(validators), std::move(defs), std::move(default_value));
                    }
                    else
                    {
                        schema_validator_ptr = make_object_schema_validator(new_context, sch, anchor_dict);
                    }
                    schema_validator<Json>* p = schema_validator_ptr.get();
                    for (const auto& uri : new_context.uris()) 
                    { 
                        this->insert_schema(uri, p);
                        for (const auto& item : sch.object_range())
                        {
                            if (known_keywords().find(item.key()) == known_keywords().end())
                            {
                                this->insert_unknown_keyword(uri, item.key(), item.value()); // save unknown keywords for later reference
                            }
                        }
                    }          
                    break;
                }
                default:
                    JSONCONS_THROW(schema_error("invalid JSON-type for a schema for " + new_context.get_base_uri().string() + ", expected: boolean or object"));
                    break;
            }
            
            return schema_validator_ptr;
        }

        schema_validator_ptr_type make_object_schema_validator(const compilation_context<Json>& context, 
            const Json& sch, anchor_uri_map_type& anchor_dict)
        {
            jsoncons::optional<jsoncons::uri> id = context.id();
            Json default_value{ jsoncons::null_type() };
            std::vector<keyword_validator_ptr_type> validators;
            std::map<std::string,schema_validator_ptr_type> defs;

            auto it = sch.find("definitions");
            if (it != sch.object_range().end()) 
            {
                for (const auto& def : (*it).value().object_range())
                {
                    std::string sub_keys[] = { "definitions", def.key() };
                    defs.emplace(def.key(), make_schema_validator(context, def.value(), sub_keys, anchor_dict));
                }
            }

            it = sch.find("default");
            if (it != sch.object_range().end()) 
            {
                default_value = (*it).value();
            }

            for (const auto& key_value : sch.object_range())
            {
                auto factory_it = keyword_factory_map_.find(key_value.key());
                if (factory_it != keyword_factory_map_.end())
                {
                    auto validator = (*factory_it).second(context, key_value.value(), sch, anchor_dict);
                    if (validator)
                    {   
                        validators.emplace_back(std::move(validator));
                    }
                }
            }
            
            std::unique_ptr<properties_validator<Json>> properties;
            it = sch.find("properties");
            if (it != sch.object_range().end()) 
            {
                properties = factory_.make_properties_validator(context, (*it).value(), sch, anchor_dict);
            }
            std::unique_ptr<pattern_properties_validator<Json>> pattern_properties;

    #if defined(JSONCONS_HAS_STD_REGEX)
            it = sch.find("patternProperties");
            if (it != sch.object_range().end())
            {
                pattern_properties = factory_.make_pattern_properties_validator(context, (*it).value(), sch, anchor_dict);
            }
    #endif

            it = sch.find("additionalProperties");
            if (it != sch.object_range().end()) 
            {
                validators.emplace_back(factory_.make_additional_properties_validator(context, (*it).value(), sch,
                    std::move(properties), std::move(pattern_properties), anchor_dict));
            }
            else
            {
                if (properties)
                {
                    validators.emplace_back(std::move(properties));
                }
#if defined(JSONCONS_HAS_STD_REGEX)
                if (pattern_properties)
                {
                    validators.emplace_back(std::move(pattern_properties));
                }
#endif
            }

            it = sch.find("items");
            if (it != sch.object_range().end()) 
            {

                if ((*it).value().type() == json_type::array_value) 
                {
                    validators.emplace_back(factory_.make_prefix_items_validator_07(context, (*it).value(), sch, anchor_dict));
                } 
                else if ((*it).value().type() == json_type::object_value ||
                           (*it).value().type() == json_type::bool_value)
                {
                    validators.emplace_back(factory_.make_items_validator("items", context, (*it).value(), sch, anchor_dict));
                }
            }
            return jsoncons::make_unique<object_schema_validator<Json>>(context.get_base_uri(), 
                std::move(id), std::move(validators), std::move(defs), std::move(default_value));
        }

        std::unique_ptr<keyword_validator<Json>> make_maximum_validator_4(const compilation_context<Json>& context, 
            const Json& sch, const Json& parent)
        {
            uri schema_location = context.make_schema_location("maximum");
            if (!sch.is_number())
            {
                const std::string message("maximum must be a number value");
                JSONCONS_THROW(schema_error(message));
            }
            
            bool is_exclusive = false;

            if (parent.is_object())
            {
                auto it = parent.find("exclusiveMaximum");
                if (it != parent.object_range().end())
                {
                    is_exclusive = (*it).value().as_bool();
                }
            }
            if (is_exclusive)
            {
                return jsoncons::make_unique<exclusive_maximum_validator<Json>>(parent, schema_location, 
                    context.get_custom_message("maximum"), sch);
            }
            else
            {
                return jsoncons::make_unique<maximum_validator<Json>>(parent, schema_location,
                    context.get_custom_message("maximum"), sch);
            }
        }

        virtual std::unique_ptr<keyword_validator<Json>> make_minimum_validator_4(const compilation_context<Json>& context, 
            const Json& sch, const Json& parent)
        {
            uri schema_location = context.make_schema_location("minimum");

            if (!sch.is_number())
            {
                const std::string message("minimum must be an integer");
                JSONCONS_THROW(schema_error(message));
            }

            bool is_exclusive = false;
            if (parent.is_object())
            {
                auto it = parent.find("exclusiveMinimum");
                if (it != parent.object_range().end())
                {
                    is_exclusive = (*it).value().as_bool();
                }
            }
            if (is_exclusive)
            {
                return jsoncons::make_unique<exclusive_minimum_validator<Json>>(parent, schema_location,
                    context.get_custom_message("minimum"), sch);
            }
            else
            {
                return jsoncons::make_unique<minimum_validator<Json>>(parent, schema_location,
                    context.get_custom_message("minimum"), sch);
            }
        }

    private:

        compilation_context<Json> make_compilation_context(const compilation_context<Json>& parent, 
            const Json& sch, jsoncons::span<const std::string> keys) const override
        {
            // Exclude uri's that are not plain name identifiers
            std::vector<uri_wrapper> new_uris;
            for (const auto& uri : parent.uris())
            {
                if (!uri.has_plain_name_fragment())
                {
                    new_uris.push_back(uri);
                }
            }

            // Append the keys for this sub-schema to the uri's
            for (const auto& key : keys)
            {
                for (auto& uri : new_uris)
                {
                    auto new_u = uri.append(key);
                    uri = uri_wrapper(new_u);
                }
            }
            jsoncons::optional<uri> id;
            std::unordered_map<std::string,std::string> custom_messages{parent.custom_messages()};
            std::string custom_message;
            if (sch.is_object())
            {
                auto it = sch.find("id"); // If id is found, this schema can be referenced by the id
                if (it != sch.object_range().end()) 
                {
                    uri relative((*it).value().template as<std::string>()); 
                    auto resolved = parent.get_base_uri().resolve(relative);
                    id = resolved;
                    uri_wrapper new_uri{ resolved };
                    //std::cout << "id: " << id << ", " << new_uri.string() << "\n";
                    // Add it to the list if it is not already there
                    if (std::find(new_uris.begin(), new_uris.end(), new_uri) == new_uris.end())
                    {
                        new_uris.emplace_back(new_uri); 
                    }
                }

                if (this->options().enable_custom_error_message())
                {
                    it = sch.find("errorMessage"); 
                    if (it != sch.object_range().end()) 
                    {
                        const auto& value = it->value();
                        if (value.is_object())
                        {
                            for (const auto& item : value.object_range())
                            {
                                custom_messages[item.key()] =  item.value().template as<std::string>();
                            }
                        }
                        else if (value.is_string())
                        {
                            custom_message = value.template as<std::string>();
                        }
                    }
                }
            }

/*
            std::cout << "Absolute URI: " << parent.get_base_uri().string() << "\n";
            for (const auto& uri : new_uris)
            {
                std::cout << "    " << uri.string() << "\n";
            }
*/

            return compilation_context<Json>(new_uris, id, custom_messages, custom_message);
        }
    private:
        static const std::unordered_set<std::string>& known_keywords()
        {
            static std::unordered_set<std::string> keywords{
                "id",                 
                "$ref",                
                "additionalItems",     
                "additionalProperties",
                "allOf",               
                "anyOf",               
                "const",               
                "contains",            
                "contentEncoding",     
                "contentMediaType",    
                "default",    
                "definitions",         
                "dependencies",        
                "enum",                
                "exclusiveMaximum",
                "exclusiveMaximum",
                "exclusiveMinimum",
                "exclusiveMinimum",
                "items",               
                "maximum",             
                "maxItems",            
                "maxLength",           
                "maxProperties",       
                "minimum",             
                "minItems",            
                "minLength",           
                "minProperties",       
                "multipleOf",          
                "not",                 
                "oneOf",               
                "pattern",             
                "patternProperties",   
                "properties",          
                "propertyNames",       
                "readOnly",            
                "required",            
                "type",                
                "uniqueItems",         
                "writeOnly"           
            };
            return keywords;
        }
    };

} // namespace draft4
} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_DRAFT4_KEYWORD_FACTORY_HPP
