// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONSCHEMA_JSON_SCHEMA_FACTORY_HPP
#define JSONCONS_EXT_JSONSCHEMA_JSON_SCHEMA_FACTORY_HPP

#include <string>
#include <unordered_map>

#include <jsoncons/config/compiler_support.hpp>

#include <jsoncons_ext/jsonschema/draft201909/schema_validator_factory_201909.hpp>
#include <jsoncons_ext/jsonschema/draft202012/schema_validator_factory_202012.hpp>
#include <jsoncons_ext/jsonschema/draft4/schema_validator_factory_4.hpp>
#include <jsoncons_ext/jsonschema/draft6/schema_validator_factory_6.hpp>
#include <jsoncons_ext/jsonschema/draft7/schema_validator_factory_7.hpp>

namespace jsoncons {
namespace jsonschema {

    template <typename Json>
    class validator_factory_factory
    {
    public:
        using schema_store_type = std::map<jsoncons::uri, schema_validator<Json>*>;

        validator_factory_factory()
        {
        }
            
        std::unique_ptr<schema_validator_factory_base<Json>> operator()(Json sch, 
            const evaluation_options& options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs,
            const std::unordered_map<std::string,bool>& vocabulary) const
        {
            std::unique_ptr<schema_validator_factory_base<Json>> factory;

            if (sch.is_object())
            {
                auto it = sch.find("$schema");
                if (it != sch.object_range().end())
                { 
                    factory = get_factory(std::move(sch), (*it).value().as_string_view(), options, schema_store_ptr, resolve_funcs, vocabulary);
                    if (!factory)
                    {
                        std::string message("Unsupported schema version ");
                        message.append((*it).value().template as<std::string>());
                        JSONCONS_THROW(schema_error(message));
                    }
                }
                else 
                {
                    factory = get_default_schema_factory(std::move(sch), options, schema_store_ptr, resolve_funcs, vocabulary);
                }
            }
            else if (sch.is_bool())
            {
                factory = get_default_schema_factory(std::move(sch), options, schema_store_ptr, resolve_funcs, vocabulary);
            }
            else
            {
                 JSONCONS_THROW(schema_error("Schema must be object or boolean"));
            }
            return factory;
        }

        std::unique_ptr<schema_validator_factory_base<Json>> get_default_schema_factory(Json&& sch,
            const evaluation_options& options, 
            schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs,
            const std::unordered_map<std::string,bool>& vocabulary) const
        {
            if (options.default_version() == schema_version::draft202012())
            {
                return jsoncons::make_unique<jsoncons::jsonschema::draft202012::schema_validator_factory_202012<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs, vocabulary);
            }
            else if (options.default_version() == schema_version::draft201909())
            {
                return jsoncons::make_unique<jsoncons::jsonschema::draft201909::schema_validator_factory_201909<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs, vocabulary);
            }
            else if (options.default_version() == schema_version::draft7())
            {
                return jsoncons::make_unique<jsoncons::jsonschema::draft7::schema_validator_factory_7<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs);
            }
            else if (options.default_version() == schema_version::draft6())
            {
                return jsoncons::make_unique<jsoncons::jsonschema::draft6::schema_validator_factory_6<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs);
            }
            else if (options.default_version() == schema_version::draft4())
            {
                return jsoncons::make_unique<jsoncons::jsonschema::draft4::schema_validator_factory_4<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs);
            }
            else
            {
                JSONCONS_THROW(schema_error("Unsupported schema version " + options.default_version()));
            }
        }
        
        std::unique_ptr<schema_validator_factory_base<Json>> get_factory(Json&& sch, const jsoncons::string_view& schema_id,
            const evaluation_options& options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs,
            const std::unordered_map<std::string,bool>& vocabulary) const
        {
            std::unique_ptr<schema_validator_factory_base<Json>> factory;

            if (schema_id == schema_version::draft202012())
            {
                factory = jsoncons::make_unique<jsoncons::jsonschema::draft202012::schema_validator_factory_202012<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs, vocabulary);
            }
            else if (schema_id == schema_version::draft201909())
            {
                factory = jsoncons::make_unique<jsoncons::jsonschema::draft201909::schema_validator_factory_201909<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs, vocabulary);
            }
            else if (schema_id == schema_version::draft7())
            {
                factory = jsoncons::make_unique<jsoncons::jsonschema::draft7::schema_validator_factory_7<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs);
            }
            else if (schema_id == schema_version::draft6())
            {
                factory = jsoncons::make_unique<jsoncons::jsonschema::draft6::schema_validator_factory_6<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs);
            }
            else if (schema_id == schema_version::draft4())
            {
                factory = jsoncons::make_unique<jsoncons::jsonschema::draft4::schema_validator_factory_4<Json>>(std::move(sch), *this, 
                    options, schema_store_ptr, resolve_funcs);
            }
            else
            { 
                factory = get_factory_from_meta_schema(std::move(sch), schema_id, options, schema_store_ptr, resolve_funcs);
            }
            return factory;
        }
        
        std::unique_ptr<schema_validator_factory_base<Json>> get_factory_from_meta_schema(Json&& sch, const jsoncons::string_view& schema_id,
            const evaluation_options& options, schema_store_type* schema_store_ptr,
            const std::vector<resolve_uri_type<Json>>& resolve_funcs) const
        {
            std::unique_ptr<schema_validator_factory_base<Json>> factory;

            bool found = false;
            jsoncons::uri uri{ std::string(schema_id) };
            for (auto it = resolve_funcs.begin(); it != resolve_funcs.end() && !found; ++it)
            {
                Json meta_sch = (*it)(uri.base());
                if (meta_sch.is_object())
                {
                    std::unordered_map<std::string,bool> vocabulary;
                    auto vocab_it = meta_sch.find("$vocabulary");
                    if (vocab_it != meta_sch.object_range().end())
                    {
                        const auto& vocab = (*vocab_it).value();
                        if (vocab.is_object())
                        {
                            for (const auto& member : vocab.object_range())
                            {
                                vocabulary.emplace(member.key(), member.value().as_bool());
                            }
                        }
                    }
                    auto schema_it = meta_sch.find("$schema");
                    if (schema_it != meta_sch.object_range().end())
                    {
                        factory = get_factory(std::move(sch), (*schema_it).value().as_string_view(), options, schema_store_ptr, resolve_funcs, vocabulary);
                        found = true;
                    }
                }
            }
            
            return factory;
        }
    };

    template <typename Json>
    Json meta_resolver(const jsoncons::uri& uri)
    {
        if (uri.base() == jsoncons::uri{schema_version::draft202012()})
        {
            return jsoncons::jsonschema::draft202012::schema_draft202012<Json>::get_schema();
        }
        else if (uri.base() == jsoncons::uri{schema_version::draft201909()})
        {
            return jsoncons::jsonschema::draft201909::schema_draft201909<Json>::get_schema();
        }
        else if (uri.base() == jsoncons::uri{schema_version::draft7()})
        {
            return jsoncons::jsonschema::draft7::schema_draft7<Json>::get_schema();
        }
        else if (uri.base() == jsoncons::uri{schema_version::draft6()})
        {
            return jsoncons::jsonschema::draft6::schema_draft6<Json>::get_schema();
        }
        else if (uri.base() == jsoncons::uri{schema_version::draft4()})
        {
            return jsoncons::jsonschema::draft4::schema_draft4<Json>::get_schema();
        }
        else
        {
            return Json::null();
        }
    }

    template <typename Json,typename SchemaResolver>
    typename std::enable_if<extension_traits::is_unary_function_object_exact<SchemaResolver,Json,jsoncons::uri>::value,json_schema<Json>>::type
    make_json_schema(Json sch, const std::string& retrieval_uri, const SchemaResolver& resolver, 
        evaluation_options options = evaluation_options{})
    {
        using schema_store_type = std::map<jsoncons::uri, schema_validator<Json>*>;
        schema_store_type schema_store; 
        validator_factory_factory<Json> factory_factory{};
        
        std::unordered_map<std::string,bool> vocabulary{};
        std::vector<resolve_uri_type<Json>> resolve_funcs = {{meta_resolver<Json>, resolver}};
        auto schema_validator_factory_base = factory_factory(std::move(sch), options, &schema_store, resolve_funcs, vocabulary);

        schema_validator_factory_base->build_schema(retrieval_uri);
        return json_schema<Json>(schema_validator_factory_base->get_schema_validator());
    }

    template <typename Json>
    json_schema<Json> make_json_schema(Json sch, const std::string& retrieval_uri, 
        evaluation_options options = evaluation_options{})
    {
        using schema_store_type = std::map<jsoncons::uri, schema_validator<Json>*>;
        schema_store_type schema_store; 
        validator_factory_factory<Json> factory_factory{};

        std::unordered_map<std::string,bool> vocabulary{};
        std::vector<resolve_uri_type<Json>> resolve_funcs = {{meta_resolver<Json>}};
        auto schema_validator_factory_base = factory_factory(std::move(sch), options, &schema_store, resolve_funcs, vocabulary);

        schema_validator_factory_base->build_schema(retrieval_uri);
        return json_schema<Json>(schema_validator_factory_base->get_schema_validator());
    }

    template <typename Json,typename SchemaResolver>
    typename std::enable_if<extension_traits::is_unary_function_object_exact<SchemaResolver,Json,jsoncons::uri>::value,json_schema<Json>>::type
    make_json_schema(Json sch, const SchemaResolver& resolver, 
        evaluation_options options = evaluation_options{})
    {
        using schema_store_type = std::map<jsoncons::uri, schema_validator<Json>*>;
        schema_store_type schema_store; 
        validator_factory_factory<Json> factory_factory{};

        std::unordered_map<std::string,bool> vocabulary{};
        std::vector<resolve_uri_type<Json>> resolve_funcs = {{meta_resolver<Json>, resolver}};
        auto schema_validator_factory_base = factory_factory(std::move(sch), options, &schema_store, resolve_funcs, vocabulary);

        schema_validator_factory_base->build_schema();
        return json_schema<Json>(schema_validator_factory_base->get_schema_validator());
    }

    template <typename Json>
    json_schema<Json> make_json_schema(Json sch, 
        evaluation_options options = evaluation_options{})
    {
        using schema_store_type = std::map<jsoncons::uri, schema_validator<Json>*>;
        schema_store_type schema_store; 
        validator_factory_factory<Json> factory_factory{};

        std::unordered_map<std::string,bool> vocabulary{};
        std::vector<resolve_uri_type<Json>> resolve_funcs = {{meta_resolver<Json>}};
        auto schema_validator_factory_base = factory_factory(std::move(sch), options, &schema_store, resolve_funcs, vocabulary);

        schema_validator_factory_base->build_schema();
        return json_schema<Json>(schema_validator_factory_base->get_schema_validator());
    }

} // namespace jsonschema
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONSCHEMA_SCHEMA_HPP
