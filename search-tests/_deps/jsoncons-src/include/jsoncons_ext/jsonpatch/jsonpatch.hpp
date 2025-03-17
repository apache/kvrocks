// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JSONPATCH_JSONPATCH_HPP
#define JSONCONS_EXT_JSONPATCH_JSONPATCH_HPP

#include <algorithm> // std::min
#include <cstddef>
#include <string>
#include <system_error>
#include <utility> // std::move
#include <vector> 

#include <jsoncons/tag_type.hpp>

#include <jsoncons_ext/jsonpatch/jsonpatch_error.hpp>
#include <jsoncons_ext/jsonpointer/jsonpointer.hpp>

namespace jsoncons { namespace jsonpatch {

namespace detail {

    template <typename CharT>
    struct jsonpatch_names
    {
        static std::basic_string<CharT> test_name()
        {
            static std::basic_string<CharT> name{'t','e','s','t'};
            return name;
        }
        static std::basic_string<CharT> add_name()
        {
            static std::basic_string<CharT> name{'a','d','d'};
            return name;
        }
        static std::basic_string<CharT> remove_name()
        {
            static std::basic_string<CharT> name{'r','e','m','o','v','e'};
            return name;
        }
        static std::basic_string<CharT> replace_name()
        {
            static std::basic_string<CharT> name{'r','e','p','l','a','c','e'};
            return name;
        }
        static std::basic_string<CharT> move_name()
        {
            static std::basic_string<CharT> name{'m','o','v','e'};
            return name;
        }
        static std::basic_string<CharT> copy_name()
        {
            static std::basic_string<CharT> name{'c','o','p','y'};
            return name;
        }
        static std::basic_string<CharT> op_name()
        {
            static std::basic_string<CharT> name{'o','p'};
            return name;
        }
        static std::basic_string<CharT> path_name()
        {
            static std::basic_string<CharT> name{'p','a','t','h'};
            return name;
        }
        static std::basic_string<CharT> from_name()
        {
            static std::basic_string<CharT> name{'f','r','o','m'};
            return name;
        }
        static std::basic_string<CharT> value_name()
        {
            static std::basic_string<CharT> name{'v','a','l','u','e'};
            return name;
        }
        static std::basic_string<CharT> dash_name()
        {
            static std::basic_string<CharT> name{'-'};
            return name;
        }
    };

    template <typename Json>
    jsonpointer::basic_json_pointer<typename Json::char_type> definite_path(const Json& root, jsonpointer::basic_json_pointer<typename Json::char_type>& location)
    {
        using char_type = typename Json::char_type;
        using string_type = std::basic_string<char_type>;

        auto rit = location.rbegin();
        if (rit == location.rend())
        {
            return location;
        }

        if (*rit != jsonpatch_names<char_type>::dash_name())
        {
            return location;
        }

        std::vector<string_type> tokens;
        for (auto it = location.begin(); it != location.rbegin().base()-1; ++it)
        {
            tokens.push_back(*it);
        }
        jsonpointer::basic_json_pointer<char_type> pointer(tokens);

        std::error_code ec;

        Json val = jsonpointer::get(root, pointer, ec);
        if (ec || !val.is_array())
        {
            return location;
        }
        string_type last_token;
        jsoncons::detail::from_integer(val.size(), last_token);
        tokens.emplace_back(std::move(last_token));

        return jsonpointer::basic_json_pointer<char_type>(std::move(tokens));
    }

    enum class op_type {add,remove,replace};
    enum class state_type {begin,abort,commit};

    template <typename Json>
    struct operation_unwinder
    {
        using char_type = typename Json::char_type;
        using string_type = std::basic_string<char_type>;
        using json_pointer_type = jsonpointer::basic_json_pointer<char_type>;

        struct entry
        {
            op_type op;
            json_pointer_type path;
            Json value;

            entry(op_type Op, const json_pointer_type& Path, const Json& Value)
                : op(Op), path(Path), value(Value)
            {
            }

            entry(const entry&) = default;

            entry(entry&&) = default;

            entry& operator=(const entry&) = default;

            entry& operator=(entry&&) = default;
        };

        Json& target;
        state_type state;
        std::vector<entry> stack;

        operation_unwinder(Json& j)
            : target(j), state(state_type::begin)
        {
        }

        ~operation_unwinder() noexcept
        {
            std::error_code ec;
            if (state != state_type::commit)
            {
                for (auto it = stack.rbegin(); it != stack.rend(); ++it)
                {
                    if ((*it).op == op_type::add)
                    {
                        jsonpointer::add(target,(*it).path,(*it).value,ec);
                        if (ec)
                        {
                            //std::cout << "add: " << (*it).path << '\n';
                            break;
                        }
                    }
                    else if ((*it).op == op_type::remove)
                    {
                        jsonpointer::remove(target,(*it).path,ec);
                        if (ec)
                        {
                            //std::cout << "remove: " << (*it).path << '\n';
                            break;
                        }
                    }
                    else if ((*it).op == op_type::replace)
                    {
                        jsonpointer::replace(target,(*it).path,(*it).value,ec);
                        if (ec)
                        {
                            //std::cout << "replace: " << (*it).path << '\n';
                            break;
                        }
                    }
                }
            }
        }
    };

    template <typename Json>
    Json from_diff(const Json& source, const Json& target, const typename Json::string_view_type& path)
    {
        using char_type = typename Json::char_type;

        Json result = typename Json::array();

        if (source == target)
        {
            return result;
        }

        if (source.is_array() && target.is_array())
        {
            std::size_t common = (std::min)(source.size(),target.size());
            for (std::size_t i = 0; i < common; ++i)
            {
                std::basic_string<char_type> ss(path); 
                ss.push_back('/');
                jsoncons::detail::from_integer(i,ss);
                auto temp_diff = from_diff(source[i],target[i],ss);
                result.insert(result.array_range().end(),temp_diff.array_range().begin(),temp_diff.array_range().end());
            }
            // Element in source, not in target - remove
            for (std::size_t i = source.size(); i-- > target.size();)
            {
                std::basic_string<char_type> ss(path); 
                ss.push_back('/');
                jsoncons::detail::from_integer(i,ss);
                Json val(json_object_arg);
                val.insert_or_assign(jsonpatch_names<char_type>::op_name(), jsonpatch_names<char_type>::remove_name());
                val.insert_or_assign(jsonpatch_names<char_type>::path_name(), ss);
                result.push_back(std::move(val));
            }
            // Element in target, not in source - add, 
            // Fix contributed by Alexander rog13
            for (std::size_t i = source.size(); i < target.size(); ++i)
            {
                const auto& a = target[i];
                std::basic_string<char_type> ss(path); 
                ss.push_back('/');
                jsoncons::detail::from_integer(i,ss);
                Json val(json_object_arg);
                val.insert_or_assign(jsonpatch_names<char_type>::op_name(), jsonpatch_names<char_type>::add_name());
                val.insert_or_assign(jsonpatch_names<char_type>::path_name(), ss);
                val.insert_or_assign(jsonpatch_names<char_type>::value_name(), a);
                result.push_back(std::move(val));
            }
        }
        else if (source.is_object() && target.is_object())
        {
            for (const auto& a : source.object_range())
            {
                std::basic_string<char_type> ss(path);
                ss.push_back('/'); 
                jsonpointer::escape(a.key(),ss);
                auto it = target.find(a.key());
                if (it != target.object_range().end())
                {
                    auto temp_diff = from_diff(a.value(),(*it).value(),ss);
                    result.insert(result.array_range().end(),temp_diff.array_range().begin(),temp_diff.array_range().end());
                }
                else
                {
                    Json val(json_object_arg);
                    val.insert_or_assign(jsonpatch_names<char_type>::op_name(), jsonpatch_names<char_type>::remove_name());
                    val.insert_or_assign(jsonpatch_names<char_type>::path_name(), ss);
                    result.push_back(std::move(val));
                }
            }
            for (const auto& a : target.object_range())
            {
                auto it = source.find(a.key());
                if (it == source.object_range().end())
                {
                    std::basic_string<char_type> ss(path); 
                    ss.push_back('/');
                    jsonpointer::escape(a.key(),ss);
                    Json val(json_object_arg);
                    val.insert_or_assign(jsonpatch_names<char_type>::op_name(), jsonpatch_names<char_type>::add_name());
                    val.insert_or_assign(jsonpatch_names<char_type>::path_name(), ss);
                    val.insert_or_assign(jsonpatch_names<char_type>::value_name(), a.value());
                    result.push_back(std::move(val));
                }
            }
        }
        else
        {
            Json val(json_object_arg);
            val.insert_or_assign(jsonpatch_names<char_type>::op_name(), jsonpatch_names<char_type>::replace_name());
            val.insert_or_assign(jsonpatch_names<char_type>::path_name(), path);
            val.insert_or_assign(jsonpatch_names<char_type>::value_name(), target);
            result.push_back(std::move(val));
        }

        return result;
    }

} // namespace detail

template <typename Json>
void apply_patch(Json& target, const Json& patch, std::error_code& ec)
{
    if (!patch.is_array())
    {
        ec = jsonpatch_errc::invalid_patch;
        return;
    }

    using char_type = typename Json::char_type;
    using string_type = std::basic_string<char_type>;
    using json_pointer_type = jsonpointer::basic_json_pointer<char_type>;

   jsoncons::jsonpatch::detail::operation_unwinder<Json> unwinder(target);
   std::error_code local_ec;

    // Validate  
     
    for (const auto& operation : patch.array_range())
    {
        unwinder.state =jsoncons::jsonpatch::detail::state_type::begin;

        auto it_op = operation.find(detail::jsonpatch_names<char_type>::op_name());
        if (it_op == operation.object_range().end())
        {
            ec = jsonpatch_errc::invalid_patch;
            unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
            return;
        }
        string_type op = it_op->value().template as<string_type>();

        auto it_path = operation.find(detail::jsonpatch_names<char_type>::path_name());
        if (it_path == operation.object_range().end())
        {
            ec = jsonpatch_errc::invalid_patch;
            unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
            return;
        }
        string_type path = it_path->value().template as<string_type>();
        auto location = json_pointer_type::parse(path, local_ec);
        if (local_ec)
        {
            ec = jsonpatch_errc::invalid_patch;
            unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
            return;
        }

        if (op ==jsoncons::jsonpatch::detail::jsonpatch_names<char_type>::test_name())
        {
            Json val = jsonpointer::get(target,location,local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::test_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            auto it_value = operation.find(detail::jsonpatch_names<char_type>::value_name());
            if (it_value == operation.object_range().end())
            {
                ec = jsonpatch_errc::invalid_patch;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            if (val != it_value->value())
            {
                ec = jsonpatch_errc::test_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
        }
        else if (op ==jsoncons::jsonpatch::detail::jsonpatch_names<char_type>::add_name())
        {
            auto it_value = operation.find(detail::jsonpatch_names<char_type>::value_name());
            if (it_value == operation.object_range().end())
            {
                ec = jsonpatch_errc::invalid_patch;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            Json val = it_value->value();
            auto npath = jsonpatch::detail::definite_path(target,location);

            std::error_code insert_ec;
            jsonpointer::add_if_absent(target,npath,val,insert_ec); // try insert without replace
            if (insert_ec) // try a replace
            {
                std::error_code select_ec;
                Json orig_val = jsonpointer::get(target,npath,select_ec);
                if (select_ec) // shouldn't happen
                {
                    ec = jsonpatch_errc::add_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    return;
                }
                std::error_code replace_ec;
                jsonpointer::replace(target,npath,val,replace_ec);
                if (replace_ec)
                {
                    ec = jsonpatch_errc::add_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    return;
                }
                unwinder.stack.emplace_back(detail::op_type::replace,npath,orig_val);
            }
            else // insert without replace succeeded
            {
                unwinder.stack.emplace_back(detail::op_type::remove,npath,Json::null());
            }
        }
        else if (op ==jsoncons::jsonpatch::detail::jsonpatch_names<char_type>::remove_name())
        {
            Json val = jsonpointer::get(target,location,local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::remove_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            jsonpointer::remove(target,location,local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::remove_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            unwinder.stack.emplace_back(detail::op_type::add, location, val);
        }
        else if (op ==jsoncons::jsonpatch::detail::jsonpatch_names<char_type>::replace_name())
        {
            Json val = jsonpointer::get(target,location,local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::replace_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            auto it_value = operation.find(detail::jsonpatch_names<char_type>::value_name());
            if (it_value == operation.object_range().end())
            {
                ec = jsonpatch_errc::invalid_patch;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            jsonpointer::replace(target, location, it_value->value(), local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::replace_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            unwinder.stack.emplace_back(detail::op_type::replace,location,val);
        }
        else if (op ==jsoncons::jsonpatch::detail::jsonpatch_names<char_type>::move_name())
        {
            auto it_from = operation.find(detail::jsonpatch_names<char_type>::from_name());
            if (it_from == operation.object_range().end())
            {
                ec = jsonpatch_errc::invalid_patch;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            string_type from = it_from->value().as_string();
            auto from_pointer = json_pointer_type::parse(from, local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::move_failed;
                unwinder.state = jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }

            Json val = jsonpointer::get(target, from_pointer, local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::move_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            jsonpointer::remove(target, from_pointer, local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::move_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            unwinder.stack.emplace_back(detail::op_type::add, from_pointer, val);
            // add
            std::error_code insert_ec;
            auto npath = jsonpatch::detail::definite_path(target,location);
            jsonpointer::add_if_absent(target,npath,val,insert_ec); // try insert without replace
            if (insert_ec) // try a replace
            {
                std::error_code select_ec;
                Json orig_val = jsonpointer::get(target,npath,select_ec);
                if (select_ec) // shouldn't happen
                {
                    ec = jsonpatch_errc::copy_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    return;
                }
                std::error_code replace_ec;
                jsonpointer::replace(target, npath, val, replace_ec);
                if (replace_ec)
                {
                    ec = jsonpatch_errc::copy_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    return;
                }
                unwinder.stack.emplace_back(jsoncons::jsonpatch::detail::op_type::replace,npath,orig_val);
            }
            else
            {
                unwinder.stack.emplace_back(detail::op_type::remove,npath,Json::null());
            }
        }
        else if (op ==jsoncons::jsonpatch::detail::jsonpatch_names<char_type>::copy_name())
        {
            auto it_from = operation.find(detail::jsonpatch_names<char_type>::from_name());
            if (it_from == operation.object_range().end())
            {
                ec = jsonpatch_errc::invalid_patch;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            string_type from = it_from->value().as_string();
            Json val = jsonpointer::get(target,from,local_ec);
            if (local_ec)
            {
                ec = jsonpatch_errc::copy_failed;
                unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                return;
            }
            // add
            auto npath = jsonpatch::detail::definite_path(target,location);
            std::error_code insert_ec;
            jsonpointer::add_if_absent(target,npath,val,insert_ec); // try insert without replace
            if (insert_ec) // Failed, try a replace
            {
                std::error_code select_ec;
                Json orig_val = jsonpointer::get(target,npath, select_ec);
                if (select_ec) // shouldn't happen
                {
                    ec = jsonpatch_errc::copy_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    return;
                }
                std::error_code replace_ec;
                jsonpointer::replace(target, npath, val,replace_ec);
                if (replace_ec)
                {
                    ec = jsonpatch_errc::copy_failed;
                    unwinder.state =jsoncons::jsonpatch::detail::state_type::abort;
                    return;
                }
                unwinder.stack.emplace_back(jsoncons::jsonpatch::detail::op_type::replace,npath,orig_val);
            }
            else
            {
                unwinder.stack.emplace_back(detail::op_type::remove,npath,Json::null());
            }
        }
    }
    if (unwinder.state ==jsoncons::jsonpatch::detail::state_type::begin)
    {
        unwinder.state =jsoncons::jsonpatch::detail::state_type::commit;
    }
}

template <typename Json>
Json from_diff(const Json& source, const Json& target)
{
    std::basic_string<typename Json::char_type> path;
    return jsoncons::jsonpatch::detail::from_diff(source, target, path);
}

template <typename Json>
void apply_patch(Json& target, const Json& patch)
{
    std::error_code ec;
    apply_patch(target, patch, ec);
    if (ec)
    {
        JSONCONS_THROW(jsonpatch_error(ec));
    }
}

} // namespace jsonpatch
} // namespace jsoncons

#endif // JSONCONS_EXT_JSONPATCH_JSONPATCH_HPP
