// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_MERGEPATCH_MERGEPATCH_HPP
#define JSONCONS_EXT_MERGEPATCH_MERGEPATCH_HPP

#include <jsoncons/tag_type.hpp>

namespace jsoncons { 
namespace mergepatch {

    template <typename Json>
    Json from_diff(const Json& source, const Json& target)
    {
        if (!source.is_object() || !target.is_object())
        {
            return target;
        }
        Json result(json_object_arg);

        for (const auto& member : source.object_range())
        {
            auto it = target.find(member.key());
            if (it != target.object_range().end())
            {
                if (member.value() != (*it).value())
                {
                    result.try_emplace(member.key(), from_diff(member.value(), (*it).value()));
                }
            }
            else
            {
                result.try_emplace(member.key(), Json::null());
            }
        }

        for (const auto& member : target.object_range())
        {
            auto it = source.find(member.key());
            if (it == source.object_range().end())
            {
                result.try_emplace(member.key(), member.value());
            }
        }

        return result;
    }

    namespace detail {
        template <typename Json>
        Json apply_merge_patch_(Json& target, const Json& patch)
        {
            if (patch.is_object())
            {
                if (!target.is_object())
                {
                    target = Json(json_object_arg);
                }
                for (auto& member : patch.object_range())
                {
                    auto it = target.find(member.key());
                    if (it != target.object_range().end())
                    {
                        Json item = (*it).value();
                        target.erase(it);
                        if (!member.value().is_null())
                        {
                            target.try_emplace(member.key(), apply_merge_patch_(item, member.value()));
                        }
                    }
                    else if (!member.value().is_null())
                    {
                        Json item(json_object_arg);
                        target.try_emplace(member.key(), apply_merge_patch_(item, member.value()));
                    }
                }
                return target;
            }
            else
            {
                return patch;
            }
        }
    } // namespace detail

    template <typename Json>
    void apply_merge_patch(Json& target, const Json& patch)
    {
        target = detail::apply_merge_patch_(target, patch);
    }

} // namespace mergepatch
} // namespace jsoncons

#endif // JSONCONS_EXT_MERGEPATCH_MERGEPATCH_HPP
