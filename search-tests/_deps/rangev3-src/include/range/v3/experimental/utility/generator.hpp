/// \file
// Range v3 library
//
//  Copyright Casey Carter 2017
//
//  Use, modification and distribution is subject to the
//  Boost Software License, Version 1.0. (See accompanying
//  file LICENSE_1_0.txt or copy at
//  http://www.boost.org/LICENSE_1_0.txt)
//
// Project home: https://github.com/ericniebler/range-v3
//
#ifndef RANGES_V3_EXPERIMENTAL_UTILITY_GENERATOR_HPP
#define RANGES_V3_EXPERIMENTAL_UTILITY_GENERATOR_HPP

#include <range/v3/detail/config.hpp>
#if RANGES_CXX_COROUTINES >= RANGES_CXX_COROUTINES_TS1
#include <atomic>
#include <cstddef>
#include <exception>
#include RANGES_COROUTINES_HEADER
#include <utility>

#include <meta/meta.hpp>

#include <concepts/concepts.hpp>

#include <range/v3/range_fwd.hpp>

#include <range/v3/iterator/default_sentinel.hpp>
#include <range/v3/range/traits.hpp>
#include <range/v3/utility/box.hpp>
#include <range/v3/utility/semiregular_box.hpp>
#include <range/v3/utility/swap.hpp>
#include <range/v3/view/all.hpp>
#include <range/v3/view/facade.hpp>

#if defined(_MSC_VER) && !defined(RANGES_SILENCE_COROUTINE_WARNING)
#ifdef __clang__
#pragma message(                                                 \
    "DANGER: clang doesn't (yet?) grok the MSVC coroutine ABI. " \
    "Use at your own risk. "                                     \
    "(RANGES_SILENCE_COROUTINE_WARNING will silence this message.)")
#elif defined RANGES_WORKAROUND_MSVC_835948
#pragma message(                                                                 \
    "DANGER: ranges::experimental::generator is fine, but this "                 \
    "version of MSVC likely miscompiles ranges::experimental::sized_generator. " \
    "Use the latter at your own risk. "                                          \
    "(RANGES_SILENCE_COROUTINE_WARNING will silence this message.)")
#endif
#endif // RANGES_SILENCE_COROUTINE_WARNINGS

#include <range/v3/detail/prologue.hpp>

namespace ranges
{
    /// \addtogroup group-view
    /// @{
    namespace experimental
    {
        // The type of size() for a sized_generator
        using generator_size_t = std::size_t;

        // Type upon which to co_await to set the size of a sized_generator
        enum struct generator_size : generator_size_t
        {
            invalid = ~generator_size_t(0)
        };

        template<typename Promise = void>
        struct RANGES_EMPTY_BASES coroutine_owner;

        class enable_coroutine_owner
        {
            template<class>
            friend struct coroutine_owner;
            std::atomic<unsigned int> refcount_{1};
        };
    } // namespace experimental

    /// \cond
    namespace detail
    {
        inline void resume(RANGES_COROUTINES_NS::coroutine_handle<> coro)
        {
            // Pre: coro refers to a suspended coroutine.
            RANGES_EXPECT(coro);
            RANGES_EXPECT(!coro.done());
            coro.resume();
        }

        namespace coroutine_owner_
        {
            struct adl_hook
            {};

            template<typename Promise>
            void swap(experimental::coroutine_owner<Promise> & x,
                      experimental::coroutine_owner<Promise> & y) noexcept
            {
                x.swap(y);
            }
        } // namespace coroutine_owner_
    }     // namespace detail
    /// \endcond

    namespace experimental
    {
        // An owning coroutine_handle
        template<typename Promise>
        struct RANGES_EMPTY_BASES coroutine_owner
          : private RANGES_COROUTINES_NS::coroutine_handle<Promise>
          , private detail::coroutine_owner_::adl_hook
        {
            CPP_assert(derived_from<Promise, enable_coroutine_owner>);
            using base_t = RANGES_COROUTINES_NS::coroutine_handle<Promise>;

            using base_t::operator bool;
            using base_t::done;
            using base_t::promise;

            coroutine_owner() = default;
            constexpr explicit coroutine_owner(base_t coro) noexcept
              : base_t(coro)
            {}
            coroutine_owner(coroutine_owner && that) noexcept
              : base_t(ranges::exchange(that.base(), {}))
              , copied_(that.copied_.load(std::memory_order_relaxed))
            {}
            coroutine_owner(coroutine_owner const & that) noexcept
              : base_t(that.handle())
              , copied_(that.handle() != nullptr)
            {
                if(*this)
                {
                    that.copied_.store(true, std::memory_order_relaxed);
                    base().promise().refcount_.fetch_add(1, std::memory_order_relaxed);
                }
            }
            ~coroutine_owner()
            {
                if(base() && (!copied_.load(std::memory_order_relaxed) ||
                              1 == base().promise().refcount_.fetch_sub(
                                       1, std::memory_order_acq_rel)))
                    base().destroy();
            }
            coroutine_owner & operator=(coroutine_owner that) noexcept
            {
                swap(that);
                return *this;
            }
            void resume()
            {
                detail::resume(handle());
            }
            void operator()()
            {
                detail::resume(handle());
            }
            void swap(coroutine_owner & that) noexcept
            {
                bool tmp = copied_.load(std::memory_order_relaxed);
                copied_.store(that.copied_.load(std::memory_order_relaxed),
                              std::memory_order_relaxed);
                that.copied_.store(tmp, std::memory_order_relaxed);
                std::swap(base(), that.base());
            }
            base_t handle() const noexcept
            {
                return *this;
            }

        private:
            mutable std::atomic<bool> copied_{false};

            base_t & base() noexcept
            {
                return *this;
            }
        };
    } // namespace experimental

    /// \cond
    namespace detail
    {
        template<typename Reference>
        struct generator_promise : experimental::enable_coroutine_owner
        {
            std::exception_ptr except_ = nullptr;

            CPP_assert(std::is_reference<Reference>::value ||
                       copy_constructible<Reference>);

            generator_promise * get_return_object() noexcept
            {
                return this;
            }
            RANGES_COROUTINES_NS::suspend_always initial_suspend() const noexcept
            {
                return {};
            }
            RANGES_COROUTINES_NS::suspend_always final_suspend() const noexcept
            {
                return {};
            }
            void return_void() const noexcept
            {}
            void unhandled_exception() noexcept
            {
                except_ = std::current_exception();
                RANGES_EXPECT(except_);
            }
            template(typename Arg)(
                requires convertible_to<Arg, Reference> AND
                        std::is_assignable<semiregular_box_t<Reference> &, Arg>::value) //
            RANGES_COROUTINES_NS::suspend_always yield_value(Arg && arg) noexcept(
                std::is_nothrow_assignable<semiregular_box_t<Reference> &, Arg>::value)
            {
                ref_ = std::forward<Arg>(arg);
                return {};
            }
            RANGES_COROUTINES_NS::suspend_never await_transform(
                experimental::generator_size) const noexcept
            {
                RANGES_ENSURE_MSG(false,
                                  "Invalid size request for a non-sized generator");
                return {};
            }
            meta::if_<std::is_reference<Reference>, Reference, Reference const &> read()
                const noexcept
            {
                return ref_;
            }

        private:
            semiregular_box_t<Reference> ref_;
        };

        template<typename Reference>
        struct sized_generator_promise : generator_promise<Reference>
        {
            sized_generator_promise * get_return_object() noexcept
            {
                return this;
            }
            RANGES_COROUTINES_NS::suspend_never initial_suspend() const noexcept
            {
                // sized_generator doesn't suspend at its initial suspend point because...
                return {};
            }
            RANGES_COROUTINES_NS::suspend_always await_transform(
                experimental::generator_size size) noexcept
            {
                // ...we need the coroutine set the size of the range first by
                // co_awaiting on a generator_size.
                size_ = size;
                return {};
            }
            experimental::generator_size_t size() const noexcept
            {
                RANGES_EXPECT(size_ != experimental::generator_size::invalid);
                return static_cast<experimental::generator_size_t>(size_);
            }

        private:
            experimental::generator_size size_ = experimental::generator_size::invalid;
        };
    } // namespace detail
    /// \endcond

    namespace experimental
    {
        template<typename Reference, typename Value = uncvref_t<Reference>>
        struct sized_generator;

        template<typename Reference, typename Value = uncvref_t<Reference>>
        struct generator : view_facade<generator<Reference, Value>>
        {
            using promise_type = detail::generator_promise<Reference>;

            constexpr generator() noexcept = default;
            generator(promise_type * p)
              : coro_{handle::from_promise(*p)}
            {
                RANGES_EXPECT(coro_);
            }

        private:
            friend range_access;
            friend struct sized_generator<Reference, Value>;
            using handle = RANGES_COROUTINES_NS::coroutine_handle<promise_type>;
            coroutine_owner<promise_type> coro_;

            struct cursor
            {
                using value_type = Value;

                cursor() = default;
                constexpr explicit cursor(handle coro) noexcept
                  : coro_{coro}
                {}
                bool equal(default_sentinel_t) const
                {
                    RANGES_EXPECT(coro_);
                    if(coro_.done())
                    {
                        auto & e = coro_.promise().except_;
                        if(e)
                            std::rethrow_exception(std::move(e));
                        return true;
                    }
                    return false;
                }
                void next()
                {
                    detail::resume(coro_);
                }
                Reference read() const
                {
                    RANGES_EXPECT(coro_);
                    return coro_.promise().read();
                }

            private:
                handle coro_ = nullptr;
            };

            cursor begin_cursor()
            {
                detail::resume(coro_.handle());
                return cursor{coro_.handle()};
            }
        };

        template<typename Reference, typename Value /* = uncvref_t<Reference>*/>
        struct sized_generator : generator<Reference, Value>
        {
            using promise_type = detail::sized_generator_promise<Reference>;
            using handle = RANGES_COROUTINES_NS::coroutine_handle<promise_type>;

            constexpr sized_generator() noexcept = default;
            sized_generator(promise_type * p)
              : generator<Reference, Value>{p}
            {}
            generator_size_t size() const noexcept
            {
                return promise().size();
            }

        private:
            using generator<Reference, Value>::coro_;

            promise_type const & promise() const noexcept
            {
                RANGES_EXPECT(coro_);
                return static_cast<promise_type const &>(coro_.promise());
            }
        };
    } // namespace experimental

    /// @}
} // namespace ranges

#include <range/v3/detail/epilogue.hpp>

#endif // RANGES_CXX_COROUTINES >= RANGES_CXX_COROUTINES_TS1

#endif // RANGES_V3_EXPERIMENTAL_UTILITY_GENERATOR_HPP
