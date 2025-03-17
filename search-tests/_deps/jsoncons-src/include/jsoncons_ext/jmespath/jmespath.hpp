// Copyright 2013-2025 Daniel Parker
// Distributed under the Boost license, Version 1.0.
// (See accompanying file LICENSE_1_0.txt or copy at http://www.boost.org/LICENSE_1_0.txt)

// See https://github.com/danielaparker/jsoncons for latest version

#ifndef JSONCONS_EXT_JMESPATH_JMESPATH_HPP
#define JSONCONS_EXT_JMESPATH_JMESPATH_HPP

#include <algorithm> // std::stable_sort, std::reverse
#include <cmath> // std::abs
#include <cstddef>
#include <exception>
#include <functional> // 
#include <limits> // std::numeric_limits
#include <memory>
#include <string>
#include <system_error>
#include <type_traits> // std::is_const
#include <unordered_map> // std::unordered_map
#include <utility> // std::move
#include <vector>
#include <map>

#include <jsoncons/config/compiler_support.hpp>
#include <jsoncons/json_type.hpp>
#include <jsoncons/json_decoder.hpp>
#include <jsoncons/json_reader.hpp>
#include <jsoncons/detail/parse_number.hpp>
#include <jsoncons/tag_type.hpp>
#include <jsoncons/utility/unicode_traits.hpp>

#include <jsoncons_ext/jmespath/jmespath_error.hpp>

namespace jsoncons { 
namespace jmespath {

    enum class operator_kind
    {
        default_op, // Identifier, CurrentNode, Index, MultiSelectList, MultiSelectHash, FunctionExpression
        projection_op,
        flatten_projection_op, // FlattenProjection
        or_op,
        and_op,
        eq_op,
        ne_op,
        lt_op,
        lte_op,
        gt_op,
        gte_op,
        not_op
    }; 

    struct operator_table final
    {
        static int precedence_level(operator_kind oper)
        {
            switch (oper)
            {
                case operator_kind::projection_op:
                    return 11;
                case operator_kind::flatten_projection_op:
                    return 11;
                case operator_kind::or_op:
                    return 9;
                case operator_kind::and_op:
                    return 8;
                case operator_kind::eq_op:
                case operator_kind::ne_op:
                    return 6;
                case operator_kind::lt_op:
                case operator_kind::lte_op:
                case operator_kind::gt_op:
                case operator_kind::gte_op:
                    return 5;
                case operator_kind::not_op:
                    return 1;
                default:
                    return 1;
            }
        }

        static bool is_right_associative(operator_kind oper)
        {
            switch (oper)
            {
                case operator_kind::not_op:
                    return true;
                case operator_kind::projection_op:
                    return true;
                case operator_kind::flatten_projection_op:
                    return false;
                case operator_kind::or_op:
                case operator_kind::and_op:
                case operator_kind::eq_op:
                case operator_kind::ne_op:
                case operator_kind::lt_op:
                case operator_kind::lte_op:
                case operator_kind::gt_op:
                case operator_kind::gte_op:
                    return false;
                default:
                    return false;
            }
        }
    };

    // eval_context

    template <typename Json>
    class eval_context
    {
    public:
        using char_type = typename Json::char_type;
        using char_traits_type = typename Json::char_traits_type;
        using string_type = std::basic_string<char_type,char_traits_type>;
        using string_view_type = typename Json::string_view_type;
        using reference = typename Json::const_reference;
        using pointer = typename Json::pointer;
    public:
        std::vector<std::unique_ptr<Json>>& temp_storage_;
        std::map<std::string,const Json*> variables_;

    public:
        eval_context(std::vector<std::unique_ptr<Json>>& temp_storage)
            : temp_storage_(temp_storage)
        {
        }

        eval_context(std::vector<std::unique_ptr<Json>>& temp_storage, 
            const std::map<std::string,const Json*>& variables)
            : temp_storage_(temp_storage), variables_(variables)
        {
        }
        
        ~eval_context() noexcept = default;
        
        void set_variable(const std::string& key, const Json& value)
        {
            variables_[key] = std::addressof(value);
        }
        
        const Json& get_variable(const std::string& key, std::error_code& ec) const
        {
            auto it = variables_.find(key);
            if (it == variables_.end())
            {
                ec = jmespath_errc::undefined_variable;
                return Json::null();
            }
            return *it->second;
        }

        reference number_type_name() 
        {
            static Json number_type_name(JSONCONS_STRING_CONSTANT(char_type, "number"));

            return number_type_name;
        }

        reference boolean_type_name()
        {
            static Json boolean_type_name(JSONCONS_STRING_CONSTANT(char_type, "boolean"));

            return boolean_type_name;
        }

        reference string_type_name()
        {
            static Json string_type_name(JSONCONS_STRING_CONSTANT(char_type, "string"));

            return string_type_name;
        }

        reference object_type_name()
        {
            static Json object_type_name(JSONCONS_STRING_CONSTANT(char_type, "object"));

            return object_type_name;
        }

        reference array_type_name()
        {
            static Json array_type_name(JSONCONS_STRING_CONSTANT(char_type, "array"));

            return array_type_name;
        }

        reference null_type_name()
        {
            static Json null_type_name(JSONCONS_STRING_CONSTANT(char_type, "null"));

            return null_type_name;
        }

        reference true_value() const
        {
            static const Json true_value(true, semantic_tag::none);
            return true_value;
        }

        reference false_value() const
        {
            static const Json false_value(false, semantic_tag::none);
            return false_value;
        }

        reference null_value() const
        {
            static const Json null_value(null_type(), semantic_tag::none);
            return null_value;
        }

        template <typename... Args>
        Json* create_json(Args&& ... args)
        {
            auto temp = jsoncons::make_unique<Json>(std::forward<Args>(args)...);
            Json* ptr = temp.get();
            temp_storage_.push_back(std::move(temp));
            return ptr;
        }
    };

    // expr_base

    template <typename Json>
    class expr_base
    {
    public:
        using reference = const Json&;

        virtual ~expr_base() = default;

        virtual reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const = 0;
    };  

    template <typename Json>
    class expr_wrapper : public expr_base<Json>
    {
    public:
        using reference = const Json&;
        using pointer = const Json*;
    private:
        const expr_base<Json>* expr_;
    public:
        expr_wrapper()
            : expr_(nullptr)
        {
        }
        expr_wrapper(const expr_base<Json>& expr)
            : expr_(std::addressof(expr))
        {
        }
        expr_wrapper(const expr_wrapper& other)
            : expr_(other.expr_)
        {
        }
        
        expr_wrapper& operator=(const expr_wrapper& other)
        {
            expr_ = other.expr_;
            return *this;
        }

        reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const final
        {
            return *context.create_json(deep_copy(expr_->evaluate(val, context, ec)));
        }
    };

    // expr_base_impl
    template <typename Json>
    class expr_base_impl : public expr_base<Json>
    {
    public:
        using reference = const Json&;
    private:
        std::size_t precedence_level_{0};
        bool is_right_associative_;
        bool is_projection_;
    public:
        expr_base_impl(operator_kind oper, bool is_projection)
            : precedence_level_(operator_table::precedence_level(oper)), 
              is_right_associative_(operator_table::is_right_associative(oper)), 
              is_projection_(is_projection)
        {
        }
        
        ~expr_base_impl() = default;

        std::size_t precedence_level() const
        {
            return precedence_level_;
        }

        bool is_right_associative() const
        {
            return is_right_associative_;
        }

        bool is_projection() const 
        {
            return is_projection_;
        }

        virtual void add_expression(expr_base_impl* expressions) = 0;

        virtual std::string to_string(std::size_t = 0) const
        {
            return std::string("to_string not implemented");
        }
    };  

    // parameter

    enum class parameter_kind{value, expression};

    template <typename Json>
    class parameter
    {
    public:
        using reference = const Json&;
        using expression_type = expr_base<Json>;
    private:
        parameter_kind type_;
    public:
        union
        {
            const expression_type* expression_;
            const Json* value_;
        };

    public:

        parameter(const parameter<Json>& other) noexcept
            : type_(other.type_)
        {
            switch (type_)
            {
                case parameter_kind::expression:
                    expression_ = other.expression_;
                    break;
                case parameter_kind::value:
                    value_ = other.value_;
                    break;
                default:
                    break;
            }
        }

        parameter(reference value) noexcept
            : type_(parameter_kind::value), value_(std::addressof(value))
        {
        }

        parameter(const expression_type& expression) noexcept
            : type_(parameter_kind::expression), expression_(std::addressof(expression))
        {
        }

        parameter& operator=(const parameter& other)
        {
            if (&other != this)
            {
                type_ = other.type_;
                switch (type_)
                {
                    case parameter_kind::expression:
                        expression_ = other.expression_;
                        break;
                    case parameter_kind::value:
                        value_ = other.value_;
                        break;
                    default:
                        break;
                }
            }
            return *this;
        }

        bool is_value() const
        {
            return type_ == parameter_kind::value;
        }

        bool is_expression() const
        {
            return type_ == parameter_kind::expression;
        }

        const Json& value() const
        {
            return *value_;
        }

        const expression_type& expression() const
        {
            return *expression_;
        }
    };

    template <typename Json>
    class custom_function
    {
    public:
        using value_type = Json;
        using reference = const Json&;
        using char_type = typename Json::char_type;
        using parameter_type = parameter<Json>;
        using custom_function_type = std::function<Json(jsoncons::span<const parameter_type>, 
            eval_context<Json>&, std::error_code& ec)>;
        using string_type = typename Json::string_type;

        string_type function_name_;
        optional<std::size_t> arity_;
        custom_function_type f_;

        custom_function(const string_type& function_name,
                        const optional<std::size_t>& arity,
                        const custom_function_type& f)
            : function_name_(function_name),
              arity_(arity),
              f_(f)
        {
        }

        custom_function(string_type&& function_name,
                        optional<std::size_t>&& arity,
                        custom_function_type&& f)
            : function_name_(std::move(function_name)),
              arity_(std::move(arity)),
              f_(std::move(f))
        {
        }

        custom_function(const custom_function&) = default;

        custom_function(custom_function&&) = default;

        const string_type& name() const 
        {
            return function_name_;
        }

        optional<std::size_t> arity() const 
        {
            return arity_;
        }

        const custom_function_type& function() const 
        {
            return f_;
        }
    };

    // function_base
    
    template <typename Json>
    class function_base
    {
    public:
        using reference = const Json&;
        using parameter_type = parameter<Json>;
    private:
        jsoncons::optional<std::size_t> arg_count_;
    public:
        function_base(jsoncons::optional<std::size_t> arg_count)
            : arg_count_(arg_count)
        {
        }

        jsoncons::optional<std::size_t> arity() const
        {
            return arg_count_;
        }

        virtual ~function_base() = default;

        virtual reference evaluate(const std::vector<parameter_type>& params, eval_context<Json>& context, 
            std::error_code& ec) const = 0;

        virtual bool is_custom() const
        {
            return false;
        }
        
        virtual std::string to_string(std::size_t = 0) const
        {
            return std::string("to_string not implemented");
        }
    };  

    template <typename Json>
    class function_wrapper : public function_base<Json>
    {
    public:
        using value_type = Json;
        using reference = const Json&;
        using parameter_type = parameter<Json>;
        using string_view_type = typename Json::string_view_type;
        using custom_function_type = std::function<Json(jsoncons::span<const parameter_type>, 
            eval_context<Json>&, std::error_code& ec)>;
    private:
        custom_function_type f_;
    public:
        function_wrapper(jsoncons::optional<std::size_t> arity, const custom_function_type& f)
            : function_base<Json>(arity), f_(f)
        {
        }
        
        bool is_custom() const final
        {
            return true;
        }

        reference evaluate(const std::vector<parameter_type>& params, 
            eval_context<Json>& context,
            std::error_code& ec) const override
        {
            auto val = f_(params, context, ec);
            auto ptr = context.create_json(std::move(val));
            return *ptr;
        }
    };

    template <typename Json>
    class custom_functions
    {
        using char_type = typename Json::char_type;
        using string_type = typename Json::string_type;
        using value_type = Json;
        using reference = const Json&;
        using parameter_type = parameter<Json>;
        using custom_function_type = std::function<Json(jsoncons::span<const parameter_type>, eval_context<Json>& context, 
            std::error_code& ec)>;
        using const_iterator = typename std::vector<custom_function<Json>>::const_iterator;

        std::vector<custom_function<Json>> functions_;
    public:
        void register_function(const string_type& name,
            jsoncons::optional<std::size_t> arity,
            const custom_function_type& f)
        {
            functions_.emplace_back(name, arity, f);
        }

        const_iterator begin() const
        {
            return functions_.begin();
        }

        const_iterator end() const
        {
            return functions_.end();
        }
    };

namespace detail {

    template <typename Json>
    class unary_operator
    {
    public:
        using reference = typename Json::const_reference;
    private:
        std::size_t precedence_level_;
        bool is_right_associative_;

    protected:
        virtual ~unary_operator() = default; 
    public:
        unary_operator(operator_kind oper)
            : precedence_level_(operator_table::precedence_level(oper)), 
              is_right_associative_(operator_table::is_right_associative(oper))
        {
        }

        std::size_t precedence_level() const 
        {
            return precedence_level_;
        }
        bool is_right_associative() const
        {
            return is_right_associative_;
        }

        virtual reference evaluate(reference val, eval_context<Json>&, std::error_code& ec) const = 0;
    };

    template <typename Json>
    class binary_operator
    {
    public:  
        using reference = typename Json::const_reference;
    private:
        std::size_t precedence_level_;
        bool is_right_associative_;
    protected:
        virtual ~binary_operator() = default; 
    public:
        binary_operator(operator_kind oper)
            : precedence_level_(operator_table::precedence_level(oper)), 
              is_right_associative_(operator_table::is_right_associative(oper))
        {
        }


        std::size_t precedence_level() const 
        {
            return precedence_level_;
        }
        bool is_right_associative() const
        {
            return is_right_associative_;
        }

        virtual reference evaluate(reference lhs, reference rhs, eval_context<Json>&, std::error_code& ec) const = 0;

        virtual std::string to_string(std::size_t indent = 0) const
        {
            std::string s;
            for (std::size_t i = 0; i <= indent; ++i)
            {
                s.push_back(' ');
            }
            s.append("to_string not implemented\n");
            return s;
        }
    };

    enum class token_kind 
    {
        current_node,
        lparen,
        rparen,
        begin_multi_select_hash,
        end_multi_select_hash,
        begin_multi_select_list,
        end_multi_select_list,
        begin_filter,
        end_filter,
        pipe,
        separator,
        key,
        literal,
        expression,
        binary_operator,
        unary_operator,
        function,
        end_function,
        argument,
        begin_expression_type,
        end_expression_type,
        end_of_expression,
        variable,
        variable_binding
    };

    struct literal_arg_t
    {
        explicit literal_arg_t() = default;
    };
    constexpr literal_arg_t literal_arg{};

    struct begin_expression_type_arg_t
    {
        explicit begin_expression_type_arg_t() = default;
    };
    constexpr begin_expression_type_arg_t begin_expression_type_arg{};

    struct end_expression_type_arg_t
    {
        explicit end_expression_type_arg_t() = default;
    };
    constexpr end_expression_type_arg_t end_expression_type_arg{};

    struct end_of_expression_arg_t
    {
        explicit end_of_expression_arg_t() = default;
    };
    constexpr end_of_expression_arg_t end_of_expression_arg{};

    struct separator_arg_t
    {
        explicit separator_arg_t() = default;
    };
    constexpr separator_arg_t separator_arg{};

    struct key_arg_t
    {
        explicit key_arg_t() = default;
    };
    constexpr key_arg_t key_arg{};

    struct lparen_arg_t
    {
        explicit lparen_arg_t() = default;
    };
    constexpr lparen_arg_t lparen_arg{};

    struct rparen_arg_t
    {
        explicit rparen_arg_t() = default;
    };
    constexpr rparen_arg_t rparen_arg{};

    struct begin_multi_select_hash_arg_t
    {
        explicit begin_multi_select_hash_arg_t() = default;
    };
    constexpr begin_multi_select_hash_arg_t begin_multi_select_hash_arg{};

    struct end_multi_select_hash_arg_t
    {
        explicit end_multi_select_hash_arg_t() = default;
    };
    constexpr end_multi_select_hash_arg_t end_multi_select_hash_arg{};

    struct begin_multi_select_list_arg_t
    {
        explicit begin_multi_select_list_arg_t() = default;
    };
    constexpr begin_multi_select_list_arg_t begin_multi_select_list_arg{};

    struct end_multi_select_list_arg_t
    {
        explicit end_multi_select_list_arg_t() = default;
    };
    constexpr end_multi_select_list_arg_t end_multi_select_list_arg{};

    struct begin_filter_arg_t
    {
        explicit begin_filter_arg_t() = default;
    };
    constexpr begin_filter_arg_t begin_filter_arg{};

    struct end_filter_arg_t
    {
        explicit end_filter_arg_t() = default;
    };
    constexpr end_filter_arg_t end_filter_arg{};

    struct pipe_arg_t
    {
        explicit pipe_arg_t() = default;
    };
    constexpr pipe_arg_t pipe_arg{};

    struct current_node_arg_t
    {
        explicit current_node_arg_t() = default;
    };
    constexpr current_node_arg_t current_node_arg{};

    struct end_function_arg_t
    {
        explicit end_function_arg_t() = default;
    };
    constexpr end_function_arg_t end_function_arg{};

    struct argument_arg_t
    {
        explicit argument_arg_t() = default;
    };
    constexpr argument_arg_t argument_arg{};

    struct variable_binding_arg_t
    {
        explicit variable_binding_arg_t() = default;
    };
    constexpr variable_binding_arg_t variable_binding_arg{};

    struct slice
    {
        jsoncons::optional<int64_t> start_;
        jsoncons::optional<int64_t> stop_;
        int64_t step_;

        slice()
            : step_(1)
        {
        }

        slice(const jsoncons::optional<int64_t>& start, const jsoncons::optional<int64_t>& end, int64_t step) 
            : start_(start), stop_(end), step_(step)
        {
        }

        slice(const slice& other) = default;

        slice(slice&& other) = default;

        slice& operator=(const slice& other) = default;

        slice& operator=(slice&& other) = default;

        ~slice() = default;

        int64_t get_start(std::size_t size) const
        {
            if (start_)
            {
                auto len = *start_ >= 0 ? *start_ : (static_cast<int64_t>(size) + *start_);
                return len <= static_cast<int64_t>(size) ? len : static_cast<int64_t>(size);
            }
            else
            {
                if (step_ >= 0)
                {
                    return 0;
                }
                else 
                {
                    return static_cast<int64_t>(size);
                }
            }
        }

        int64_t get_stop(std::size_t size) const
        {
            if (stop_)
            {
                auto len = *stop_ >= 0 ? *stop_ : (static_cast<int64_t>(size) + *stop_);
                return len <= static_cast<int64_t>(size) ? len : static_cast<int64_t>(size);
            }
            else
            {
                return step_ >= 0 ? static_cast<int64_t>(size) : -1;
            }
        }

        int64_t step() const
        {
            return step_; // Allow negative
        }
    };

    template <typename Json>
    class token
    {
    public:
        using char_type = typename Json::char_type;
        using char_traits_type = typename Json::char_traits_type;
        using string_type = std::basic_string<char_type,char_traits_type>;

        token_kind type_;

        string_type key_;
        union
        {
            expr_base_impl<Json>* expression_;
            const unary_operator<Json>* unary_operator_;
            const binary_operator<Json>* binary_operator_;
            const function_base<Json>* function_;
            Json value_;
        };
    public:

        token(current_node_arg_t) noexcept
            : type_(token_kind::current_node), expression_{nullptr}
        {
        }

        token(end_function_arg_t) noexcept
            : type_(token_kind::end_function), expression_{nullptr}
        {
        }

        token(separator_arg_t) noexcept
            : type_(token_kind::separator), expression_{nullptr}
        {
        }

        token(lparen_arg_t) noexcept
            : type_(token_kind::lparen), expression_{nullptr}
        {
        }

        token(rparen_arg_t) noexcept
            : type_(token_kind::rparen), expression_{nullptr}
        {
        }

        token(end_of_expression_arg_t) noexcept
            : type_(token_kind::end_of_expression), expression_{nullptr}
        {
        }

        token(begin_multi_select_hash_arg_t) noexcept
            : type_(token_kind::begin_multi_select_hash), expression_{nullptr}
        {
        }

        token(end_multi_select_hash_arg_t) noexcept
            : type_(token_kind::end_multi_select_hash)
        {
        }

        token(begin_multi_select_list_arg_t) noexcept
            : type_(token_kind::begin_multi_select_list)
        {
        }

        token(end_multi_select_list_arg_t) noexcept
            : type_(token_kind::end_multi_select_list)
        {
        }

        token(begin_filter_arg_t) noexcept
            : type_(token_kind::begin_filter)
        {
        }

        token(end_filter_arg_t) noexcept
            : type_(token_kind::end_filter)
        {
        }

        token(pipe_arg_t) noexcept
            : type_(token_kind::pipe)
        {
        }

        token(key_arg_t, const string_type& key)
            : type_(token_kind::key)
        {
            new (&key_) string_type(key);
        }

        token(expr_base_impl<Json>* expression)
            : type_(token_kind::expression), 
              expression_(expression)
        {
        }

        token(const unary_operator<Json>* expression) noexcept
            : type_(token_kind::unary_operator),
              unary_operator_(expression)
        {
        }

        token(const binary_operator<Json>* expression) noexcept
            : type_(token_kind::binary_operator),
              binary_operator_(expression)
        {
        }

        token(const function_base<Json>* function) noexcept
            : type_(token_kind::function),
              function_(function)
        {
        }

        token(argument_arg_t) noexcept
            : type_(token_kind::argument)
        {
        }

        token(begin_expression_type_arg_t) noexcept
            : type_(token_kind::begin_expression_type)
        {
        }

        token(end_expression_type_arg_t) noexcept
            : type_(token_kind::end_expression_type)
        {
        }

        token(literal_arg_t, Json&& value) noexcept
            : type_(token_kind::literal), value_(std::move(value))
        {
        }

        token(token&& other) noexcept
        {
            construct(std::move(other));
        }

        token(const token& other)
        {
            construct(other);
        }

        token(const std::string& variable_ref, expr_base_impl<Json>* expression) noexcept
            : type_(token_kind::variable), 
              key_(variable_ref), 
              expression_(expression)
        {
        }

        token(variable_binding_arg_t, const string_type& variable_ref)
            : type_(token_kind::variable_binding), key_(variable_ref)
        {
        }

        ~token() noexcept
        {
            destroy();
        }

        token& operator=(const token& other)
        {
            if (&other != this)
            {
                destroy();
                construct(other);
            }
            return *this;
        }

        token& operator=(token&& other) noexcept
        {
            if (&other != this)
            {
                destroy();
                construct(std::move(other));
            }
            return *this;
        }

        token_kind type() const
        {
            return type_;
        }

        bool is_lparen() const
        {
            return type_ == token_kind::lparen; 
        }

        bool is_lbrace() const
        {
            return type_ == token_kind::begin_multi_select_hash; 
        }

        bool is_key() const
        {
            return type_ == token_kind::key; 
        }

        bool is_rparen() const
        {
            return type_ == token_kind::rparen; 
        }

        bool is_current_node() const
        {
            return type_ == token_kind::current_node; 
        }

        bool is_projection() const
        {
            if (is_expression())
            {
                JSONCONS_ASSERT(expression_ != nullptr);
                return expression_->is_projection();;
            }
            return false; 
        }

        bool is_expression() const
        {
            return type_ == token_kind::expression; 
        }

        bool is_operator() const
        {
            return type_ == token_kind::unary_operator || 
                   type_ == token_kind::binary_operator; 
        }

        std::size_t precedence_level() const
        {
            switch(type_)
            {
                case token_kind::unary_operator:
                    JSONCONS_ASSERT(unary_operator_ != nullptr);
                    return unary_operator_->precedence_level();
                case token_kind::binary_operator:
                    JSONCONS_ASSERT(binary_operator_ != nullptr);
                    return binary_operator_->precedence_level();
                case token_kind::expression:
                    JSONCONS_ASSERT(expression_ != nullptr);
                    return expression_->precedence_level();
                default:
                    return 0;
            }
        }

        jsoncons::optional<std::size_t> arity() const
        {
            return type_ == token_kind::function ? function_->arity() : jsoncons::optional<std::size_t>();
        }

        bool is_right_associative() const
        {
            switch(type_)
            {
                case token_kind::unary_operator:
                    JSONCONS_ASSERT(unary_operator_ != nullptr);
                    return unary_operator_->is_right_associative();
                case token_kind::binary_operator:
                    JSONCONS_ASSERT(binary_operator_ != nullptr);
                    return binary_operator_->is_right_associative();
                case token_kind::expression:
                    JSONCONS_ASSERT(expression_ != nullptr);
                    return expression_->is_right_associative();
                default:
                    return false;
            }
        }

        void construct(token<Json>&& other)
        {
            type_ = other.type_;
            switch (type_)
            {
                case token_kind::variable:
                    key_ = std::move(other.key_);
                    expression_ = other.expression_;
                    break;
                case token_kind::variable_binding:
                case token_kind::key:
                    key_ = std::move(other.key_);
                    break;
                case token_kind::literal:
                    new (&value_) Json(std::move(other.value_));
                    break;
                case token_kind::expression:
                    expression_ = other.expression_;
                    break;
                case token_kind::unary_operator:
                    unary_operator_ = other.unary_operator_;
                    break;
                case token_kind::binary_operator:
                    binary_operator_ = other.binary_operator_;
                    break;
                case token_kind::function:
                    function_ = other.function_;
                    break;
                default:
                    break;
            }
        }

        void construct(const token<Json>& other)
        {
            type_ = other.type_;
            switch (type_)
            {
                case token_kind::variable:
                    key_ = other.key_;
                    expression_ = other.expression_;
                    break;
                case token_kind::variable_binding:
                case token_kind::key:
                    key_ = other.key_;
                    break;
                case token_kind::literal:
                    new (&value_) Json(other.value_);
                    break;
                case token_kind::expression:
                    expression_ = other.expression_;
                    break;
                case token_kind::unary_operator:
                    unary_operator_ = other.unary_operator_;
                    break;
                case token_kind::binary_operator:
                    binary_operator_ = other.binary_operator_;
                    break;
                case token_kind::function:
                    function_ = other.function_;
                    break;
                default:
                    break;
            }
        }

        void destroy() noexcept 
        {
            switch(type_)
            {
                case token_kind::literal:
                    value_.~Json();
                    break;
                default:
                    break;
            }
        }

        std::string to_string(std::size_t indent = 0) const
        {
            switch(type_)
            {
                case token_kind::expression:
                    return expression_->to_string(indent);
                    break;
                case token_kind::unary_operator:
                    return std::string("unary_operator");
                    break;
                case token_kind::binary_operator:
                    return binary_operator_->to_string(indent);
                    break;
                case token_kind::current_node:
                    return std::string("current_node");
                    break;
                case token_kind::end_function:
                    return std::string("end_function");
                    break;
                case token_kind::separator:
                    return std::string("separator");
                    break;
                case token_kind::literal:
                    return std::string("literal");
                    break;
                case token_kind::key:
                    return std::string("key") + key_;
                    break;
                case token_kind::begin_multi_select_hash:
                    return std::string("begin_multi_select_hash");
                    break;
                case token_kind::begin_multi_select_list:
                    return std::string("begin_multi_select_list");
                    break;
                case token_kind::begin_filter:
                    return std::string("begin_filter");
                    break;
                case token_kind::pipe:
                    return std::string("pipe");
                    break;
                case token_kind::lparen:
                    return std::string("lparen");
                    break;
                case token_kind::function:
                    return function_->to_string();
                case token_kind::argument:
                    return std::string("argument");
                    break;
                case token_kind::begin_expression_type:
                    return std::string("begin_expression_type");
                    break;
                case token_kind::end_expression_type:
                    return std::string("end_expression_type");
                    break;
                default:
                    return std::string("default");
                    break;
            }
        }
    };
     
    enum class expr_state 
    {
        start,
        lhs_expression,
        rhs_expression,
        sub_expression,
        expression_type,
        comparator_expression,
        function_expression,
        argument,
        expression_or_expression_type,
        quoted_string,
        raw_string,
        raw_string_escape_char,
        quoted_string_escape_char,
        escape_u1, 
        escape_u2, 
        escape_u3, 
        escape_u4, 
        escape_expect_surrogate_pair1, 
        escape_expect_surrogate_pair2, 
        escape_u5, 
        escape_u6, 
        escape_u7, 
        escape_u8, 
        literal,
        key_expr,
        val_expr,
        identifier_or_function_expr,
        unquoted_string,
        key_val_expr,
        number,
        digit,
        index_or_slice_expression,
        bracket_specifier,
        bracket_specifier_or_multi_select_list,
        filter,
        multi_select_list,
        multi_select_hash,
        rhs_slice_expression_stop,
        rhs_slice_expression_step,
        expect_rbracket,
        expect_rparen,
        expect_dot,
        expect_rbrace,
        expect_colon,
        expect_multi_select_list,
        cmp_lt_or_lte,
        cmp_eq,
        cmp_gt_or_gte,
        cmp_ne,
        expect_pipe_or_or,
        expect_and,
        variable_binding,
        variable_ref,
        expect_assign,
        expect_in_or_comma,
        substitute_variable
    };
    
    template <typename Json>
    struct expression_context
    {
        std::size_t end_index{0};
        std::string variable_ref;
        
        expression_context() = default;
    };

    template <typename Json>
    class jmespath_evaluator 
    {
    public:
        typedef typename Json::char_type char_type;
        typedef typename Json::char_traits_type char_traits_type;
        typedef std::basic_string<char_type,char_traits_type> string_type;
        typedef typename Json::string_view_type string_view_type;
        using reference = const Json&;
        using pointer = typename Json::const_pointer;
        using const_pointer = typename Json::const_pointer;
        using parameter_type = parameter<Json>;
        using expression_type = expr_base_impl<Json>;
        using function_type = function_base<Json>;

        static bool is_false(reference ref)
        {
            return (ref.is_array() && ref.empty()) ||
                   (ref.is_object() && ref.empty()) ||
                   (ref.is_string() && ref.as_string_view().size() == 0) ||
                   (ref.is_bool() && !ref.as_bool()) ||
                   ref.is_null();
        }

        static bool is_true(reference ref)
        {
            return !is_false(ref);
        }

        class not_expression final : public unary_operator<Json>
        {
        public:
            not_expression()
                : unary_operator<Json>(operator_kind::not_op)
            {}

            reference evaluate(reference val, eval_context<Json>& context, std::error_code&) const override
            {
                return is_false(val) ? context.true_value() : context.false_value();
            }
        };

        class abs_function : public function_base<Json>
        {
        public:
            abs_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                switch (arg0.type())
                {
                    case json_type::uint64_value:
                        return arg0;
                    case json_type::int64_value:
                    {
                        return arg0.template as<int64_t>() >= 0 ? arg0 : *context.create_json(std::abs(arg0.template as<int64_t>()));
                    }
                    case json_type::double_value:
                    {
                        return arg0.template as<double>() >= 0 ? arg0 : *context.create_json(std::abs(arg0.template as<double>()));
                    }
                    default:
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                }
            }
        };

        class avg_function : public function_base<Json>
        {
        public:
            avg_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.empty())
                {
                    return context.null_value();
                }

                double sum = 0;
                for (auto& j : arg0.array_range())
                {
                    if (!j.is_number())
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    sum += j.template as<double>();
                }

                return arg0.size() == 0 ? context.null_value() : *context.create_json(sum / arg0.size());
            }
        };

        class ceil_function : public function_base<Json>
        {
        public:
            ceil_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                switch (arg0.type())
                {
                    case json_type::uint64_value:
                    case json_type::int64_value:
                    {
                        return *context.create_json(arg0.template as<double>());
                    }
                    case json_type::double_value:
                    {
                        return *context.create_json(std::ceil(arg0.template as<double>()));
                    }
                    default:
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                }
            }
        };

        class contains_function : public function_base<Json>
        {
        public:
            contains_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_value() && args[1].is_value()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }


                reference arg0 = args[0].value();
                reference arg1 = args[1].value();

                switch (arg0.type())
                {
                    case json_type::array_value:
                        for (auto& j : arg0.array_range())
                        {
                            if (j == arg1)
                            {
                                return context.true_value();
                            }
                        }
                        return context.false_value();
                    case json_type::string_value:
                    {
                        if (!arg1.is_string())
                        {
                            ec = jmespath_errc::invalid_type;
                            return context.null_value();
                        }
                        auto sv0 = arg0.template as<string_view_type>();
                        auto sv1 = arg1.template as<string_view_type>();
                        return sv0.find(sv1) != string_view_type::npos ? context.true_value() : context.false_value();
                    }
                    default:
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                }
            }
        };

        class ends_with_function : public function_base<Json>
        {
        public:
            ends_with_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_value() && args[1].is_value()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_string())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg1 = args[1].value();
                if (!arg1.is_string())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                auto sv0 = arg0.template as<string_view_type>();
                auto sv1 = arg1.template as<string_view_type>();

                if (sv1.length() <= sv0.length() && sv1 == sv0.substr(sv0.length() - sv1.length()))
                {
                    return context.true_value();
                }
                else
                {
                    return context.false_value();
                }
            }
        };

        class floor_function : public function_base<Json>
        {
        public:
            floor_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                switch (arg0.type())
                {
                    case json_type::uint64_value:
                    case json_type::int64_value:
                    {
                        return *context.create_json(arg0.template as<double>());
                    }
                    case json_type::double_value:
                    {
                        return *context.create_json(std::floor(arg0.template as<double>()));
                    }
                    default:
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                }
            }
        };

        class join_function : public function_base<Json>
        {
        public:
            join_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                reference arg0 = args[0].value();
                reference arg1 = args[1].value();

                if (!(args[0].is_value() && args[1].is_value()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                if (!arg0.is_string())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (!arg1.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                string_type sep = arg0.template as<string_type>();
                string_type buf;
                bool is_first = true;
                for (auto &j : arg1.array_range())
                {
                    if (!j.is_string())
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }

                    if (is_first)
                    {
                        is_first = false;
                    }
                    else
                    {
                        buf.append(sep);
                    }
                    
                    auto sv = j.template as<string_view_type>();
                    buf.append(sv.begin(), sv.end());
                }
                return *context.create_json(buf);
            }
        };

        class length_function : public function_base<Json>
        {
        public:
            length_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();

                switch (arg0.type())
                {
                    case json_type::object_value:
                    case json_type::array_value:
                        return *context.create_json(arg0.size());
                    case json_type::string_value:
                    {
                        auto sv0 = arg0.template as<string_view_type>();
                        auto length = unicode_traits::count_codepoints(sv0.data(), sv0.size());
                        return *context.create_json(length);
                    }
                    default:
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                }
            }
        };

        class max_function : public function_base<Json>
        {
        public:
            max_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.empty())
                {
                    return context.null_value();
                }

                bool is_number = arg0.at(0).is_number();
                bool is_string = arg0.at(0).is_string();
                if (!is_number && !is_string)
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                std::size_t index = 0;
                for (std::size_t i = 1; i < arg0.size(); ++i)
                {
                    if (!(arg0.at(i).is_number() == is_number && arg0.at(i).is_string() == is_string))
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    if (arg0.at(i) > arg0.at(index))
                    {
                        index = i;
                    }
                }

                return arg0.at(index);
            }
        };

        class max_by_function : public function_base<Json>
        {
        public:
            max_by_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_value() && args[1].is_expression()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.empty())
                {
                    return context.null_value();
                }

                const auto& expr = args[1].expression();

                std::error_code ec2;
                Json key1 = expr.evaluate(arg0.at(0), context, ec2); 

                bool is_number = key1.is_number();
                bool is_string = key1.is_string();
                if (!(is_number || is_string))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                std::size_t index = 0;
                for (std::size_t i = 1; i < arg0.size(); ++i)
                {
                    reference key2 = expr.evaluate(arg0.at(i), context, ec2); 
                    if (!(key2.is_number() == is_number && key2.is_string() == is_string))
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    if (key2 > key1)
                    {
                        key1 = key2;
                        index = i;
                    }
                }

                return arg0.at(index);
            }
        };

        class map_function : public function_base<Json>
        {
        public:
            map_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_expression() && args[1].is_value()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                const auto& expr = args[0].expression();

                reference arg0 = args[1].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);

                for (auto& item : arg0.array_range())
                {
                    auto& j = expr.evaluate(item, context, ec);
                    if (ec)
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    result->emplace_back(json_const_pointer_arg, std::addressof(j));
                }

                return *result;
            }

            std::string to_string(std::size_t = 0) const override
            {
                return std::string("map_function\n");
            }
        };

        class min_function : public function_base<Json>
        {
        public:
            min_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.empty())
                {
                    return context.null_value();
                }

                bool is_number = arg0.at(0).is_number();
                bool is_string = arg0.at(0).is_string();
                if (!is_number && !is_string)
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                std::size_t index = 0;
                for (std::size_t i = 1; i < arg0.size(); ++i)
                {
                    if (!(arg0.at(i).is_number() == is_number && arg0.at(i).is_string() == is_string))
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    if (arg0.at(i) < arg0.at(index))
                    {
                        index = i;
                    }
                }

                return arg0.at(index);
            }
        };

        class min_by_function : public function_base<Json>
        {
        public:
            min_by_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_value() && args[1].is_expression()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.empty())
                {
                    return context.null_value();
                }

                const auto& expr = args[1].expression();

                std::error_code ec2;
                Json key1 = expr.evaluate(arg0.at(0), context, ec2); 

                bool is_number = key1.is_number();
                bool is_string = key1.is_string();
                if (!(is_number || is_string))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                std::size_t index = 0;
                for (std::size_t i = 1; i < arg0.size(); ++i)
                {
                    reference key2 = expr.evaluate(arg0.at(i), context, ec2); 
                    if (!(key2.is_number() == is_number && key2.is_string() == is_string))
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    if (key2 < key1)
                    {
                        key1 = key2;
                        index = i;
                    }
                }

                return arg0.at(index);
            }
        };

        class merge_function : public function_base<Json>
        {
        public:
            merge_function()
                : function_base<Json>(jsoncons::optional<std::size_t>())
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (args.empty())
                {
                    ec = jmespath_errc::invalid_arity;
                    return context.null_value();
                }

                for (auto& param : args)
                {
                    if (!param.is_value())
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                }

                reference arg0 = args[0].value();
                if (!arg0.is_object())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (args.size() == 1)
                {
                    return arg0;
                }

                auto result = context.create_json(arg0);
                for (std::size_t i = 1; i < args.size(); ++i)
                {
                    reference argi = args[i].value();
                    if (!argi.is_object())
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    for (auto& item : argi.object_range())
                    {
                        result->insert_or_assign(item.key(),item.value());
                    }
                }

                return *result;
            }
        };

        class type_function : public function_base<Json>
        {
        public:
            type_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();

                switch (arg0.type())
                {
                    case json_type::int64_value:
                    case json_type::uint64_value:
                    case json_type::double_value:
                        return context.number_type_name();
                    case json_type::bool_value:
                        return context.boolean_type_name();
                    case json_type::string_value:
                        return context.string_type_name();
                    case json_type::object_value:
                        return context.object_type_name();
                    case json_type::array_value:
                        return context.array_type_name();
                    default:
                        return context.null_type_name();
                        break;

                }
            }
        };

        class sort_function : public function_base<Json>
        {
        public:
            sort_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.size() <= 1)
                {
                    return arg0;
                }

                bool is_number = arg0.at(0).is_number();
                bool is_string = arg0.at(0).is_string();
                if (!is_number && !is_string)
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                for (std::size_t i = 1; i < arg0.size(); ++i)
                {
                    if (arg0.at(i).is_number() != is_number || arg0.at(i).is_string() != is_string)
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                }

                auto v = context.create_json(arg0);
                std::stable_sort((v->array_range()).begin(), (v->array_range()).end());
                return *v;
            }
        };

        class sort_by_function : public function_base<Json>
        {
        public:
            sort_by_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_value() && args[1].is_expression()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                if (arg0.size() <= 1)
                {
                    return arg0;
                }

                const auto& expr = args[1].expression();

                auto v = context.create_json(arg0);
                std::stable_sort((v->array_range()).begin(), (v->array_range()).end(),
                    [&expr,&context,&ec](reference lhs, reference rhs) -> bool
                {
                    std::error_code ec2;
                    reference key1 = expr.evaluate(lhs, context, ec2);
                    bool is_number = key1.is_number();
                    bool is_string = key1.is_string();
                    if (!(is_number || is_string))
                    {
                        ec = jmespath_errc::invalid_type;
                    }

                    reference key2 = expr.evaluate(rhs, context, ec2);
                    if (!(key2.is_number() == is_number && key2.is_string() == is_string))
                    {
                        ec = jmespath_errc::invalid_type;
                    }
                    
                    return key1 < key2;
                });
                return ec ? context.null_value() : *v;
            }

            std::string to_string(std::size_t = 0) const override
            {
                return std::string("sort_by_function\n");
            }
        };

        class keys_function final : public function_base<Json>
        {
        public:
            keys_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_object())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);
                result->reserve(args.size());

                for (auto& item : arg0.object_range())
                {
                    result->emplace_back(item.key());
                }
                return *result;
            }
        };

        class values_function final : public function_base<Json>
        {
        public:
            values_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_object())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);
                result->reserve(args.size());

                for (auto& item : arg0.object_range())
                {
                    result->emplace_back(item.value());
                }
                return *result;
            }
        };

        class reverse_function final : public function_base<Json>
        {
        public:
            reverse_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                switch (arg0.type())
                {
                    case json_type::string_value:
                    {
                        string_view_type sv = arg0.as_string_view();
                        std::basic_string<char32_t> buf;
                        unicode_traits::convert(sv.data(), sv.size(), buf);
                        std::reverse(buf.begin(), buf.end());
                        string_type s;
                        unicode_traits::convert(buf.data(), buf.size(), s);
                        return *context.create_json(s);
                    }
                    case json_type::array_value:
                    {
                        auto result = context.create_json(arg0);
                        std::reverse(result->array_range().begin(),result->array_range().end());
                        return *result;
                    }
                    default:
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                }
            }
        };

        class starts_with_function : public function_base<Json>
        {
        public:
            starts_with_function()
                : function_base<Json>(2)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!(args[0].is_value() && args[1].is_value()))
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_string())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg1 = args[1].value();
                if (!arg1.is_string())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                auto sv0 = arg0.template as<string_view_type>();
                auto sv1 = arg1.template as<string_view_type>();

                if (sv1.length() <= sv0.length() && sv1 == sv0.substr(0, sv1.length()))
                {
                    return context.true_value();
                }
                else
                {
                    return context.false_value();
                }
            }
        };

        class sum_function : public function_base<Json>
        {
        public:
            sum_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (!arg0.is_array())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }
                double sum = 0;
                for (auto& j : arg0.array_range())
                {
                    if (!j.is_number())
                    {
                        ec = jmespath_errc::invalid_type;
                        return context.null_value();
                    }
                    sum += j.template as<double>();
                }

                return *context.create_json(sum);
            }
        };

        class to_array_function final : public function_base<Json>
        {
        public:
            to_array_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                if (arg0.is_array())
                {
                    return arg0;
                }
                else
                {
                    auto result = context.create_json(json_array_arg);
                    result->push_back(arg0);
                    return *result;
                }
            }

            std::string to_string(std::size_t = 0) const override
            {
                return std::string("to_array_function\n");
            }
        };

        class to_number_function final : public function_base<Json>
        {
        public:
            to_number_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                switch (arg0.type())
                {
                    case json_type::int64_value:
                    case json_type::uint64_value:
                    case json_type::double_value:
                        return arg0;
                    case json_type::string_value:
                    {
                        auto sv = arg0.as_string_view();
                        uint64_t uval{ 0 };
                        auto result1 = jsoncons::detail::to_integer(sv.data(), sv.length(), uval);
                        if (result1)
                        {
                            return *context.create_json(uval);
                        }
                        int64_t sval{ 0 };
                        auto result2 = jsoncons::detail::to_integer(sv.data(), sv.length(), sval);
                        if (result2)
                        {
                            return *context.create_json(sval);
                        }
                        const jsoncons::detail::chars_to to_double;
                        try
                        {
                            auto s = arg0.as_string();
                            double d = to_double(s.c_str(), s.length());
                            return *context.create_json(d);
                        }
                        catch (const std::exception&)
                        {
                            return context.null_value();
                        }
                    }
                    default:
                        return context.null_value();
                }
            }

            std::string to_string(std::size_t = 0) const override
            {
                return std::string("to_number_function\n");
            }
        };

        class to_string_function final : public function_base<Json>
        {
        public:
            to_string_function()
                : function_base<Json>(1)
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code& ec) const override
            {
                JSONCONS_ASSERT(args.size() == *this->arity());

                if (!args[0].is_value())
                {
                    ec = jmespath_errc::invalid_type;
                    return context.null_value();
                }

                reference arg0 = args[0].value();
                return *context.create_json(arg0.template as<string_type>());
            }

            std::string to_string(std::size_t = 0) const override
            {
                return std::string("to_string_function\n");
            }
        };

        class not_null_function final : public function_base<Json>
        {
        public:
            not_null_function()
                : function_base<Json>(jsoncons::optional<std::size_t>())
            {
            }

            reference evaluate(const std::vector<parameter_type>& args, eval_context<Json>& context, std::error_code&) const override
            {
                for (auto& param : args)
                {
                    if (param.is_value() && !param.value().is_null())
                    {
                        return param.value();
                    }
                }
                return context.null_value();
            }

            std::string to_string(std::size_t = 0) const override
            {
                return std::string("to_string_function\n");
            }
        };

        static pointer evaluate_tokens(reference doc, 
            const std::vector<token<Json>>& output_stack, 
            eval_context<Json>& context, 
            std::error_code& ec)
        {
            pointer root_ptr = std::addressof(doc);
            std::vector<parameter_type> stack;
            std::vector<parameter_type> arg_stack;
            for (std::size_t i = 0; i < output_stack.size(); ++i)
            {
                auto& t = output_stack[i];
                switch (t.type())
                {
                    case token_kind::literal:
                    {
                        stack.emplace_back(t.value_);
                        break;
                    }
                    case token_kind::begin_expression_type:
                    {
                        JSONCONS_ASSERT(i+1 < output_stack.size());
                        ++i;
                        JSONCONS_ASSERT(output_stack[i].is_expression());
                        JSONCONS_ASSERT(!stack.empty());
                        stack.pop_back();
                        stack.emplace_back(*output_stack[i].expression_);
                        break;
                    }
                    case token_kind::pipe:
                    {
                        JSONCONS_ASSERT(!stack.empty());
                        root_ptr = std::addressof(stack.back().value());
                        break;
                    }
                    case token_kind::current_node:
                        stack.emplace_back(*root_ptr);
                        break;
                    case token_kind::expression:
                    {
                        JSONCONS_ASSERT(!stack.empty());
                        pointer ptr = std::addressof(stack.back().value());
                        stack.pop_back();
                        auto& ref = t.expression_->evaluate(*ptr, context, ec);
                        stack.emplace_back(ref);
                        break;
                    }
                    case token_kind::variable:
                    {
                        auto& ref = t.expression_->evaluate(doc, context, ec);
                        context.set_variable(t.key_, ref);
                        break;
                    }
                    case token_kind::variable_binding:
                    {
                        JSONCONS_ASSERT(!stack.empty());
                        stack.pop_back();
                        const auto& j = context.get_variable(t.key_, ec);
                        if (ec)
                        {
                            ec = jmespath_errc::undefined_variable;
                            return std::addressof(context.null_value());
                        }
                        stack.push_back(j);
                        break;
                    }
                    case token_kind::unary_operator:
                    {
                        JSONCONS_ASSERT(!stack.empty());
                        pointer ptr = std::addressof(stack.back().value());
                        stack.pop_back();
                        reference r = t.unary_operator_->evaluate(*ptr, context, ec);
                        stack.emplace_back(r);
                        break;
                    }
                    case token_kind::binary_operator:
                    {
                        JSONCONS_ASSERT(stack.size() >= 2);
                        pointer rhs = std::addressof(stack.back().value());
                        stack.pop_back();
                        pointer lhs = std::addressof(stack.back().value());
                        stack.pop_back();
                        reference r = t.binary_operator_->evaluate(*lhs,*rhs, context, ec);
                        stack.emplace_back(r);
                        break;
                    }
                    case token_kind::argument:
                    {
                        JSONCONS_ASSERT(!stack.empty());
                        arg_stack.push_back(std::move(stack.back()));
                        stack.pop_back();
                        break;
                    }
                    case token_kind::function:
                    {
                        if (t.function_->arity() && *(t.function_->arity()) != arg_stack.size())
                        {
                            ec = jmespath_errc::invalid_arity;
                            return std::addressof(context.null_value());
                        }
                        
                        std::vector<expr_wrapper<Json>> expr_wrappers;
                        if (t.function_->is_custom())
                        {
                            if (expr_wrappers.empty())
                            {
                                expr_wrappers.resize(arg_stack.size());
                            }
                            for (std::size_t k = 0; k < arg_stack.size(); ++k)
                            {
                                if (arg_stack[k].is_expression())
                                {
                                    expr_wrappers[k] = expr_wrapper<Json>{ *(arg_stack[k].expression_) };
                                    arg_stack[k].expression_ = std::addressof(expr_wrappers[k]);
                                }
                            }
                        }

                        reference r = t.function_->evaluate(arg_stack, context, ec);
                        if (ec)
                        {
                            return std::addressof(context.null_value());
                        }
                        arg_stack.clear();
                        stack.emplace_back(r);
                        break;
                    }
                    default:
                        break;
                }
            }
            JSONCONS_ASSERT(stack.size() == 1);
            return std::addressof(stack.back().value());
        }

        // Implementations

        class or_operator final : public binary_operator<Json>
        {
        public:
            or_operator()
                : binary_operator<Json>(operator_kind::or_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override
            {
                if (lhs.is_null() && rhs.is_null())
                {
                    return context.null_value();
                }
                if (!is_false(lhs))
                {
                    return lhs;
                }
                else
                {
                    return rhs;
                }
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("or_operator\n");
                return s;
            }
        };

        class and_operator final : public binary_operator<Json>
        {
        public:
            and_operator()
                : binary_operator<Json>(operator_kind::and_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>&, std::error_code&) const override
            {
                if (is_true(lhs))
                {
                    return rhs;
                }
                else
                {
                    return lhs;
                }
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("and_operator\n");
                return s;
            }
        };

        class eq_operator final : public binary_operator<Json>
        {
        public:
            eq_operator()
                : binary_operator<Json>(operator_kind::eq_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override 
            {
                return lhs == rhs ? context.true_value() : context.false_value();
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("eq_operator\n");
                return s;
            }
        };

        class ne_operator final : public binary_operator<Json>
        {
        public:
            ne_operator()
                : binary_operator<Json>(operator_kind::ne_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override 
            {
                return lhs != rhs ? context.true_value() : context.false_value();
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("ne_operator\n");
                return s;
            }
        };

        class lt_operator final : public binary_operator<Json>
        {
        public:
            lt_operator()
                : binary_operator<Json>(operator_kind::lt_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override 
            {
                if (!(lhs.is_number() && rhs.is_number()))
                {
                    return context.null_value();
                }
                return lhs < rhs ? context.true_value() : context.false_value();
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("lt_operator\n");
                return s;
            }
        };

        class lte_operator final : public binary_operator<Json>
        {
        public:
            lte_operator()
                : binary_operator<Json>(operator_kind::lte_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override 
            {
                if (!(lhs.is_number() && rhs.is_number()))
                {
                    return context.null_value();
                }
                return lhs <= rhs ? context.true_value() : context.false_value();
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("lte_operator\n");
                return s;
            }
        };

        class gt_operator final : public binary_operator<Json>
        {
        public:
            gt_operator()
                : binary_operator<Json>(operator_kind::gt_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override
            {
                if (!(lhs.is_number() && rhs.is_number()))
                {
                    return context.null_value();
                }
                return lhs > rhs ? context.true_value() : context.false_value();
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("gt_operator\n");
                return s;
            }
        };

        class gte_operator final : public binary_operator<Json>
        {
        public:
            gte_operator()
                : binary_operator<Json>(operator_kind::gte_op)
            {
            }

            reference evaluate(reference lhs, reference rhs, eval_context<Json>& context, std::error_code&) const override
            {
                if (!(lhs.is_number() && rhs.is_number()))
                {
                    return context.null_value();
                }
                return lhs >= rhs ? context.true_value() : context.false_value();
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("gte_operator\n");
                return s;
            }
        };

        // basic_expression
        class basic_expression :  public expression_type
        {
        public:
            basic_expression()
                : expression_type(operator_kind::default_op, false)
            {
            }

            void add_expression(expression_type*) override
            {
            }
        };

        class identifier_selector final : public basic_expression
        {
        private:
            string_type identifier_;
        public:
            identifier_selector(const string_view_type& name)
                : identifier_(name)
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code&) const override
            {
                //std::cout << "(identifier_selector " << identifier_  << " ) " << pretty_print(val) << "\n";
                if (val.is_object() && val.contains(identifier_))
                {
                    return val.at(identifier_);
                }
                else 
                {
                    return context.null_value();
                }
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("identifier_selector ");
                s.append(identifier_);
                return s;
            }
        };

        class current_node final : public basic_expression
        {
        public:
            current_node()
            {
            }

            reference evaluate(reference val, eval_context<Json>&, std::error_code&) const override
            {
                return val;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("current_node ");
                return s;
            }
        };

        class index_selector final : public basic_expression
        {
            int64_t index_;
        public:
            index_selector(int64_t index)
                : index_(index)
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code&) const override
            {
                if (!val.is_array())
                {
                    return context.null_value();
                }
                auto slen = static_cast<int64_t>(val.size());
                if (index_ >= 0 && index_ < slen)
                {
                    std::size_t index = static_cast<std::size_t>(index_);
                    return val.at(index);
                }
                else if ((slen + index_) >= 0 && (slen+index_) < slen)
                {
                    std::size_t index = static_cast<std::size_t>(slen + index_);
                    return val.at(index);
                }
                else
                {
                    return context.null_value();
                }
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("index_selector ");
                s.append(std::to_string(index_));
                return s;
            }
        };

        // projection_base
        class projection_base : public expression_type
        {
        protected:
            std::vector<expression_type*> expressions_;
        public:
            projection_base(operator_kind oper)
                : expression_type(oper, true)
            {
            }

            void add_expression(expression_type* expr) override
            {
                if (!expressions_.empty() && expressions_.back()->is_projection() && 
                    (expr->precedence_level() < expressions_.back()->precedence_level() ||
                     (expr->precedence_level() == expressions_.back()->precedence_level() && expr->is_right_associative())))
                {
                    expressions_.back()->add_expression(expr);
                }
                else
                {
                    expressions_.emplace_back(expr);
                }
            }

            reference apply_expressions(reference val, eval_context<Json>& context, std::error_code& ec) const
            {
                pointer ptr = std::addressof(val);
                for (auto& expression : expressions_)
                {
                    ptr = std::addressof(expression->evaluate(*ptr, context, ec));
                }
                return *ptr;
            }
        };

        class object_projection final : public projection_base
        {
        public:
            object_projection()
                : projection_base(operator_kind::projection_op)
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (!val.is_object())
                {
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);
                for (auto& item : val.object_range())
                {
                    if (!item.value().is_null())
                    {
                        reference j = this->apply_expressions(item.value(), context, ec);
                        if (!j.is_null())
                        {
                            result->emplace_back(json_const_pointer_arg, std::addressof(j));
                        }
                    }
                }
                return *result;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("object_projection\n");
                for (auto& expr : this->expressions_)
                {
                    std::string sss = expr->to_string(indent+2);
                    s.insert(s.end(), sss.begin(), sss.end());
                    s.push_back('\n');
                }
                return s;
            }
        };

        class list_projection final : public projection_base
        {
        public:
            list_projection()
                : projection_base(operator_kind::projection_op)
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (!val.is_array())
                {
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);
                for (reference item : val.array_range())
                {
                    if (!item.is_null())
                    {
                        reference j = this->apply_expressions(item, context, ec);
                        if (!j.is_null())
                        {
                            result->emplace_back(json_const_pointer_arg, std::addressof(j));
                        }
                    }
                }
                return *result;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("list_projection\n");
                for (auto& expr : this->expressions_)
                {
                    std::string sss = expr->to_string(indent+2);
                    s.insert(s.end(), sss.begin(), sss.end());
                    s.push_back('\n');
                }
                return s;
            }
        };

        class slice_projection final : public projection_base
        {
            slice slice_;
        public:
            slice_projection(const slice& s)
                : projection_base(operator_kind::projection_op), slice_(s)
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (!val.is_array())
                {
                    return context.null_value();
                }

                auto start = slice_.get_start(val.size());
                auto end = slice_.get_stop(val.size());
                auto step = slice_.step();

                if (step == 0)
                {
                    ec = jmespath_errc::step_cannot_be_zero;
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);
                if (step > 0)
                {
                    if (start < 0)
                    {
                        start = 0;
                    }
                    if (end > static_cast<int64_t>(val.size()))
                    {
                        end = val.size();
                    }
                    for (int64_t i = start; i < end; i += step)
                    {
                        reference j = this->apply_expressions(val.at(static_cast<std::size_t>(i)), context, ec);
                        if (!j.is_null())
                        {
                            result->emplace_back(json_const_pointer_arg, std::addressof(j));
                        }
                    }
                }
                else
                {
                    if (start >= static_cast<int64_t>(val.size()))
                    {
                        start = static_cast<int64_t>(val.size()) - 1;
                    }
                    if (end < -1)
                    {
                        end = -1;
                    }
                    for (int64_t i = start; i > end; i += step)
                    {
                        reference j = this->apply_expressions(val.at(static_cast<std::size_t>(i)), context, ec);
                        if (!j.is_null())
                        {
                            result->emplace_back(json_const_pointer_arg, std::addressof(j));
                        }
                    }
                }

                return *result;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("slice_projection\n");
                for (auto& expr : this->expressions_)
                {
                    std::string sss = expr->to_string(indent+2);
                    s.insert(s.end(), sss.begin(), sss.end());
                    s.push_back('\n');
                }
                return s;
            }
        };

        class filter_expression final : public projection_base
        {
            std::vector<token<Json>> token_list_;
        public:
            filter_expression(std::vector<token<Json>>&& token_list)
                : projection_base(operator_kind::projection_op), token_list_(std::move(token_list))
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (!val.is_array())
                {
                    return context.null_value();
                }
                auto result = context.create_json(json_array_arg);

                for (auto& item : val.array_range())
                {
                    eval_context<Json> new_context{ context.temp_storage_, context.variables_ };
                    Json j(json_const_pointer_arg, evaluate_tokens(item, token_list_, new_context, ec));
                    if (is_true(j))
                    {
                        reference jj = this->apply_expressions(item, context, ec);
                        if (!jj.is_null())
                        {
                            result->emplace_back(json_const_pointer_arg, std::addressof(jj));
                        }
                    }
                }
                return *result;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("filter_expression\n");
                for (auto& item : token_list_)
                {
                    std::string sss = item.to_string(indent+2);
                    s.insert(s.end(), sss.begin(), sss.end());
                    s.push_back('\n');
                }
                return s;
            }
        };

        class flatten_projection final : public projection_base
        {
        public:
            flatten_projection()
                : projection_base(operator_kind::flatten_projection_op)
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (!val.is_array())
                {
                    return context.null_value();
                }

                auto result = context.create_json(json_array_arg);
                for (reference current_elem : val.array_range())
                {
                    if (current_elem.is_array())
                    {
                        for (reference elem : current_elem.array_range())
                        {
                            if (!elem.is_null())
                            {
                                reference j = this->apply_expressions(elem, context, ec);
                                if (!j.is_null())
                                {
                                    result->emplace_back(json_const_pointer_arg, std::addressof(j));
                                }
                            }
                        }
                    }
                    else
                    {
                        if (!current_elem.is_null())
                        {
                            reference j = this->apply_expressions(current_elem, context, ec);
                            if (!j.is_null())
                            {
                                result->emplace_back(json_const_pointer_arg, std::addressof(j));
                            }
                        }
                    }
                }
                return *result;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("flatten_projection\n");
                for (auto& expr : this->expressions_)
                {
                    std::string sss = expr->to_string(indent+2);
                    s.insert(s.end(), sss.begin(), sss.end());
                    s.push_back('\n');
                }
                return s;
            }
        };

        class multi_select_list final : public basic_expression
        {
            std::vector<std::vector<token<Json>>> token_lists_;
        public:
            multi_select_list(std::vector<std::vector<token<Json>>>&& token_lists)
                : token_lists_(std::move(token_lists))
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (val.is_null())
                {
                    return val;
                }
                auto result = context.create_json(json_array_arg);
                result->reserve(token_lists_.size());

                for (auto& list : token_lists_)
                {
                    eval_context<Json> new_context{ context.temp_storage_, context.variables_ };
                    result->emplace_back(json_const_pointer_arg, evaluate_tokens(val, list, new_context, ec));
                }
                return *result;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("multi_select_list\n");
                for (auto& list : token_lists_)
                {
                    for (auto& item : list)
                    {
                        std::string sss = item.to_string(indent+2);
                        s.insert(s.end(), sss.begin(), sss.end());
                        s.push_back('\n');
                    }
                    s.append("---\n");
                }
                return s;
            }
        };

        class variable_expression final : public basic_expression
        {
            std::vector<token<Json>> tokens_;
        public:
            variable_expression(std::vector<token<Json>>&& tokens)
                : tokens_(std::move(tokens))
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                eval_context<Json> new_context{ context.temp_storage_, context.variables_ };
                auto ptr = evaluate_tokens(val, tokens_, new_context, ec);
                return *ptr;
            }

            std::string to_string(std::size_t = 0) const override
            {
                std::string s;
                s.append("variable_expression\n");
                return s;
            }
        };

        struct key_tokens
        {
            string_type key;
            std::vector<token<Json>> tokens;

            key_tokens(string_type&& Key, std::vector<token<Json>>&& Tokens) noexcept
                : key(std::move(Key)), tokens(std::move(Tokens))
            {
            }
        };

        class multi_select_hash final : public basic_expression
        {
        public:
            std::vector<key_tokens> key_toks_;

            multi_select_hash(std::vector<key_tokens>&& key_toks)
                : key_toks_(std::move(key_toks))
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                if (val.is_null())
                {
                    return val;
                }
                auto resultp = context.create_json(json_object_arg);
                resultp->reserve(key_toks_.size());
                for (auto& item : key_toks_)
                {
                    eval_context<Json> new_context{ context.temp_storage_, context.variables_ };
                    resultp->try_emplace(item.key, json_const_pointer_arg, evaluate_tokens(val, item.tokens, new_context, ec));
                }

                return *resultp;
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("multi_select_list\n");
                return s;
            }
        };

        class function_expression final : public basic_expression
        {
        public:
            std::vector<token<Json>> toks_;

            function_expression(std::vector<token<Json>>&& toks)
                : toks_(std::move(toks))
            {
            }

            reference evaluate(reference val, eval_context<Json>& context, std::error_code& ec) const override
            {
                eval_context<Json> new_context{ context.temp_storage_, context.variables_ };
                return *evaluate_tokens(val, toks_, new_context, ec);
            }

            std::string to_string(std::size_t indent = 0) const override
            {
                std::string s;
                for (std::size_t i = 0; i <= indent; ++i)
                {
                    s.push_back(' ');
                }
                s.append("function_expression\n");
                for (auto& tok : toks_)
                {
                    for (std::size_t i = 0; i <= indent+2; ++i)
                    {
                        s.push_back(' ');
                    }
                    std::string sss = tok.to_string(indent+2);
                    s.insert(s.end(), sss.begin(), sss.end());
                    s.push_back('\n');
                }
                return s;
            }
        };

        class static_resources
        {
            struct MyHash
            {
                std::uintmax_t operator()(string_type const& s) const noexcept
                {
                    const int p = 31;
                    const int m = static_cast<int>(1e9) + 9;
                    std::uintmax_t hash_value = 0;
                    std::uintmax_t p_pow = 1;
                    for (char_type c : s) {
                        hash_value = (hash_value + (c - 'a' + 1) * p_pow) % m;
                        p_pow = (p_pow * p) % m;
                    }
                    return hash_value;   
                }
            };

            std::unordered_map<string_type,std::unique_ptr<function_base<Json>>,MyHash> custom_functions_;
            std::vector<std::unique_ptr<expr_base<Json>>> expr_storage_;
            
        public:

            static_resources() = default;
            static_resources(const static_resources& expr) = delete;
            static_resources& operator=(const static_resources& expr) = delete;
            static_resources(static_resources&& expr) = default;
            static_resources& operator=(static_resources&& expr) = default;

            static_resources(const custom_functions<Json>& functions)
                : static_resources{}
            {
                for (const auto& item : functions)
                {
                    custom_functions_.emplace(item.name(),
                        jsoncons::make_unique<function_wrapper<Json>>(item.arity(),item.function()));
                }
            }
            
            template <typename T>
            expr_base_impl<Json>* create_expression(T&& val)
            {
                auto temp = jsoncons::make_unique<T>(std::forward<T>(val));
                expr_base_impl<Json>* ptr = temp.get();
                expr_storage_.push_back(std::move(temp));
                return ptr;
            }

            const function_base<Json>* get_function(const string_type& name, std::error_code& ec) const
            {
                static abs_function abs_func;
                static avg_function avg_func;
                static ceil_function ceil_func;
                static contains_function contains_func;
                static ends_with_function ends_with_func;
                static floor_function floor_func;
                static join_function join_func;
                static length_function length_func;
                static max_function max_func;
                static max_by_function max_by_func;
                static map_function map_func;
                static merge_function merge_func;
                static min_function min_func;
                static min_by_function min_by_func;
                static type_function type_func;
                static sort_function sort_func;
                static sort_by_function sort_by_func;
                static keys_function keys_func;
                static values_function values_func;
                static reverse_function reverse_func;
                static starts_with_function starts_with_func;
                static const sum_function sum_func;
                static to_array_function to_array_func;
                static to_number_function to_number_func;
                static to_string_function to_string_func;
                static not_null_function not_null_func;

                using function_dictionary = std::unordered_map<string_type,const function_base<Json>*>;
                static const function_dictionary functions_ =
                {
                    {string_type{'a','b','s'}, &abs_func},
                    {string_type{'a','v','g'}, &avg_func},
                    {string_type{'c','e','i', 'l'}, &ceil_func},
                    {string_type{'c','o','n', 't', 'a', 'i', 'n', 's'}, &contains_func},
                    {string_type{'e','n','d', 's', '_', 'w', 'i', 't', 'h'}, &ends_with_func},
                    {string_type{'f','l','o', 'o', 'r'}, &floor_func},
                    {string_type{'j','o','i', 'n'}, &join_func},
                    {string_type{'l','e','n', 'g', 't', 'h'}, &length_func},
                    {string_type{'m','a','x'}, &max_func},
                    {string_type{'m','a','x','_','b','y'}, &max_by_func},
                    {string_type{'m','a','p'}, &map_func},
                    {string_type{'m','i','n'}, &min_func},
                    {string_type{'m','i','n','_','b','y'}, &min_by_func},
                    {string_type{'m','e','r', 'g', 'e'}, &merge_func},
                    {string_type{'t','y','p', 'e'}, &type_func},
                    {string_type{'s','o','r', 't'}, &sort_func},
                    {string_type{'s','o','r', 't','_','b','y'}, &sort_by_func},
                    {string_type{'k','e','y', 's'}, &keys_func},
                    {string_type{'v','a','l', 'u','e','s'}, &values_func},
                    {string_type{'r','e','v', 'e', 'r', 's','e'}, &reverse_func},
                    {string_type{'s','t','a', 'r','t','s','_','w','i','t','h'}, &starts_with_func},
                    {string_type{'s','u','m'}, &sum_func},
                    {string_type{'t','o','_','a','r','r','a','y',}, &to_array_func},
                    {string_type{'t','o','_', 'n', 'u', 'm','b','e','r'}, &to_number_func},
                    {string_type{'t','o','_', 's', 't', 'r','i','n','g'}, &to_string_func},
                    {string_type{'n','o','t', '_', 'n', 'u','l','l'}, &not_null_func}
                };

                auto it = functions_.find(name);
                if (it != functions_.end())
                {
                    return (*it).second;
                }
                auto it2 = custom_functions_.find(name);
                if (it2 == custom_functions_.end())
                {
                    ec = jmespath_errc::unknown_function;
                    return nullptr;
                }
                return it2->second.get();
            }

            const unary_operator<Json>* get_not_operator() const
            {
                static const not_expression not_oper;

                return &not_oper;
            }

            const binary_operator<Json>* get_or_operator() const
            {
                static const or_operator or_oper;

                return &or_oper;
            }

            const binary_operator<Json>* get_and_operator() const
            {
                static const and_operator and_oper;

                return &and_oper;
            }

            const binary_operator<Json>* get_eq_operator() const
            {
                static const eq_operator eq_oper;
                return &eq_oper;
            }

            const binary_operator<Json>* get_ne_operator() const
            {
                static const ne_operator ne_oper;
                return &ne_oper;
            }

            const binary_operator<Json>* get_lt_operator() const
            {
                static const lt_operator lt_oper;
                return &lt_oper;
            }

            const binary_operator<Json>* get_lte_operator() const
            {
                static const lte_operator lte_oper;
                return &lte_oper;
            }

            const binary_operator<Json>* get_gt_operator() const
            {
                static const gt_operator gt_oper;
                return &gt_oper;
            }

            const binary_operator<Json>* get_gte_operator() const
            {
                static const gte_operator gte_oper;
                return &gte_oper;
            }
        };

        class jmespath_expression
        {
        public:
            static_resources resources_;
            std::vector<token<Json>> output_stack_;
        public:
            jmespath_expression() = default;

            jmespath_expression(const jmespath_expression& expr) = delete;
            jmespath_expression& operator=(const jmespath_expression& expr) = delete;

            jmespath_expression(jmespath_expression&& expr)
                : resources_(std::move(expr.resources_)),
                  output_stack_(std::move(expr.output_stack_))
            {
            }

            jmespath_expression(static_resources&& resources,
                std::vector<token<Json>>&& output_stack)
                : resources_(std::move(resources)), output_stack_(std::move(output_stack))
            {
            }

            Json evaluate(reference doc) const
            {
                if (output_stack_.empty())
                {
                    return Json::null();
                }
                std::error_code ec;
                Json result = evaluate(doc, ec);
                if (ec)
                {
                    JSONCONS_THROW(jmespath_error(ec));
                }
                return result;
            }

            Json evaluate(reference doc, 
                const std::vector<std::pair<std::string,Json>>& params) const
            {
                if (output_stack_.empty())
                {
                    return Json::null();
                }
                std::error_code ec;
                Json result = evaluate(doc, params, ec);
                if (ec)
                {
                    JSONCONS_THROW(jmespath_error(ec));
                }
                return result;
            }

            Json evaluate(reference doc, std::error_code& ec) const
            {
                if (output_stack_.empty())
                {
                    return Json::null();
                }
                std::vector<std::unique_ptr<Json>> temp_storage;
                eval_context<Json> context{temp_storage};
                return deep_copy(*evaluate_tokens(doc, output_stack_, context, ec));
            }

            Json evaluate(reference doc, 
                const std::vector<std::pair<std::string,Json>>& params,
                std::error_code& ec) const
            {
                if (output_stack_.empty())
                {
                    return Json::null();
                }
                std::vector<std::unique_ptr<Json>> temp_storage;
                eval_context<Json> context{temp_storage};
                for (const auto& param : params)
                {
                    context.set_variable(param.first, param.second);
                }
                
                return deep_copy(*evaluate_tokens(doc, output_stack_, context, ec));
            }
        };
    public:
        std::size_t line_{1};
        std::size_t column_{1};
        const char_type* begin_input_{nullptr};
        const char_type* end_input_{nullptr};
        const char_type* p_{nullptr};
        std::vector<token<Json>> operator_stack_;

    public:
        jmespath_evaluator()
        {
        }
        
        ~jmespath_evaluator() = default;

        std::size_t line() const
        {
            return line_;
        }

        std::size_t column() const
        {
            return column_;
        }

        jmespath_expression compile(const char_type* path, std::size_t length, 
            const jsoncons::jmespath::custom_functions<Json>& funcs, 
            std::error_code& ec)
        {
            static_resources resources{funcs};
            std::vector<expression_context<Json>> context_stack;
            std::vector<expr_state> state_stack;
            std::vector<token<Json>> output_stack;
            std::string key_buffer;

            state_stack.push_back(expr_state::start);

            string_type buffer;
            uint32_t cp = 0;
            uint32_t cp2 = 0;
     
            begin_input_ = path;
            end_input_ = path + length;
            p_ = begin_input_;

            slice slic{};

            bool done = false;
            while (p_ < end_input_ && !done)
            {
                switch (state_stack.back())
                {
                    case expr_state::start: 
                    {
                        state_stack.back() = expr_state::rhs_expression;
                        state_stack.push_back(expr_state::lhs_expression);
                        context_stack.push_back(expression_context<Json>{});
                        break;
                    }
                    case expr_state::rhs_expression:
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '.': 
                                ++p_;
                                ++column_;
                                state_stack.push_back(expr_state::sub_expression);
                                break;
                            case '|':
                                ++p_;
                                ++column_;
                                state_stack.push_back(expr_state::lhs_expression);
                                state_stack.push_back(expr_state::expect_pipe_or_or);
                                break;
                            case '&':
                                ++p_;
                                ++column_;
                                state_stack.push_back(expr_state::lhs_expression);
                                state_stack.push_back(expr_state::expect_and);
                                break;
                            case '<':
                            case '>':
                            case '=':
                            {
                                state_stack.push_back(expr_state::lhs_expression);
                                state_stack.push_back(expr_state::comparator_expression);
                                break;
                            }
                            case '!':
                            {
                                ++p_;
                                ++column_;
                                state_stack.push_back(expr_state::lhs_expression);
                                state_stack.push_back(expr_state::cmp_ne);
                                break;
                            }
                            case '[':
                                state_stack.push_back(expr_state::bracket_specifier);
                                ++p_;
                                ++column_;
                                break;
                            case ')':
                            {
                                state_stack.pop_back();
                                JSONCONS_ASSERT(!context_stack.empty());
                                context_stack.pop_back();
                                break;
                            }
                            default:
                                if (state_stack.size() > 1) 
                                {
                                    state_stack.pop_back();
                                    JSONCONS_ASSERT(!context_stack.empty());
                                    context_stack.pop_back();
                                }
                                else
                                {
                                    ec = jmespath_errc::syntax_error;
                                    return jmespath_expression{};
                                }
                                break;
                        }
                        break;
                    case expr_state::comparator_expression:
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '<':
                                ++p_;
                                ++column_;
                                state_stack.back() = expr_state::cmp_lt_or_lte;
                                break;
                            case '>':
                                ++p_;
                                ++column_;
                                state_stack.back() = expr_state::cmp_gt_or_gte;
                                break;
                            case '=':
                            {
                                ++p_;
                                ++column_;
                                state_stack.back() = expr_state::cmp_eq;
                                break;
                            }
                            default:
                                if (state_stack.size() > 1)
                                {
                                    state_stack.pop_back();
                                }
                                else
                                {
                                    ec = jmespath_errc::syntax_error;
                                    return jmespath_expression{};
                                }
                                break;
                        }
                        break;
                    case expr_state::substitute_variable:
                    {
                        push_token(token<Json>{variable_binding_arg, buffer},
                            resources, output_stack, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        buffer.clear();
                        state_stack.pop_back();
                        break;
                    }
                    case expr_state::lhs_expression: 
                    {
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '\"':
                                state_stack.back() = expr_state::val_expr;
                                state_stack.push_back(expr_state::quoted_string);
                                ++p_;
                                ++column_;
                                break;
                            case '\'':
                                state_stack.back() = expr_state::raw_string;
                                ++p_;
                                ++column_;
                                break;
                            case '`':
                                state_stack.back() = expr_state::literal;
                                ++p_;
                                ++column_;
                                break;
                            case '{':
                                push_token(begin_multi_select_hash_arg, resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::multi_select_hash;
                                ++p_;
                                ++column_;
                                break;
                            case '*': // wildcard
                                push_token(token<Json>(resources.create_expression(object_projection())), 
                                    resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                ++p_;
                                ++column_;
                                break;
                            case '(':
                            {
                                ++p_;
                                ++column_;
                                push_token(lparen_arg, resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::expect_rparen;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                break;
                            }
                            case '!':
                            {
                                ++p_;
                                ++column_;
                                push_token(token<Json>(resources.get_not_operator()), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                break;
                            }
                            case '@':
                                ++p_;
                                ++column_;
                                push_token(resources.create_expression(current_node()), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                break;
                            case '[': 
                                state_stack.back() = expr_state::bracket_specifier_or_multi_select_list;
                                ++p_;
                                ++column_;
                                break;
                            case '$':
                                state_stack.back() = expr_state::substitute_variable;
                                state_stack.push_back(expr_state::unquoted_string);
                                buffer.clear();
                                ++p_;
                                ++column_;
                                break;
                            default:
                                if ((*p_ >= 'A' && *p_ <= 'Z') || (*p_ >= 'a' && *p_ <= 'z') || (*p_ == '_'))
                                {
                                    buffer.clear();
                                    state_stack.back() = expr_state::identifier_or_function_expr;
                                    state_stack.push_back(expr_state::unquoted_string);
                                    buffer.push_back(*p_);
                                    ++p_;
                                    ++column_;
                                }
                                else
                                {
                                    ec = jmespath_errc::expected_identifier;
                                    return jmespath_expression{};
                                }
                                break;
                        };
                        break;
                    }
                    case expr_state::sub_expression: 
                    {
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '\"':
                                state_stack.back() = expr_state::val_expr;
                                state_stack.push_back(expr_state::quoted_string);
                                ++p_;
                                ++column_;
                                break;
                            case '{':
                                push_token(begin_multi_select_hash_arg, resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::multi_select_hash;
                                ++p_;
                                ++column_;
                                break;
                            case '*':
                                push_token(resources.create_expression(object_projection()), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                ++p_;
                                ++column_;
                                break;
                            case '[': 
                                state_stack.back() = expr_state::expect_multi_select_list;
                                ++p_;
                                ++column_;
                                break;
                            default:
                                if ((*p_ >= 'A' && *p_ <= 'Z') || (*p_ >= 'a' && *p_ <= 'z') || (*p_ == '_'))
                                {
                                    buffer.clear();
                                    state_stack.back() = expr_state::identifier_or_function_expr;
                                    state_stack.push_back(expr_state::unquoted_string);
                                    buffer.push_back(*p_);
                                    ++p_;
                                    ++column_;
                                }
                                else
                                {
                                    ec = jmespath_errc::expected_identifier;
                                    return jmespath_expression{};
                                }
                                break;
                        };
                        break;
                    }
                    case expr_state::key_expr:
                        push_token(token<Json>(key_arg, buffer), resources, output_stack, ec);
                        if (ec) {return jmespath_expression{};}
                        buffer.clear(); 
                        state_stack.pop_back(); 
                        break;
                    case expr_state::val_expr:
                        push_token(resources.create_expression(identifier_selector(buffer)), resources, output_stack, ec);
                        if (ec) {return jmespath_expression{};}
                        buffer.clear();
                        state_stack.pop_back(); 
                        break;
                    case expr_state::expression_or_expression_type:
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '&':
                                push_token(token<Json>(begin_expression_type_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::expression_type;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                ++p_;
                                ++column_;
                                break;
                            default:
                                state_stack.back() = expr_state::argument;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                break;
                        }
                        break;
                    case expr_state::expect_in_or_comma:
                    {
                        //std::cout << "expr_state::expect_in_or_comma\n";
                        advance_past_space_character();
                        if (*p_ == ',')
                        {
                            std::vector<token<Json>> toks;
                            for (std::size_t i = context_stack.back().end_index; i < output_stack.size(); ++i)
                            {
                                toks.push_back(std::move(output_stack[i]));
                            }
                            output_stack.erase(output_stack.begin() + context_stack.back().end_index, output_stack.end());
                            JSONCONS_ASSERT(!toks.empty());
                            if (toks.front().type() != token_kind::literal)
                            {
                                toks.emplace(toks.begin(), current_node_arg);
                            }
                            push_token(token<Json>{ context_stack.back().variable_ref, 
                                resources.create_expression(variable_expression(std::move(toks))) },
                                resources, output_stack, ec);
                            if (ec)
                            {
                                return jmespath_expression{};
                            }

                            state_stack.back() = expr_state::variable_binding;
                            ++p_;
                            ++column_;
                        }
                        else if (*p_ == 'i' && (p_ + 1) < end_input_ && *(p_ + 1) == 'n')
                        {
                            p_ += 2;
                            column_ += 2;

                            std::vector<token<Json>> toks;
                            for (std::size_t i = context_stack.back().end_index; i < output_stack.size(); ++i)
                            {
                                toks.push_back(std::move(output_stack[i]));
                            }
                            output_stack.erase(output_stack.begin() + context_stack.back().end_index, output_stack.end());
                            JSONCONS_ASSERT(!toks.empty());
                            if (toks.front().type() != token_kind::literal)
                            {
                                toks.emplace(toks.begin(), current_node_arg);
                            }
                            push_token(token<Json>{ context_stack.back().variable_ref, 
                                resources.create_expression(variable_expression(std::move(toks))) },
                                resources, output_stack, ec);
                            if (ec)
                            {
                                return jmespath_expression{};
                            }

                            state_stack.pop_back(); // pop expect_in_or_comma
                        }
                        else
                        {
                            ec = jmespath_errc::syntax_error;
                            return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_assign:
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '=':
                            {
                                ++p_;
                                ++column_;
                                
                                context_stack.back().end_index = output_stack.size();
                                context_stack.back().variable_ref = buffer;
                                state_stack.back() = expr_state::expect_in_or_comma;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                buffer.clear();
                                break;
                            }
                            default:
                            {
                                ec = jmespath_errc::syntax_error;
                                return jmespath_expression{};
                            }
                        }
                        break;
                    case expr_state::variable_ref:
                        state_stack.back() = expr_state::expect_assign;
                        break;
                    case expr_state::variable_binding:
                    {
                        //std::cout << "expr_state::variable_binding\n";
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '$':
                                state_stack.back() = expr_state::variable_ref;
                                state_stack.push_back(expr_state::unquoted_string);
                                buffer.clear();
                                ++p_;
                                ++column_;
                                break;
                            default:
                            {
                                ec = jmespath_errc::syntax_error;
                                return jmespath_expression{};
                            }
                        }
                        break;
                    }
                    case expr_state::identifier_or_function_expr:
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '(':
                            {
                                auto f = resources.get_function(buffer, ec);
                                if (ec)
                                {
                                    return jmespath_expression{};
                                }
                                buffer.clear();
                                push_token(token<Json>(f), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::function_expression;
                                // check no-args function
                                bool is_no_args_func = true;
                                bool isEnd = false;
                                for (const char_type *p2_ = p_ + 1; p2_ < end_input_ && !isEnd; ++p2_)
                                {
                                    
                                    switch (*p2_)
                                    {
                                        case ' ':case '\t':case '\r':case '\n':
                                            break;
                                        case ')':
                                            isEnd = true;
                                            break;
                                        default:
                                            is_no_args_func = false;
                                            isEnd = true;
                                            break;
                                        }
                                }
                                if (!is_no_args_func)
                                {
                                    state_stack.push_back(expr_state::expression_or_expression_type);
                                }
                                ++p_;
                                ++column_;
                                break;
                            }
                            case '$':
                            {
                                if (buffer.size() == 3 && buffer[0] == 'l' && buffer[1] == 'e' && buffer[2] == 't')
                                {
                                    state_stack.back() = expr_state::lhs_expression;
                                    state_stack.push_back(expr_state::variable_binding);
                                    buffer.clear();
                                }
                                else
                                {
                                    ec = jmespath_errc::syntax_error;
                                    return jmespath_expression{};
                                }
                                break;
                            }
                            default:
                            {
                                push_token(resources.create_expression(identifier_selector(buffer)), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                buffer.clear();
                                state_stack.pop_back(); 
                                break;
                            }
                        }
                        break;

                    case expr_state::function_expression:
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case ',':
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.push_back(expr_state::expression_or_expression_type);
                                ++p_;
                                ++column_;
                                break;
                            case ')':
                            {
                                push_token(token<Json>(end_function_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                ++p_;
                                ++column_;
                                break;
                            }
                            default:
                                break;
                        }
                        break;

                    case expr_state::argument:
                        push_token(argument_arg, resources, output_stack, ec);
                        if (ec) {return jmespath_expression{};}
                        state_stack.pop_back();
                        break;

                    case expr_state::expression_type:
                        push_token(end_expression_type_arg, resources, output_stack, ec);
                        push_token(argument_arg, resources, output_stack, ec);
                        if (ec) {return jmespath_expression{};}
                        state_stack.pop_back();
                        break;

                    case expr_state::quoted_string: 
                        switch (*p_)
                        {
                            case '\"':
                                state_stack.pop_back(); // quoted_string
                                ++p_;
                                ++column_;
                                break;
                            case '\\':
                                state_stack.push_back(expr_state::quoted_string_escape_char);
                                ++p_;
                                ++column_;
                                break;
                            default:
                                buffer.push_back(*p_);
                                ++p_;
                                ++column_;
                                break;
                        };
                        break;

                    case expr_state::unquoted_string: 
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                state_stack.pop_back(); // unquoted_string
                                advance_past_space_character();
                                break;
                            default:
                                if ((*p_ >= '0' && *p_ <= '9') || (*p_ >= 'A' && *p_ <= 'Z') || (*p_ >= 'a' && *p_ <= 'z') || (*p_ == '_'))
                                {
                                    buffer.push_back(*p_);
                                    ++p_;
                                    ++column_;
                                }
                                else
                                {
                                    state_stack.pop_back(); // unquoted_string
                                }
                                break;
                        };
                        break;
                    case expr_state::raw_string_escape_char:
                        switch (*p_)
                        {
                            case '\'':
                                buffer.push_back(*p_);
                                state_stack.pop_back();
                                ++p_;
                                ++column_;
                                break;
                            default:
                                buffer.push_back('\\');
                                buffer.push_back(*p_);
                                state_stack.pop_back();
                                ++p_;
                                ++column_;
                                break;
                        }
                        break;
                    case expr_state::quoted_string_escape_char:
                        switch (*p_)
                        {
                            case '\"':
                                buffer.push_back('\"');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case '\\': 
                                buffer.push_back('\\');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case '/':
                                buffer.push_back('/');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case 'b':
                                buffer.push_back('\b');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case 'f':
                                buffer.push_back('\f');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case 'n':
                                buffer.push_back('\n');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case 'r':
                                buffer.push_back('\r');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case 't':
                                buffer.push_back('\t');
                                ++p_;
                                ++column_;
                                state_stack.pop_back();
                                break;
                            case 'u':
                                ++p_;
                                ++column_;
                                state_stack.back() = expr_state::escape_u1;
                                break;
                            default:
                                ec = jmespath_errc::illegal_escaped_character;
                                return jmespath_expression{};
                        }
                        break;
                    case expr_state::escape_u1:
                        cp = append_to_codepoint(0, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        ++p_;
                        ++column_;
                        state_stack.back() = expr_state::escape_u2;
                        break;
                    case expr_state::escape_u2:
                        cp = append_to_codepoint(cp, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        ++p_;
                        ++column_;
                        state_stack.back() = expr_state::escape_u3;
                        break;
                    case expr_state::escape_u3:
                        cp = append_to_codepoint(cp, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        ++p_;
                        ++column_;
                        state_stack.back() = expr_state::escape_u4;
                        break;
                    case expr_state::escape_u4:
                        cp = append_to_codepoint(cp, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        if (unicode_traits::is_high_surrogate(cp))
                        {
                            ++p_;
                            ++column_;
                            state_stack.back() = expr_state::escape_expect_surrogate_pair1;
                        }
                        else
                        {
                            unicode_traits::convert(&cp, 1, buffer);
                            ++p_;
                            ++column_;
                            state_stack.pop_back();
                        }
                        break;
                    case expr_state::escape_expect_surrogate_pair1:
                        switch (*p_)
                        {
                            case '\\': 
                                ++p_;
                                ++column_;
                                state_stack.back() = expr_state::escape_expect_surrogate_pair2;
                                break;
                            default:
                                ec = jmespath_errc::invalid_codepoint;
                                return jmespath_expression{};
                        }
                        break;
                    case expr_state::escape_expect_surrogate_pair2:
                        switch (*p_)
                        {
                            case 'u': 
                                ++p_;
                                ++column_;
                                state_stack.back() = expr_state::escape_u5;
                                break;
                            default:
                                ec = jmespath_errc::invalid_codepoint;
                                return jmespath_expression{};
                        }
                        break;
                    case expr_state::escape_u5:
                        cp2 = append_to_codepoint(0, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        ++p_;
                        ++column_;
                        state_stack.back() = expr_state::escape_u6;
                        break;
                    case expr_state::escape_u6:
                        cp2 = append_to_codepoint(cp2, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        ++p_;
                        ++column_;
                        state_stack.back() = expr_state::escape_u7;
                        break;
                    case expr_state::escape_u7:
                        cp2 = append_to_codepoint(cp2, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        ++p_;
                        ++column_;
                        state_stack.back() = expr_state::escape_u8;
                        break;
                    case expr_state::escape_u8:
                    {
                        cp2 = append_to_codepoint(cp2, *p_, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        uint32_t codepoint = 0x10000 + ((cp & 0x3FF) << 10) + (cp2 & 0x3FF);
                        unicode_traits::convert(&codepoint, 1, buffer);
                        state_stack.pop_back();
                        ++p_;
                        ++column_;
                        break;
                    }
                    case expr_state::raw_string: 
                        switch (*p_)
                        {
                            case '\'':
                            {
                                push_token(token<Json>(literal_arg, Json(buffer)), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                buffer.clear();
                                state_stack.pop_back(); // raw_string
                                ++p_;
                                ++column_;
                                break;
                            }
                            case '\\':
                                state_stack.push_back(expr_state::raw_string_escape_char);
                                ++p_;
                                ++column_;
                                break;
                            default:
                                buffer.push_back(*p_);
                                ++p_;
                                ++column_;
                                break;
                        };
                        break;
                    case expr_state::literal: 
                        switch (*p_)
                        {
                            case '`':
                            {
                                json_decoder<Json> decoder;
                                basic_json_reader<char_type,string_source<char_type>> reader(buffer, decoder);
                                std::error_code parse_ec;
                                reader.read(parse_ec);
                                if (parse_ec)
                                {
                                    ec = jmespath_errc::invalid_literal;
                                    return jmespath_expression{};
                                }
                                auto j = decoder.get_result();

                                push_token(token<Json>(literal_arg, std::move(j)), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                buffer.clear();
                                state_stack.pop_back(); // json_value
                                ++p_;
                                ++column_;
                                break;
                            }
                            case '\\':
                                if (p_+1 < end_input_)
                                {
                                    ++p_;
                                    ++column_;
                                    if (*p_ != '`')
                                    {
                                        buffer.push_back('\\');
                                    }
                                    buffer.push_back(*p_);
                                }
                                else
                                {
                                    ec = jmespath_errc::unexpected_end_of_input;
                                    return jmespath_expression{};
                                }
                                ++p_;
                                ++column_;
                                break;
                            default:
                                buffer.push_back(*p_);
                                ++p_;
                                ++column_;
                                break;
                        };
                        break;
                    case expr_state::number:
                        switch(*p_)
                        {
                            case '-':
                                buffer.push_back(*p_);
                                state_stack.back() = expr_state::digit;
                                ++p_;
                                ++column_;
                                break;
                            default:
                                state_stack.back() = expr_state::digit;
                                break;
                        }
                        break;
                    case expr_state::digit:
                        switch(*p_)
                        {
                            case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                                buffer.push_back(*p_);
                                ++p_;
                                ++column_;
                                break;
                            default:
                                state_stack.pop_back(); // digit
                                break;
                        }
                        break;

                    case expr_state::bracket_specifier:
                        switch(*p_)
                        {
                            case '*':
                                push_token(resources.create_expression(list_projection()), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::expect_rbracket;
                                ++p_;
                                ++column_;
                                break;
                            case ']': // []
                                push_token(resources.create_expression(flatten_projection()), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); // bracket_specifier
                                ++p_;
                                ++column_;
                                break;
                            case '?':
                                push_token(token<Json>(begin_filter_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::filter;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                ++p_;
                                ++column_;
                                break;
                            case ':': // slice_expression
                                state_stack.back() = expr_state::rhs_slice_expression_stop ;
                                state_stack.push_back(expr_state::number);
                                ++p_;
                                ++column_;
                                break;
                            // number
                            case '-':case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                                state_stack.back() = expr_state::index_or_slice_expression;
                                state_stack.push_back(expr_state::number);
                                break;
                            default:
                                ec = jmespath_errc::expected_index_expression;
                                return jmespath_expression{};
                        }
                        break;
                    case expr_state::bracket_specifier_or_multi_select_list:
                        switch(*p_)
                        {
                            case '*':
                                if (p_+1 >= end_input_)
                                {
                                    ec = jmespath_errc::unexpected_end_of_input;
                                    return jmespath_expression{};
                                }
                                if (*(p_+1) == ']')
                                {
                                    state_stack.back() = expr_state::bracket_specifier;
                                }
                                else
                                {
                                    push_token(token<Json>(begin_multi_select_list_arg), resources, output_stack, ec);
                                    if (ec) {return jmespath_expression{};}
                                    state_stack.back() = expr_state::multi_select_list;
                                    state_stack.push_back(expr_state::rhs_expression);                                
                                    state_stack.push_back(expr_state::lhs_expression);                                
                                    context_stack.push_back(expression_context<Json>{});
                                }
                                break;
                            case ']': // []
                            case '?':
                            case ':': // slice_expression
                            case '-':case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                                state_stack.back() = expr_state::bracket_specifier;
                                break;
                            default:
                                push_token(token<Json>(begin_multi_select_list_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::multi_select_list;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                break;
                        }
                        break;

                    case expr_state::expect_multi_select_list:
                        switch(*p_)
                        {
                            case ']':
                            case '?':
                            case ':':
                            case '-':case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                                ec = jmespath_errc::expected_multi_select_list;
                                return jmespath_expression{};
                            case '*':
                                push_token(resources.create_expression(list_projection()), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::expect_rbracket;
                                ++p_;
                                ++column_;
                                break;
                            default:
                                push_token(token<Json>(begin_multi_select_list_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::multi_select_list;
                                state_stack.push_back(expr_state::rhs_expression);
                                state_stack.push_back(expr_state::lhs_expression);
                                context_stack.push_back(expression_context<Json>{});
                                break;
                        }
                        break;

                    case expr_state::multi_select_hash:
                        switch(*p_)
                        {
                            case '*':
                            case ']':
                            case '?':
                            case ':':
                            case '-':case '0':case '1':case '2':case '3':case '4':case '5':case '6':case '7':case '8':case '9':
                                break;
                            default:
                                state_stack.back() = expr_state::key_val_expr;
                                break;
                        }
                        break;

                    case expr_state::index_or_slice_expression:
                        switch(*p_)
                        {
                            case ']':
                            {
                                if (buffer.empty())
                                {
                                    push_token(resources.create_expression(flatten_projection()), resources, output_stack, ec);
                                    if (ec) {return jmespath_expression{};}
                                }
                                else
                                {
                                    int64_t val{ 0 };
                                    auto r = jsoncons::detail::to_integer(buffer.data(), buffer.size(), val);
                                    if (!r)
                                    {
                                        ec = jmespath_errc::invalid_number;
                                        return jmespath_expression{};
                                    }
                                    push_token(resources.create_expression(index_selector(val)), resources, output_stack, ec);
                                    if (ec) {return jmespath_expression{};}

                                    buffer.clear();
                                }
                                state_stack.pop_back(); // bracket_specifier
                                ++p_;
                                ++column_;
                                break;
                            }
                            case ':':
                            {
                                if (!buffer.empty())
                                {
                                    int64_t val{};
                                    auto r = jsoncons::detail::to_integer(buffer.data(), buffer.size(), val);
                                    if (!r)
                                    {
                                        ec = jmespath_errc::invalid_number;
                                        return jmespath_expression{};
                                    }
                                    slic.start_ = val;
                                    buffer.clear();
                                }
                                state_stack.back() = expr_state::rhs_slice_expression_stop;
                                state_stack.push_back(expr_state::number);
                                ++p_;
                                ++column_;
                                break;
                            }
                            default:
                                ec = jmespath_errc::expected_rbracket;
                                return jmespath_expression{};
                        }
                        break;
                    case expr_state::rhs_slice_expression_stop :
                    {
                        if (!buffer.empty())
                        {
                            int64_t val{ 0 };
                            auto r = jsoncons::detail::to_integer(buffer.data(), buffer.size(), val);
                            if (!r)
                            {
                                ec = jmespath_errc::invalid_number;
                                return jmespath_expression{};
                            }
                            slic.stop_ = jsoncons::optional<int64_t>(val);
                            buffer.clear();
                        }
                        switch(*p_)
                        {
                            case ']':
                                push_token(resources.create_expression(slice_projection(slic)), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                slic = slice{};
                                state_stack.pop_back(); // bracket_specifier2
                                ++p_;
                                ++column_;
                                break;
                            case ':':
                                state_stack.back() = expr_state::rhs_slice_expression_step;
                                state_stack.push_back(expr_state::number);
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_rbracket;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::rhs_slice_expression_step:
                    {
                        if (!buffer.empty())
                        {
                            int64_t val{ 0 };
                            auto r = jsoncons::detail::to_integer(buffer.data(), buffer.size(), val);
                            if (!r)
                            {
                                ec = jmespath_errc::invalid_number;
                                return jmespath_expression{};
                            }
                            if (val == 0)
                            {
                                ec = jmespath_errc::step_cannot_be_zero;
                                return jmespath_expression{};
                            }
                            slic.step_ = val;
                            buffer.clear();
                        }
                        switch(*p_)
                        {
                            case ']':
                                push_token(resources.create_expression(slice_projection(slic)), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                buffer.clear();
                                slic = slice{};
                                state_stack.pop_back(); // rhs_slice_expression_step
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_rbracket;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_rbracket:
                    {
                        switch(*p_)
                        {
                            case ']':
                                state_stack.pop_back(); // expect_rbracket
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_rbracket;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_rparen:
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case ')':
                                ++p_;
                                ++column_;
                                push_token(rparen_arg, resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                break;
                            default:
                                ec = jmespath_errc::expected_rparen;
                                return jmespath_expression{};
                        }
                        break;
                    case expr_state::key_val_expr: 
                    {
                        switch (*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '\"':
                                state_stack.back() = expr_state::expect_colon;
                                state_stack.push_back(expr_state::key_expr);
                                state_stack.push_back(expr_state::quoted_string);
                                ++p_;
                                ++column_;
                                break;
                            case '\'':
                                state_stack.back() = expr_state::expect_colon;
                                state_stack.push_back(expr_state::raw_string);
                                ++p_;
                                ++column_;
                                break;
                            default:
                                if ((*p_ >= 'A' && *p_ <= 'Z') || (*p_ >= 'a' && *p_ <= 'z') || (*p_ == '_'))
                                {
                                    state_stack.back() = expr_state::expect_colon;
                                    state_stack.push_back(expr_state::key_expr);
                                    state_stack.push_back(expr_state::unquoted_string);
                                    buffer.push_back(*p_);
                                    ++p_;
                                    ++column_;
                                }
                                else
                                {
                                    ec = jmespath_errc::expected_key;
                                    return jmespath_expression{};
                                }
                                break;
                        };
                        break;
                    }
                    case expr_state::cmp_lt_or_lte:
                    {
                        switch(*p_)
                        {
                            case '=':
                                push_token(token<Json>(resources.get_lte_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                ++p_;
                                ++column_;
                                break;
                            default:
                                push_token(token<Json>(resources.get_lt_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                break;
                        }
                        break;
                    }
                    case expr_state::cmp_gt_or_gte:
                    {
                        switch(*p_)
                        {
                            case '=':
                                push_token(token<Json>(resources.get_gte_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                ++p_;
                                ++column_;
                                break;
                            default:
                                push_token(token<Json>(resources.get_gt_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                break;
                        }
                        break;
                    }
                    case expr_state::cmp_eq:
                    {
                        switch(*p_)
                        {
                            case '=':
                                push_token(token<Json>(resources.get_eq_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_comparator;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::cmp_ne:
                    {
                        switch(*p_)
                        {
                            case '=':
                                push_token(token<Json>(resources.get_ne_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_comparator;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_dot:
                    {
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case '.':
                                state_stack.pop_back(); // expect_dot
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_dot;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_pipe_or_or:
                    {
                        switch(*p_)
                        {
                            case '|':
                                push_token(token<Json>(resources.get_or_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                ++p_;
                                ++column_;
                                break;
                            default:
                                push_token(token<Json>(pipe_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); 
                                break;
                        }
                        break;
                    }
                    case expr_state::expect_and:
                    {
                        switch(*p_)
                        {
                            case '&':
                                push_token(token<Json>(resources.get_and_operator()), resources, output_stack, ec);
                                push_token(token<Json>(current_node_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back(); // expect_and
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_and;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::multi_select_list:
                    {
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case ',':
                                JSONCONS_ASSERT(!context_stack.empty());
                                push_token(token<Json>(separator_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.push_back(expr_state::lhs_expression);
                                ++p_;
                                ++column_;
                                break;
                            case '[':
                                state_stack.push_back(expr_state::lhs_expression);
                                break;
                            case '.':
                                state_stack.push_back(expr_state::sub_expression);
                                ++p_;
                                ++column_;
                                break;
                            case '|':
                            {
                                ++p_;
                                ++column_;
                                state_stack.push_back(expr_state::lhs_expression);
                                state_stack.push_back(expr_state::expect_pipe_or_or);
                                break;
                            }
                            case ']':
                            {
                                push_token(token<Json>(end_multi_select_list_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();

                                ++p_;
                                ++column_;
                                break;
                            }
                            default:
                                ec = jmespath_errc::expected_rbracket;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::filter:
                    {
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case ']':
                            {
                                push_token(token<Json>(end_filter_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.pop_back();
                                ++p_;
                                ++column_;
                                break;
                            }
                            default:
                                ec = jmespath_errc::expected_rbracket;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_rbrace:
                    {
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case ',':
                                push_token(token<Json>(separator_arg), resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                state_stack.back() = expr_state::key_val_expr; 
                                ++p_;
                                ++column_;
                                break;
                            case '[':
                            case '{':
                                state_stack.push_back(expr_state::lhs_expression);
                                break;
                            case '.':
                                state_stack.push_back(expr_state::sub_expression);
                                ++p_;
                                ++column_;
                                break;
                            case '}':
                            {
                                state_stack.pop_back();
                                push_token(end_multi_select_hash_arg, resources, output_stack, ec);
                                if (ec) {return jmespath_expression{};}
                                ++p_;
                                ++column_;
                                break;
                            }
                            default:
                                ec = jmespath_errc::expected_rbrace;
                                return jmespath_expression{};
                        }
                        break;
                    }
                    case expr_state::expect_colon:
                    {
                        switch(*p_)
                        {
                            case ' ':case '\t':case '\r':case '\n':
                                advance_past_space_character();
                                break;
                            case ':':
                                state_stack.back() = expr_state::expect_rbrace;
                                state_stack.push_back(expr_state::lhs_expression);
                                ++p_;
                                ++column_;
                                break;
                            default:
                                ec = jmespath_errc::expected_colon;
                                return jmespath_expression{};
                        }
                        break;
                    }
                }
                
            }

            if (state_stack.empty())
            {
                ec = jmespath_errc::syntax_error;
                return jmespath_expression{};
            }
            while (state_stack.size() > 1)
            {
                switch (state_stack.back())
                {
                    case expr_state::rhs_expression:
                        state_stack.pop_back();
                        JSONCONS_ASSERT(!context_stack.empty());
                        context_stack.pop_back();
                        break;
                    case expr_state::substitute_variable:
                    {
                        push_token(token<Json>{variable_binding_arg, buffer},
                            resources, output_stack, ec);
                        if (ec)
                        {
                            return jmespath_expression{};
                        }
                        buffer.clear();
                        state_stack.pop_back();
                        break;
                    }
                    case expr_state::val_expr:
                        push_token(resources.create_expression(identifier_selector(buffer)), resources, output_stack, ec);
                        if (ec) {return jmespath_expression{};}
                        state_stack.pop_back(); 
                        break;
                    case expr_state::identifier_or_function_expr:
                        push_token(resources.create_expression(identifier_selector(buffer)), resources, output_stack, ec);
                        if (ec) {return jmespath_expression{};}
                        state_stack.pop_back(); 
                        break;
                    case expr_state::unquoted_string: 
                        state_stack.pop_back(); 
                        break;
                    default:
                        ec = jmespath_errc::syntax_error;
                        return jmespath_expression{};
                        break;
                }
            }

            if (!(state_stack.back() == expr_state::rhs_expression))
            {
                ec = jmespath_errc::unexpected_end_of_input;
                return jmespath_expression{};
            }

            state_stack.pop_back();
            JSONCONS_ASSERT(!context_stack.empty());
            context_stack.pop_back();
            
            push_token(end_of_expression_arg, resources, output_stack, ec);
            if (ec) {return jmespath_expression{};}

            JSONCONS_ASSERT(context_stack.empty());
            
            if (output_stack.front().type() != token_kind::literal)
            {
                output_stack.insert(output_stack.begin(), token<Json>{current_node_arg});
            }
            
            return jmespath_expression{ std::move(resources), std::move(output_stack) };
        }

        void advance_past_space_character()
        {
            switch (*p_)
            {
                case ' ':case '\t':
                    ++p_;
                    ++column_;
                    break;
                case '\r':
                    if (p_+1 < end_input_)
                    {
                        if (*(p_ + 1) == '\n')
                            ++p_;
                    }
                    ++line_;
                    column_ = 1;
                    ++p_;
                    break;
                case '\n':
                    ++line_;
                    column_ = 1;
                    ++p_;
                    break;
                default:
                    break;
            }
        }

        void unwind_rparen(std::vector<token<Json>>& output_stack, std::error_code& ec)
        {
            auto it = operator_stack_.rbegin();
            while (it != operator_stack_.rend() && !(*it).is_lparen())
            {
                output_stack.push_back(std::move(*it));
                ++it;
            }
            if (it == operator_stack_.rend())
            {
                ec = jmespath_errc::unbalanced_parentheses;
                return;
            }
            ++it;
            operator_stack_.erase(it.base(),operator_stack_.end());
            if (output_stack.back().is_expression())
            {
                output_stack.push_back(token<Json>(pipe_arg));
            }
        }

        void push_token(token<Json>&& tok, static_resources& resources, 
            std::vector<token<Json>>& output_stack, std::error_code& ec)
        {
            switch (tok.type())
            {
                case token_kind::end_filter:
                {
                    unwind_rparen(output_stack, ec);
                    std::vector<token<Json>> toks;
                    auto it = output_stack.rbegin();
                    while (it != output_stack.rend() && (*it).type() != token_kind::begin_filter)
                    {
                        toks.emplace_back(std::move(*it));
                        ++it;
                    }
                    if (it == output_stack.rend())
                    {
                        ec = jmespath_errc::unbalanced_braces;
                        return;
                    }
                    if (toks.back().type() != token_kind::literal)
                    {
                        toks.emplace_back(current_node_arg);
                    }
                    std::reverse(toks.begin(), toks.end());
                    ++it;
                    output_stack.erase(it.base(),output_stack.end());

                    if (!output_stack.empty() && output_stack.back().is_projection() && 
                        (tok.precedence_level() < output_stack.back().precedence_level() ||
                        (tok.precedence_level() == output_stack.back().precedence_level() && tok.is_right_associative())))
                    {
                        output_stack.back().expression_->add_expression(resources.create_expression(filter_expression(std::move(toks))));
                    }
                    else
                    {
                        output_stack.push_back(resources.create_expression(filter_expression(std::move(toks))));
                    }
                    break;
                }
                case token_kind::end_multi_select_list:
                {
                    unwind_rparen(output_stack, ec);
                    std::vector<std::vector<token<Json>>> vals;
                    auto it = output_stack.rbegin();
                    while (it != output_stack.rend() && (*it).type() != token_kind::begin_multi_select_list)
                    {
                        std::vector<token<Json>> toks;
                        do
                        {
                            toks.emplace_back(std::move(*it));
                            ++it;
                        } while (it != output_stack.rend() && (*it).type() != token_kind::begin_multi_select_list && (*it).type() != token_kind::separator);
                        if ((*it).type() == token_kind::separator)
                        {
                            ++it;
                        }
                        if (toks.back().type() != token_kind::literal)
                        {
                            toks.emplace_back(current_node_arg);
                        }
                        std::reverse(toks.begin(), toks.end());
                        vals.emplace_back(std::move(toks));
                    }
                    if (it == output_stack.rend())
                    {
                        ec = jmespath_errc::unbalanced_braces;
                        return;
                    }
                    ++it;
                    output_stack.erase(it.base(),output_stack.end());
                    std::reverse(vals.begin(), vals.end());
                    if (!output_stack.empty() && output_stack.back().is_projection() && 
                        (tok.precedence_level() < output_stack.back().precedence_level() ||
                        (tok.precedence_level() == output_stack.back().precedence_level() && tok.is_right_associative())))
                    {
                        output_stack.back().expression_->add_expression(resources.create_expression(multi_select_list(std::move(vals))));
                    }
                    else
                    {
                        output_stack.push_back(resources.create_expression(multi_select_list(std::move(vals))));
                    }
                    break;
                }
                case token_kind::end_multi_select_hash:
                {
                    unwind_rparen(output_stack, ec);
                    std::vector<key_tokens> key_toks;
                    auto it = output_stack.rbegin();
                    while (it != output_stack.rend() && (*it).type() != token_kind::begin_multi_select_hash)
                    {
                        std::vector<token<Json>> toks;
                        do
                        {
                            toks.emplace_back(std::move(*it));
                            ++it;
                        } while (it != output_stack.rend() && (*it).type() != token_kind::key);
                        JSONCONS_ASSERT((*it).is_key());
                        auto key = std::move((*it).key_);
                        ++it;
                        if ((*it).type() == token_kind::separator)
                        {
                            ++it;
                        }
                        if (toks.back().type() != token_kind::literal)
                        {
                            toks.emplace_back(current_node_arg);
                        }
                        std::reverse(toks.begin(), toks.end());
                        key_toks.emplace_back(std::move(key), std::move(toks));
                    }
                    if (it == output_stack.rend())
                    {
                        ec = jmespath_errc::unbalanced_braces;
                        return;
                    }
                    std::reverse(key_toks.begin(), key_toks.end());
                    ++it;
                    output_stack.erase(it.base(),output_stack.end());

                    if (!output_stack.empty() && output_stack.back().is_projection() && 
                        (tok.precedence_level() < output_stack.back().precedence_level() ||
                        (tok.precedence_level() == output_stack.back().precedence_level() && tok.is_right_associative())))
                    {
                        output_stack.back().expression_->add_expression(resources.create_expression(multi_select_hash(std::move(key_toks))));
                    }
                    else
                    {
                        output_stack.push_back(resources.create_expression(multi_select_hash(std::move(key_toks))));
                    }
                    break;
                }
                case token_kind::end_expression_type:
                {
                    std::vector<token<Json>> toks;
                    auto it = output_stack.rbegin();
                    while (it != output_stack.rend() && (*it).type() != token_kind::begin_expression_type)
                    {
                        toks.emplace_back(std::move(*it));
                        ++it;
                    }
                    if (it == output_stack.rend())
                    {
                        JSONCONS_THROW(json_runtime_error<std::runtime_error>("Unbalanced braces"));
                    }
                    if (toks.back().type() != token_kind::literal)
                    {
                        toks.emplace_back(current_node_arg);
                    }
                    std::reverse(toks.begin(), toks.end());
                    output_stack.erase(it.base(),output_stack.end());
                    output_stack.push_back(resources.create_expression(function_expression(std::move(toks))));
                    break;
                }
                case token_kind::variable:
                    output_stack.push_back(std::move(tok));
                    break;
                case token_kind::variable_binding:
                    output_stack.push_back(std::move(tok));
                    break;
                case token_kind::literal:
                    if (!output_stack.empty() && output_stack.back().type() == token_kind::current_node)
                    {
                        output_stack.back() = std::move(tok);
                    }
                    else
                    {
                        output_stack.push_back(std::move(tok));
                    }
                    break;
                case token_kind::expression:
                    if (!output_stack.empty() && output_stack.back().is_projection() && 
                        (tok.precedence_level() < output_stack.back().precedence_level() ||
                        (tok.precedence_level() == output_stack.back().precedence_level() && tok.is_right_associative())))
                    {
                        output_stack.back().expression_->add_expression(tok.expression_);
                    }
                    else
                    {
                        output_stack.push_back(std::move(tok));
                    }
                    break;
                case token_kind::rparen:
                    {
                        unwind_rparen(output_stack, ec);
                        break;
                    }
                case token_kind::end_function:
                    {
                        unwind_rparen(output_stack, ec);
                        std::vector<token<Json>> toks;
                        auto it = output_stack.rbegin();
                        std::size_t arg_count = 0;
                        while (it != output_stack.rend() && (*it).type() != token_kind::function)
                        {
                            if ((*it).type() == token_kind::argument)
                            {
                                ++arg_count;
                            }
                            toks.emplace_back(std::move(*it));
                            ++it;
                        }
                        if (it == output_stack.rend())
                        {
                            ec = jmespath_errc::unbalanced_parentheses;
                            return;
                        }
                        if ((*it).arity() && arg_count != *((*it).arity()))
                        {
                            ec = jmespath_errc::invalid_arity;
                            return;
                        }
                        if (arg_count == 0)
                        {
                            toks.emplace_back(std::move(*it));
                            ++it;
                            output_stack.erase(it.base(), output_stack.end());
                            output_stack.push_back(resources.create_expression(function_expression(std::move(toks))));
                            break;
                        }
                        if (toks.back().type() != token_kind::literal)
                        {
                            toks.emplace_back(current_node_arg);
                        }
                        std::reverse(toks.begin(), toks.end());
                        toks.push_back(std::move(*it));
                        ++it;
                        output_stack.erase(it.base(),output_stack.end());

                        if (!output_stack.empty() && output_stack.back().is_projection() && 
                            (tok.precedence_level() < output_stack.back().precedence_level() ||
                            (tok.precedence_level() == output_stack.back().precedence_level() && tok.is_right_associative())))
                        {
                            output_stack.back().expression_->add_expression(resources.create_expression(function_expression(std::move(toks))));
                        }
                        else
                        {
                            output_stack.push_back(resources.create_expression(function_expression(std::move(toks))));
                        }
                        break;
                    }
                case token_kind::end_of_expression:
                    {
                        auto it = operator_stack_.rbegin();
                        while (it != operator_stack_.rend())
                        {
                            output_stack.push_back(std::move(*it));
                            ++it;
                        }
                        operator_stack_.clear();
                        break;
                    }
                case token_kind::unary_operator:
                case token_kind::binary_operator:
                {
                    if (operator_stack_.empty() || operator_stack_.back().is_lparen())
                    {
                        operator_stack_.emplace_back(std::move(tok));
                    }
                    else if (tok.precedence_level() < operator_stack_.back().precedence_level()
                             || (tok.precedence_level() == operator_stack_.back().precedence_level() && tok.is_right_associative()))
                    {
                        operator_stack_.emplace_back(std::move(tok));
                    }
                    else
                    {
                        auto it = operator_stack_.rbegin();
                        while (it != operator_stack_.rend() && (*it).is_operator()
                               && (tok.precedence_level() > (*it).precedence_level()
                             || (tok.precedence_level() == (*it).precedence_level() && tok.is_right_associative())))
                        {
                            output_stack.push_back(std::move(*it));
                            ++it;
                        }

                        operator_stack_.erase(it.base(),operator_stack_.end());
                        operator_stack_.emplace_back(std::move(tok));
                    }
                    break;
                }
                case token_kind::separator:
                {
                    unwind_rparen(output_stack, ec);
                    output_stack.push_back(std::move(tok));
                    operator_stack_.emplace_back(token<Json>(lparen_arg));
                    break;
                }
                case token_kind::begin_filter:
                    output_stack.push_back(std::move(tok));
                    operator_stack_.emplace_back(token<Json>(lparen_arg));
                    break;
                case token_kind::begin_multi_select_list:
                    output_stack.push_back(std::move(tok));
                    operator_stack_.emplace_back(token<Json>(lparen_arg));
                    break;
                case token_kind::begin_multi_select_hash:
                    output_stack.push_back(std::move(tok));
                    operator_stack_.emplace_back(token<Json>(lparen_arg));
                    break;
                case token_kind::function:
                    output_stack.push_back(std::move(tok));
                    operator_stack_.emplace_back(token<Json>(lparen_arg));
                    break;
                case token_kind::current_node:
                    output_stack.push_back(std::move(tok));
                    break;
                case token_kind::key:
                case token_kind::pipe:
                case token_kind::argument:
                case token_kind::begin_expression_type:
                    output_stack.push_back(std::move(tok));
                    break;
                case token_kind::lparen:
                    operator_stack_.emplace_back(std::move(tok));
                    break;
                default:
                    break;
            }
        }

        uint32_t append_to_codepoint(uint32_t cp, int c, std::error_code& ec)
        {
            cp *= 16;
            if (c >= '0'  &&  c <= '9')
            {
                cp += c - '0';
            }
            else if (c >= 'a'  &&  c <= 'f')
            {
                cp += c - 'a' + 10;
            }
            else if (c >= 'A'  &&  c <= 'F')
            {
                cp += c - 'A' + 10;
            }
            else
            {
                ec = jmespath_errc::invalid_codepoint;
            }
            return cp;
        }
    };

} // namespace detail

    template <typename Json>
    using jmespath_expression = typename jsoncons::jmespath::detail::jmespath_evaluator<Json>::jmespath_expression;

    template <typename Json>
    Json search(const Json& doc, const typename Json::string_view_type& path)
    {
        jsoncons::jmespath::detail::jmespath_evaluator<Json> evaluator;
        std::error_code ec;
        auto expr = evaluator.compile(path.data(), path.size(), 
            jsoncons::jmespath::custom_functions<Json>{}, ec);
        if (ec)
        {
            JSONCONS_THROW(jmespath_error(ec, evaluator.line(), evaluator.column()));
        }
        auto result = expr.evaluate(doc, ec);
        if (ec)
        {
            JSONCONS_THROW(jmespath_error(ec));
        }
        return result;
    }

    template <typename Json>
    Json search(const Json& doc, const typename Json::string_view_type& path, std::error_code& ec)
    {
        jsoncons::jmespath::detail::jmespath_evaluator<Json> evaluator;
        auto expr = evaluator.compile(path.data(), path.size(), 
            jsoncons::jmespath::custom_functions<Json>{}, ec);
        if (ec)
        {
            return Json::null();
        }
        auto result = expr.evaluate(doc, ec);
        if (ec)
        {
            return Json::null();
        }
        return result;
    }

    template <typename Json>
    jmespath_expression<Json> make_expression(const typename Json::string_view_type& expr,
        const jsoncons::jmespath::custom_functions<Json>& funcs = jsoncons::jmespath::custom_functions<Json>())
    {
        jsoncons::jmespath::detail::jmespath_evaluator<Json> evaluator{};
        std::error_code ec;
        auto compiled = evaluator.compile(expr.data(), expr.size(), funcs, ec);
        if (ec)
        {
            JSONCONS_THROW(jmespath_error(ec, evaluator.line(), evaluator.column()));
        }
        return compiled;
    }

    template <typename Json>
    jmespath_expression<Json> make_expression(const typename Json::string_view_type& expr,
        std::error_code& ec)
    {
        jsoncons::jmespath::detail::jmespath_evaluator<Json> evaluator{};
        return evaluator.compile(expr.data(), expr.size(), 
            jsoncons::jmespath::custom_functions<Json>{}, ec);
    }

    template <typename Json>
    jmespath_expression<Json> make_expression(const typename Json::string_view_type& expr,
        const jsoncons::jmespath::custom_functions<Json>& funcs,
        std::error_code& ec)
    {
        jsoncons::jmespath::detail::jmespath_evaluator<Json> evaluator{};
        return evaluator.compile(expr.data(), expr.size(), funcs, ec);
    }

} // namespace jmespath
} // namespace jsoncons

#endif // JSONCONS_EXT_JMESPATH_JMESPATH_HPP
