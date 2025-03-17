// Copyright (c) 2015-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#ifndef TAO_PEGTL_SRC_EXAMPLES_PEGTL_LUA53_HPP
#define TAO_PEGTL_SRC_EXAMPLES_PEGTL_LUA53_HPP

#if !defined( __cpp_exceptions )
#error "Exception support required for lua53.hpp"
#else

#include <tao/pegtl.hpp>
#include <tao/pegtl/contrib/raw_string.hpp>

namespace lua53
{
   // PEGTL grammar for the Lua 5.3.0 lexer and parser.
   //
   // The grammar here is not very similar to the grammar
   // in the Lua reference documentation on which it is based
   // which is due to multiple causes.
   //
   // The main difference is that this grammar includes really
   // "everything", not just the structural parts from the
   // reference documentation:
   // - The PEG-approach combines lexer and parser; this grammar
   //   handles comments and tokenisation.
   // - The operator precedence and associativity are reflected
   //   in the structure of this grammar.
   // - All details for all types of literals are included, with
   //   escape-sequences for literal strings, and long literals.
   //
   // The second necessary difference is that all left-recursion
   // had to be eliminated.
   //
   // In some places the grammar was optimised to require as little
   // back-tracking as possible, most prominently for expressions.
   // The original grammar contains the following production rules:
   //
   //   prefixexp ::= var | functioncall | ‘(’ exp ‘)’
   //   functioncall ::=  prefixexp args | prefixexp ‘:’ Name args
   //   var ::=  Name | prefixexp ‘[’ exp ‘]’ | prefixexp ‘.’ Name
   //
   // We need to eliminate the left-recursion, and we also want to
   // remove the ambiguity between function calls and variables,
   // i.e. the fact that we can have expressions like
   //
   //   ( a * b ).c()[ d ].e:f()
   //
   // where only the last element decides between function call and
   // variable, making it necessary to parse the whole thing again
   // if we chose wrong at the beginning.
   // First we eliminate prefixexp and obtain:
   //
   //   functioncall ::=  ( var | functioncall | ‘(’ exp ‘)’ ) ( args | ‘:’ Name args )
   //   var ::=  Name | ( var | functioncall | ‘(’ exp ‘)’ ) ( ‘[’ exp ‘]’ | ‘.’ Name )
   //
   // Next we split function_call and variable into a first part,
   // a "head", or how they can start, and a second part, the "tail",
   // which, in a sequence like above, is the final deciding part:
   //
   //   vartail ::= '[' exp ']' | '.' Name
   //   varhead ::= Name | '(' exp ')' vartail
   //   functail ::= args | ':' Name args
   //   funchead ::= Name | '(' exp ')'
   //
   // This allows us to rewrite var and function_call as follows.
   //
   //   var ::= varhead { { functail } vartail }
   //   function_call ::= funchead [ { vartail } functail ]
   //
   // Finally we can define a single expression that takes care
   // of var, function_call, and expressions in a bracket:
   //
   //   chead ::= '(' exp ')' | Name
   //   combined ::= chead { functail | vartail }
   //
   // Such a combined expression starts with a bracketed
   // expression or a name, and continues with an arbitrary
   // number of functail and/or vartail parts, all in a one
   // grammar rule without back-tracking.
   //
   // The rule expr_thirteen below implements "combined".
   //
   // Another issue of interest when writing a PEG is how to
   // manage the separators, the white-space and comments that
   // can occur in many places; in the classical two-stage
   // lexer-parser approach the lexer would have taken care of
   // this, but here we use the PEG approach that combines both.
   //
   // In the following grammar most rules adopt the convention
   // that they take care of "internal padding", i.e. spaces
   // and comments that can occur within the rule, but not
   // "external padding", i.e. they don't start or end with
   // a rule that "eats up" all extra padding (spaces and
   // comments). In some places, where it is more efficient,
   // right padding is used.

   // clang-format off
   struct short_comment : tao::pegtl::until< tao::pegtl::eolf > {};
   struct long_string : tao::pegtl::raw_string< '[', '=', ']' > {};
   struct comment : tao::pegtl::disable< tao::pegtl::two< '-' >, tao::pegtl::sor< long_string, short_comment > > {};

   struct sep : tao::pegtl::sor< tao::pegtl::ascii::space, comment > {};
   struct seps : tao::pegtl::star< sep > {};

   struct str_and : TAO_PEGTL_STRING( "and" ) {};
   struct str_break : TAO_PEGTL_STRING( "break" ) {};
   struct str_do : TAO_PEGTL_STRING( "do" ) {};
   struct str_else : TAO_PEGTL_STRING( "else" ) {};
   struct str_elseif : TAO_PEGTL_STRING( "elseif" ) {};
   struct str_end : TAO_PEGTL_STRING( "end" ) {};
   struct str_false : TAO_PEGTL_STRING( "false" ) {};
   struct str_for : TAO_PEGTL_STRING( "for" ) {};
   struct str_function : TAO_PEGTL_STRING( "function" ) {};
   struct str_goto : TAO_PEGTL_STRING( "goto" ) {};
   struct str_if : TAO_PEGTL_STRING( "if" ) {};
   struct str_in : TAO_PEGTL_STRING( "in" ) {};
   struct str_local : TAO_PEGTL_STRING( "local" ) {};
   struct str_nil : TAO_PEGTL_STRING( "nil" ) {};
   struct str_not : TAO_PEGTL_STRING( "not" ) {};
   struct str_or : TAO_PEGTL_STRING( "or" ) {};
   struct str_repeat : TAO_PEGTL_STRING( "repeat" ) {};
   struct str_return : TAO_PEGTL_STRING( "return" ) {};
   struct str_then : TAO_PEGTL_STRING( "then" ) {};
   struct str_true : TAO_PEGTL_STRING( "true" ) {};
   struct str_until : TAO_PEGTL_STRING( "until" ) {};
   struct str_while : TAO_PEGTL_STRING( "while" ) {};

   // Note that 'elseif' precedes 'else' in order to prevent only matching
   // the "else" part of an "elseif" and running into an error in the
   // 'keyword' rule.

   template< typename Key >
   struct key : tao::pegtl::seq< Key, tao::pegtl::not_at< tao::pegtl::identifier_other > > {};

   struct sor_keyword : tao::pegtl::sor< str_and, str_break, str_do, str_elseif, str_else, str_end, str_false, str_for, str_function, str_goto, str_if, str_in, str_local, str_nil, str_not, str_repeat, str_return, str_then, str_true, str_until, str_while > {};

   struct key_and : key< str_and > {};
   struct key_break : key< str_break > {};
   struct key_do : key< str_do > {};
   struct key_else : key< str_else > {};
   struct key_elseif : key< str_elseif > {};
   struct key_end : key< str_end > {};
   struct key_false : key< str_false > {};
   struct key_for : key< str_for > {};
   struct key_function : key< str_function > {};
   struct key_goto : key< str_goto > {};
   struct key_if : key< str_if > {};
   struct key_in : key< str_in > {};
   struct key_local : key< str_local > {};
   struct key_nil : key< str_nil > {};
   struct key_not : key< str_not > {};
   struct key_or : key< str_or > {};
   struct key_repeat : key< str_repeat > {};
   struct key_return : key< str_return > {};
   struct key_then : key< str_then > {};
   struct key_true : key< str_true > {};
   struct key_until : key< str_until > {};
   struct key_while : key< str_while > {};

   struct keyword : key< sor_keyword > {};

   template< typename R >
   struct pad : tao::pegtl::pad< R, sep > {};

   struct name : tao::pegtl::seq< tao::pegtl::not_at< keyword >, tao::pegtl::identifier > {};

   struct single : tao::pegtl::one< 'a', 'b', 'f', 'n', 'r', 't', 'v', '\\', '"', '\'', '0', '\n' > {};
   struct spaces : tao::pegtl::seq< tao::pegtl::one< 'z' >, tao::pegtl::star< tao::pegtl::space > > {};
   struct hexbyte : tao::pegtl::if_must< tao::pegtl::one< 'x' >, tao::pegtl::xdigit, tao::pegtl::xdigit > {};
   struct decbyte : tao::pegtl::if_must< tao::pegtl::digit, tao::pegtl::rep_opt< 2, tao::pegtl::digit > > {};
   struct unichar : tao::pegtl::if_must< tao::pegtl::one< 'u' >, tao::pegtl::one< '{' >, tao::pegtl::plus< tao::pegtl::xdigit >, tao::pegtl::one< '}' > > {};
   struct escaped : tao::pegtl::if_must< tao::pegtl::one< '\\' >, tao::pegtl::sor< hexbyte, decbyte, unichar, single, spaces > > {};
   struct regular : tao::pegtl::not_one< '\r', '\n' > {};
   struct character : tao::pegtl::sor< escaped, regular > {};

   template< char Q >
   struct short_string : tao::pegtl::if_must< tao::pegtl::one< Q >, tao::pegtl::until< tao::pegtl::one< Q >, character > > {};
   struct literal_string : tao::pegtl::sor< short_string< '"' >, short_string< '\'' >, long_string > {};

   template< typename E >
   struct exponent : tao::pegtl::opt_must< E, tao::pegtl::opt< tao::pegtl::one< '+', '-' > >, tao::pegtl::plus< tao::pegtl::digit > > {};

   template< typename D, typename E >
   struct numeral_three : tao::pegtl::seq< tao::pegtl::if_must< tao::pegtl::one< '.' >, tao::pegtl::plus< D > >, exponent< E > > {};
   template< typename D, typename E >
   struct numeral_two : tao::pegtl::seq< tao::pegtl::plus< D >, tao::pegtl::opt< tao::pegtl::one< '.' >, tao::pegtl::star< D > >, exponent< E > > {};
   template< typename D, typename E >
   struct numeral_one : tao::pegtl::sor< numeral_two< D, E >, numeral_three< D, E > > {};

   struct decimal : numeral_one< tao::pegtl::digit, tao::pegtl::one< 'e', 'E' > > {};
   struct hexadecimal : tao::pegtl::if_must< tao::pegtl::istring< '0', 'x' >, numeral_one< tao::pegtl::xdigit, tao::pegtl::one< 'p', 'P' > > > {};
   struct numeral : tao::pegtl::sor< hexadecimal, decimal > {};

   struct label_statement : tao::pegtl::if_must< tao::pegtl::two< ':' >, seps, name, seps, tao::pegtl::two< ':' > > {};
   struct goto_statement : tao::pegtl::if_must< key_goto, seps, name > {};

   struct statement;
   struct expression;

   struct name_list : tao::pegtl::list< name, tao::pegtl::one< ',' >, sep > {};
   struct name_list_must : tao::pegtl::list_must< name, tao::pegtl::one< ',' >, sep > {};
   struct expr_list_must : tao::pegtl::list_must< expression, tao::pegtl::one< ',' >, sep > {};

   struct statement_return : tao::pegtl::seq< tao::pegtl::pad_opt< expr_list_must, sep >, tao::pegtl::opt< tao::pegtl::one< ';' >, seps > > {};

   template< typename E >
   struct statement_list : tao::pegtl::seq< seps, tao::pegtl::until< tao::pegtl::sor< E, tao::pegtl::if_must< key_return, statement_return, E > >, statement, seps > > {};

   template< char O, char... N >
   struct op_one : tao::pegtl::seq< tao::pegtl::one< O >, tao::pegtl::at< tao::pegtl::not_one< N... > > > {};
   template< char O, char P, char... N >
   struct op_two : tao::pegtl::seq< tao::pegtl::string< O, P >, tao::pegtl::at< tao::pegtl::not_one< N... > > > {};

   struct table_field_one : tao::pegtl::if_must< tao::pegtl::one< '[' >, seps, expression, seps, tao::pegtl::one< ']' >, seps, tao::pegtl::one< '=' >, seps, expression > {};
   struct table_field_two : tao::pegtl::if_must< tao::pegtl::seq< name, seps, op_one< '=', '=' > >, seps, expression > {};
   struct table_field : tao::pegtl::sor< table_field_one, table_field_two, expression > {};
   struct table_field_list : tao::pegtl::list_tail< table_field, tao::pegtl::one< ',', ';' >, sep > {};
   struct table_constructor : tao::pegtl::if_must< tao::pegtl::one< '{' >, tao::pegtl::pad_opt< table_field_list, sep >, tao::pegtl::one< '}' > > {};

   struct parameter_list_one : tao::pegtl::seq< name_list, tao::pegtl::opt_must< pad< tao::pegtl::one< ',' > >, tao::pegtl::ellipsis > > {};
   struct parameter_list : tao::pegtl::sor< tao::pegtl::ellipsis, parameter_list_one > {};

   struct function_body : tao::pegtl::seq< tao::pegtl::one< '(' >, tao::pegtl::pad_opt< parameter_list, sep >, tao::pegtl::one< ')' >, seps, statement_list< key_end > > {};
   struct function_literal : tao::pegtl::if_must< key_function, seps, function_body > {};

   struct bracket_expr : tao::pegtl::if_must< tao::pegtl::one< '(' >, seps, expression, seps, tao::pegtl::one< ')' > > {};

   struct function_args_one : tao::pegtl::if_must< tao::pegtl::one< '(' >, tao::pegtl::pad_opt< expr_list_must, sep >, tao::pegtl::one< ')' > > {};
   struct function_args : tao::pegtl::sor< function_args_one, table_constructor, literal_string > {};

   struct variable_tail_one : tao::pegtl::if_must< tao::pegtl::one< '[' >, seps, expression, seps, tao::pegtl::one< ']' > > {};
   struct variable_tail_two : tao::pegtl::if_must< tao::pegtl::seq< tao::pegtl::not_at< tao::pegtl::two< '.' > >, tao::pegtl::one< '.' > >, seps, name > {};
   struct variable_tail : tao::pegtl::sor< variable_tail_one, variable_tail_two > {};

   struct function_call_tail_one : tao::pegtl::if_must< tao::pegtl::seq< tao::pegtl::not_at< tao::pegtl::two< ':' > >, tao::pegtl::one< ':' > >, seps, name, seps, function_args > {};
   struct function_call_tail : tao::pegtl::sor< function_args, function_call_tail_one > {};

   struct variable_head_one : tao::pegtl::seq< bracket_expr, seps, variable_tail > {};
   struct variable_head : tao::pegtl::sor< name, variable_head_one > {};

   struct function_call_head : tao::pegtl::sor< name, bracket_expr > {};

   struct variable : tao::pegtl::seq< variable_head, tao::pegtl::star< tao::pegtl::star< seps, function_call_tail >, seps, variable_tail > > {};
   struct function_call : tao::pegtl::seq< function_call_head, tao::pegtl::plus< tao::pegtl::until< tao::pegtl::seq< seps, function_call_tail >, seps, variable_tail > > > {};

   template< typename S, typename O >
   struct left_assoc : tao::pegtl::seq< S, seps, tao::pegtl::star_must< O, seps, S, seps > > {};
   template< typename S, typename O >
   struct right_assoc : tao::pegtl::seq< S, seps, tao::pegtl::opt_must< O, seps, right_assoc< S, O > > > {};

   struct unary_operators : tao::pegtl::sor< tao::pegtl::one< '-' >,
                                             tao::pegtl::one< '#' >,
                                             op_one< '~', '=' >,
                                             key_not > {};

   struct expr_ten;
   struct expr_thirteen : tao::pegtl::seq< tao::pegtl::sor< bracket_expr, name >, tao::pegtl::star< seps, tao::pegtl::sor< function_call_tail, variable_tail > > > {};
   struct expr_twelve : tao::pegtl::sor< key_nil,
                                         key_true,
                                         key_false,
                                         tao::pegtl::ellipsis,
                                         numeral,
                                         literal_string,
                                         function_literal,
                                         expr_thirteen,
                                         table_constructor > {};
   struct expr_eleven : tao::pegtl::seq< expr_twelve, seps, tao::pegtl::opt< tao::pegtl::one< '^' >, seps, expr_ten, seps > > {};
   struct unary_apply : tao::pegtl::if_must< unary_operators, seps, expr_ten, seps > {};
   struct expr_ten : tao::pegtl::sor< unary_apply, expr_eleven > {};
   struct operators_nine : tao::pegtl::sor< tao::pegtl::two< '/' >,
                                            tao::pegtl::one< '/' >,
                                            tao::pegtl::one< '*' >,
                                            tao::pegtl::one< '%' > > {};
   struct expr_nine : left_assoc< expr_ten, operators_nine > {};
   struct operators_eight : tao::pegtl::sor< tao::pegtl::one< '+' >,
                                             tao::pegtl::one< '-' > > {};
   struct expr_eight : left_assoc< expr_nine, operators_eight > {};
   struct expr_seven : right_assoc< expr_eight, op_two< '.', '.', '.' > > {};
   struct operators_six : tao::pegtl::sor< tao::pegtl::two< '<' >,
                                           tao::pegtl::two< '>' > > {};
   struct expr_six : left_assoc< expr_seven, operators_six > {};
   struct expr_five : left_assoc< expr_six, tao::pegtl::one< '&' > > {};
   struct expr_four : left_assoc< expr_five, op_one< '~', '=' > > {};
   struct expr_three : left_assoc< expr_four, tao::pegtl::one< '|' > > {};
   struct operators_two : tao::pegtl::sor< tao::pegtl::two< '=' >,
                                           tao::pegtl::string< '<', '=' >,
                                           tao::pegtl::string< '>', '=' >,
                                           op_one< '<', '<' >,
                                           op_one< '>', '>' >,
                                           tao::pegtl::string< '~', '=' > > {};
   struct expr_two : left_assoc< expr_three, operators_two > {};
   struct expr_one : left_assoc< expr_two, key_and > {};
   struct expression : left_assoc< expr_one, key_or > {};

   struct do_statement : tao::pegtl::if_must< key_do, statement_list< key_end > > {};
   struct while_statement : tao::pegtl::if_must< key_while, seps, expression, seps, key_do, statement_list< key_end > > {};
   struct repeat_statement : tao::pegtl::if_must< key_repeat, statement_list< key_until >, seps, expression > {};

   struct at_elseif_else_end : tao::pegtl::sor< tao::pegtl::at< key_elseif >, tao::pegtl::at< key_else >, tao::pegtl::at< key_end > > {};
   struct elseif_statement : tao::pegtl::if_must< key_elseif, seps, expression, seps, key_then, statement_list< at_elseif_else_end > > {};
   struct else_statement : tao::pegtl::if_must< key_else, statement_list< key_end > > {};
   struct if_statement : tao::pegtl::if_must< key_if, seps, expression, seps, key_then, statement_list< at_elseif_else_end >, seps, tao::pegtl::until< tao::pegtl::sor< else_statement, key_end >, elseif_statement, seps > > {};

   struct for_statement_one : tao::pegtl::seq< tao::pegtl::one< '=' >, seps, expression, seps, tao::pegtl::one< ',' >, seps, expression, tao::pegtl::pad_opt< tao::pegtl::if_must< tao::pegtl::one< ',' >, seps, expression >, sep > > {};
   struct for_statement_two : tao::pegtl::seq< tao::pegtl::opt_must< tao::pegtl::one< ',' >, seps, name_list_must, seps >, key_in, seps, expr_list_must, seps > {};
   struct for_statement : tao::pegtl::if_must< key_for, seps, name, seps, tao::pegtl::sor< for_statement_one, for_statement_two >, key_do, statement_list< key_end > > {};

   struct assignment_variable_list : tao::pegtl::list_must< variable, tao::pegtl::one< ',' >, sep > {};
   struct assignments_one : tao::pegtl::if_must< tao::pegtl::one< '=' >, seps, expr_list_must > {};
   struct assignments : tao::pegtl::seq< assignment_variable_list, seps, assignments_one > {};
   struct function_name : tao::pegtl::seq< tao::pegtl::list< name, tao::pegtl::one< '.' >, sep >, seps, tao::pegtl::opt_must< tao::pegtl::one< ':' >, seps, name, seps > > {};
   struct function_definition : tao::pegtl::if_must< key_function, seps, function_name, function_body > {};

   struct local_function : tao::pegtl::if_must< key_function, seps, name, seps, function_body > {};
   struct local_variables : tao::pegtl::if_must< name_list_must, seps, tao::pegtl::opt< assignments_one > > {};
   struct local_statement : tao::pegtl::if_must< key_local, seps, tao::pegtl::sor< local_function, local_variables > > {};

   struct semicolon : tao::pegtl::one< ';' > {};
   struct statement : tao::pegtl::sor< semicolon,
                                       assignments,
                                       function_call,
                                       label_statement,
                                       key_break,
                                       goto_statement,
                                       do_statement,
                                       while_statement,
                                       repeat_statement,
                                       if_statement,
                                       for_statement,
                                       function_definition,
                                       local_statement > {};

   struct grammar : tao::pegtl::must< tao::pegtl::opt< tao::pegtl::shebang >, statement_list< tao::pegtl::eof > > {};
   // clang-format on

}  // namespace lua53

#endif
#endif
