// Copyright (c) 2014-2022 Dr. Colin Hirsch and Daniel Frey
// Please see LICENSE for license or visit https://github.com/taocpp/PEGTL/

#include "test.hpp"
#include "verify_meta.hpp"

namespace TAO_PEGTL_NAMESPACE
{
   template< typename... Rules >
   struct strange
   {
      using rule_t = strange;
      using subs_t = type_list< Rules... >;

      static_assert( sizeof...( Rules ) > 1 );

      // Pretend to be a rule that consumes on success by itself _and_ has sub-rules,
      // to test a supported combination even though it is not frequently encountered.
   };

   template< typename Name, typename... Rules >
   struct analyze_traits< Name, strange< Rules... > >
      : analyze_any_traits< Rules... >
   {};

   void unit_test()
   {
      verify_analyze< strange< alpha, digit > >( __LINE__, __FILE__, true, false );
      verify_analyze< strange< opt< alpha >, opt< digit > > >( __LINE__, __FILE__, true, false );

      verify_analyze< strange< star< star< alpha > >, digit > >( __LINE__, __FILE__, true, true );
      verify_analyze< strange< digit, star< star< alpha > > > >( __LINE__, __FILE__, true, true );

      verify_analyze< eof >( __LINE__, __FILE__, false, false );
      verify_analyze< eolf >( __LINE__, __FILE__, false, false );
      verify_analyze< success >( __LINE__, __FILE__, false, false );
      verify_analyze< failure >( __LINE__, __FILE__, true, false );

      // clang-format off
      {
         struct tst : star< tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : plus< tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : seq< eof, at< digit >, tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );  // This is a false positive.
      }
      {
         struct tst : sor< digit, seq< at< digit >, tst > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );  // This is a false positive.
      }
      {
         struct tst : sor< digit, seq< opt< digit >, tst > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : sor< digit, tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : at< any > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : at< tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : at< any, tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : not_at< any > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : opt< tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : opt< any, tst > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct rec : sor< seq< rec, alpha >, alpha > {};
         verify_analyze< rec >( __LINE__, __FILE__, true, true );
      }
      {
         struct bar;
         struct foo : seq< digit, bar > {};
         struct bar : plus< foo > {};
         verify_analyze< seq< any, bar > >( __LINE__, __FILE__, true, false );
      }
      {
         struct bar;
         struct foo : seq< bar, digit > {};
         struct bar : plus< foo > {};
         verify_analyze< seq< bar, any > >( __LINE__, __FILE__, true, true );
      }
      {
         struct bar;
         struct foo : sor< digit, bar > {};
         struct bar : plus< foo > {};
         verify_analyze< bar >( __LINE__, __FILE__, false, true );
         verify_analyze< foo >( __LINE__, __FILE__, false, true );
         verify_analyze< sor< any, bar > >( __LINE__, __FILE__, false, true );
      }
      {
         // Excerpt from the Lua 5.3 grammar:
         //  prefixexp ::= var | functioncall | ‘(’ exp ‘)’
         //  functioncall ::=  prefixexp args | prefixexp ‘:’ Name args
         //  var ::=  Name | prefixexp ‘[’ exp ‘]’ | prefixexp ‘.’ Name
         // Simplified version, equivalent regarding consumption of input:
         struct var;
         struct fun;
         struct exp : sor< var, fun, seq< any, exp, any > > {};
         struct fun : seq< exp, any > {};
         struct var : sor< any, seq< exp, any, exp >, seq< exp, any > > {};
         verify_analyze< exp >( __LINE__, __FILE__, true, true );
         verify_analyze< fun >( __LINE__, __FILE__, true, true );
         verify_analyze< var >( __LINE__, __FILE__, true, true );
      }
      {
         struct exp : sor< exp, seq< any, exp > > {};
         verify_analyze< exp >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : until< any > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : until< star< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : until< any, star< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, true );
      }
      {
         struct tst : until< star< any >, star< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : until< any, any > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : until< star< any >, any > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : plus< plus< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : star< star< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : plus< star< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : plus< opt< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : star< opt< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : star< plus< opt< any > > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : list< any, any > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : list< star< any >, any > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : list< any, opt< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : list< star< any >, opt< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
#if defined( __cpp_exceptions )
      {
         struct tst : list_must< any, any > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : list_must< star< any >, any > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : list_must< any, opt< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, true, false );
      }
      {
         struct tst : list_must< star< any >, opt< any > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
#endif
      {
         struct tst : plus< pad_opt< alpha, digit > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      {
         struct tst : rep< 42, opt< alpha > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, false );
      }
      {
         struct tst : rep_min< 42, opt< alpha > > {};
         verify_analyze< tst >( __LINE__, __FILE__, false, true );
      }
      // clang-format on
   }

}  // namespace TAO_PEGTL_NAMESPACE

#include "main.hpp"
