/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */

#include <tao/pegtl.hpp>

namespace kqir {

namespace grammar {

namespace peg = tao::pegtl;

namespace sql {

using namespace peg;

struct True : string<'t', 'r', 'u', 'e'> {};
struct False : string<'f', 'a', 'l', 's', 'e'> {};
struct Boolean : sor<True, False> {};

struct Digits : plus<digit> {};
struct NumberExp : seq<one<'e', 'E'>, opt<one<'-', '+'>>, Digits> {};
struct NumberFrac : seq<one<'.'>, Digits> {};
struct Number : seq<opt<one<'-'>>, Digits, opt<NumberFrac>, opt<NumberExp>> {};

struct UnicodeXDigit : list<seq<one<'u'>, rep<4, xdigit>>, one<'\\'>> {};
struct EscapedSingleChar : one<'"', '\\', '/', 'b', 'f', 'n', 'r', 't'> {};
struct EscapedChar : sor<EscapedSingleChar, UnicodeXDigit> {};
struct UnescapedChar : utf8::range<0x20, 0x10FFFF> {};
struct Char : if_then_else<one<'\\'>, EscapedChar, UnescapedChar> {};

struct StringContent : until<at<one<'"'>>, Char> {};
struct String : seq<one<'"'>, StringContent, any> {
  using content = StringContent;  // NOLINT
};

struct Identifier : identifier {};

struct WhiteSpace : one<' ', '\t', '\n', '\r'> {};
template <typename T>
struct WSPad : pad<T, WhiteSpace> {};

struct HasTag : string<'h', 'a', 's', 't', 'a', 'g'> {};
struct HasTagExpr : WSPad<seq<Identifier, WSPad<HasTag>, String>> {};

struct NumericAtomExpr : WSPad<sor<Number, Identifier>> {};
struct NumericCompareOp : sor<string<'!', '='>, string<'<', '='>, string<'>', '='>, one<'=', '<', '>'>> {};
struct NumericCompareExpr : seq<NumericAtomExpr, NumericCompareOp, NumericAtomExpr> {};

struct BooleanAtomExpr : sor<HasTagExpr, NumericCompareExpr, WSPad<Boolean>> {};

struct QueryExpr;

struct ParenExpr : WSPad<seq<one<'('>, QueryExpr, one<')'>>> {};

struct BooleanExpr : sor<BooleanAtomExpr, ParenExpr> {};

struct Not : string<'n', 'o', 't'> {};
struct NotExpr : sor<seq<WSPad<Not>, BooleanExpr>, BooleanExpr> {};

struct And : string<'a', 'n', 'd'> {};
// left recursion elimination
// struct AndExpr : sor<seq<AndExpr, And, NotExpr>, NotExpr> {};
struct AndExpr : seq<NotExpr, star<seq<And, NotExpr>>> {};

struct Or : string<'o', 'r'> {};
// left recursion elimination
// struct OrExpr : sor<seq<OrExpr, Or, AndExpr>, AndExpr> {};
struct OrExpr : seq<AndExpr, star<seq<Or, AndExpr>>> {};

struct QueryExpr : OrExpr {};

struct Select : string<'s', 'e', 'l', 'e', 'c', 't'> {};
struct From : string<'f', 'r', 'o', 'm'> {};

struct Wildcard : one<'*'> {};
struct IdentifierList : seq<Identifier, star<WSPad<one<','>>, Identifier>> {};
struct SelectExpr : WSPad<sor<Wildcard, IdentifierList>> {};
struct FromExpr : WSPad<Identifier> {};

struct Where : string<'w', 'h', 'e', 'r', 'e'> {};
struct OrderBy : seq<string<'o', 'r', 'd', 'e', 'r'>, star<WhiteSpace>, string<'b', 'y'>> {};
struct Asc : string<'a', 's', 'c'> {};
struct Desc : string<'d', 'e', 's', 'c'> {};
struct Limit : string<'l', 'i', 'm', 'i', 't'> {};

struct WhereClause : seq<Where, QueryExpr> {};
struct AscOrDesc : WSPad<sor<Asc, Desc>> {};
struct OrderByClause : seq<OrderBy, WSPad<Identifier>, opt<AscOrDesc>> {};
struct LimitClause : seq<Limit, opt<seq<Digits, one<','>>>, Digits> {};

struct SearchStmt
    : WSPad<seq<Select, SelectExpr, From, FromExpr, opt<WhereClause>, opt<OrderByClause>, opt<LimitClause>>> {};

}  // namespace sql

}  // namespace grammar

}  // namespace kqir
