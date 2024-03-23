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

#pragma once

#include <tao/pegtl.hpp>

#include "common_parser.h"

namespace kqir {

namespace redis_query {

using namespace peg;

struct Field : seq<one<'@'>, Identifier> {};

struct Tag : sor<Identifier, String> {};
struct TagList : seq<one<'{'>, WSPad<Tag>, star<seq<one<'|'>, WSPad<Tag>>>, one<'}'>> {};

struct Inf : seq<opt<one<'+', '-'>>, string<'i', 'n', 'f'>> {};
struct ExclusiveNumber : seq<one<'('>, Number> {};
struct NumericRangePart : sor<Inf, ExclusiveNumber, Number> {};
struct NumericRange : seq<one<'['>, WSPad<NumericRangePart>, WSPad<NumericRangePart>, one<']'>> {};

struct FieldQuery : seq<WSPad<Field>, one<':'>, WSPad<sor<TagList, NumericRange>>> {};

struct QueryExpr;

struct ParenExpr : WSPad<seq<one<'('>, QueryExpr, one<')'>>> {};

struct NotExpr;

struct BooleanExpr : sor<FieldQuery, ParenExpr, NotExpr> {};

struct NotExpr : seq<WSPad<one<'-'>>, BooleanExpr> {};

struct AndExpr : seq<BooleanExpr, plus<BooleanExpr>> {};
struct AndExprP : sor<AndExpr, BooleanExpr> {};

struct OrExpr : seq<AndExprP, plus<seq<one<'|'>, AndExprP>>> {};
struct OrExprP : sor<OrExpr, AndExprP> {};

struct QueryExpr : seq<OrExprP> {};

}  // namespace redis_query

}  // namespace kqir
