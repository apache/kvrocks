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

namespace kqir {

namespace peg = tao::pegtl;

struct True : peg::string<'t', 'r', 'u', 'e'> {};
struct False : peg::string<'f', 'a', 'l', 's', 'e'> {};
struct Boolean : peg::sor<True, False> {};

struct Digits : peg::plus<peg::digit> {};
struct NumberExp : peg::seq<peg::one<'e', 'E'>, peg::opt<peg::one<'-', '+'>>, Digits> {};
struct NumberFrac : peg::seq<peg::one<'.'>, Digits> {};
struct Number : peg::seq<peg::opt<peg::one<'-'>>, Digits, peg::opt<NumberFrac>, peg::opt<NumberExp>> {};

struct UnicodeXDigit : peg::list<peg::seq<peg::one<'u'>, peg::rep<4, peg::xdigit>>, peg::one<'\\'>> {};
struct EscapedSingleChar : peg::one<'"', '\\', 'b', 'f', 'n', 'r', 't'> {};
struct EscapedChar : peg::sor<EscapedSingleChar, UnicodeXDigit> {};
struct UnescapedChar : peg::utf8::range<0x20, 0x10FFFF> {};
struct Char : peg::if_then_else<peg::one<'\\'>, EscapedChar, UnescapedChar> {};

struct StringContent : peg::until<peg::at<peg::one<'"'>>, Char> {};
struct String : peg::seq<peg::one<'"'>, StringContent, peg::any> {};

struct Identifier : peg::identifier {};

struct WhiteSpace : peg::one<' ', '\t', '\n', '\r'> {};
template <typename T>
struct WSPad : peg::pad<T, WhiteSpace> {};

struct UnsignedInteger : Digits {};
struct Integer : peg::seq<peg::opt<peg::one<'-'>>, Digits> {};

}  // namespace kqir
