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

#include <gtest/gtest.h>

#include <memory>
#include <status.h>

TEST(StatusOr, Scalar) {
  auto f = [](int x) -> StatusOr<int> {
    if (x > 10) {
        return {Status::NotOK, "x large than 10"};
    }

    return 2 * x + 5;
  };

  ASSERT_EQ(*f(1), 7);
  ASSERT_EQ(*f(5), 15);
  ASSERT_EQ(f(7).GetValue(), 19);
  ASSERT_EQ(f(7).GetCode(), Status::cOK);
  ASSERT_EQ(f(7).Msg(), "ok");
  ASSERT_TRUE(f(6));
  ASSERT_EQ(f(11).GetCode(), Status::NotOK);
  ASSERT_EQ(f(11).Msg(), "x large than 10");
  ASSERT_FALSE(f(12));

  auto x = f(5);
  ASSERT_EQ(*x, 15);
  ASSERT_EQ(x.Msg(), "ok");
  ASSERT_EQ(x.GetValue(), 15);
  ASSERT_EQ(x.GetCode(), Status::cOK);

  auto y = f(11);
  ASSERT_EQ(y.Msg(), "x large than 10");
  ASSERT_EQ(y.GetCode(), Status::NotOK);

  auto g = [f](int y) -> StatusOr<int> {
    if (y > 5 && y < 15) {
        return {Status::NotOK, "y large than 5"};
    }

    auto res = f(y);
    if (!res) return res;

    return *res * 10;
  };

  ASSERT_EQ(*g(1), 70);
  ASSERT_EQ(*g(5), 150);
  ASSERT_EQ(g(1).GetValue(), 70);
  ASSERT_EQ(g(1).GetCode(), Status::cOK);
  ASSERT_EQ(g(1).Msg(), "ok");
  ASSERT_EQ(g(6).GetCode(), Status::NotOK);
  ASSERT_EQ(g(6).Msg(), "y large than 5");
  ASSERT_EQ(g(20).GetCode(), Status::NotOK);
  ASSERT_EQ(g(20).Msg(), "x large than 10");
  ASSERT_EQ(g(11).GetCode(), Status::NotOK);
  ASSERT_EQ(g(11).Msg(), "y large than 5");
}

TEST(StatusOr, String) {
  auto f = [](std::string x) -> StatusOr<std::string> {
    if (x.size() > 10) {
        return {Status::NotOK, "string too long"};
    }

    return x + " hello";
  };

  auto g = [f](std::string x) -> StatusOr<std::string> {
    if (x.size() < 5) {
        return {Status::NotOK, "string too short"};
    }

    auto res = f(x);
    if (!res) return res;

    return "hi " + *res;
  };

  ASSERT_TRUE(f("1"));
  ASSERT_FALSE(f("12345678901"));
  ASSERT_FALSE(g("1"));

  ASSERT_EQ(*f("twice"), "twice hello");
  ASSERT_EQ(*g("twice"), "hi twice hello");
  ASSERT_EQ(g("shrt").GetCode(), Status::NotOK);
  ASSERT_EQ(g("shrt").Msg(), "string too short");
  ASSERT_EQ(g("loooooooooooog").GetCode(), Status::NotOK);
  ASSERT_EQ(g("loooooooooooog").Msg(), "string too long");

  ASSERT_EQ(g("twice").ToStatus().GetCode(), Status::cOK);
  ASSERT_EQ(g("").ToStatus().GetCode(), Status::NotOK);

  auto x = g("twice");
  ASSERT_EQ(x.ToStatus().GetCode(), Status::cOK);
  auto y = g("");
  ASSERT_EQ(y.ToStatus().GetCode(), Status::NotOK);
}

TEST(StatusOr, SharedPtr) {
    struct A {
      A(int *x) : x(x) { *x = 233; }
      ~A() { *x = 0; }

      int *x;
    };

    int val = 0;
    
    {
      StatusOr<std::shared_ptr<A>> x(new A(&val));

      ASSERT_EQ(val, 233);
      ASSERT_EQ(x->use_count(), 1);

      {
        StatusOr<std::shared_ptr<A>> y(*x);
        ASSERT_EQ(val, 233);
        ASSERT_EQ(x->use_count(), 2);
      }

      ASSERT_EQ(x->use_count(), 1);
    }

    ASSERT_EQ(val, 0);

}
