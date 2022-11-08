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
#include <status.h>

#include <memory>

TEST(StatusOr, Scalar) {
  auto f = [](int x) -> StatusOr<int> {
    if (x > 10) {
      return {Status::kNotOK, "x large than 10"};
    }

    return 2 * x + 5;
  };

  ASSERT_EQ(*f(1), 7);
  ASSERT_EQ(*f(5), 15);
  ASSERT_EQ(f(7).GetValue(), 19);
  ASSERT_EQ(f(7).GetCode(), Status::kOK);
  ASSERT_EQ(f(7).Msg(), "ok");
  ASSERT_TRUE(f(6));
  ASSERT_EQ(f(11).GetCode(), Status::kNotOK);
  ASSERT_EQ(f(11).Msg(), "x large than 10");
  ASSERT_FALSE(f(12));

  auto x = f(5);
  ASSERT_EQ(*x, 15);
  ASSERT_EQ(x.Msg(), "ok");
  ASSERT_EQ(x.GetValue(), 15);
  ASSERT_EQ(x.GetCode(), Status::kOK);

  auto y = f(11);
  ASSERT_EQ(y.Msg(), "x large than 10");
  ASSERT_EQ(y.GetCode(), Status::kNotOK);

  auto g = [f](int y) -> StatusOr<int> {
    if (y > 5 && y < 15) {
      return {Status::kNotOK, "y large than 5"};
    }

    auto res = f(y);
    if (!res) return res;

    return *res * 10;
  };

  ASSERT_EQ(*g(1), 70);
  ASSERT_EQ(*g(5), 150);
  ASSERT_EQ(g(1).GetValue(), 70);
  ASSERT_EQ(g(1).GetCode(), Status::kOK);
  ASSERT_EQ(g(1).Msg(), "ok");
  ASSERT_EQ(g(6).GetCode(), Status::kNotOK);
  ASSERT_EQ(g(6).Msg(), "y large than 5");
  ASSERT_EQ(g(20).GetCode(), Status::kNotOK);
  ASSERT_EQ(g(20).Msg(), "x large than 10");
  ASSERT_EQ(g(11).GetCode(), Status::kNotOK);
  ASSERT_EQ(g(11).Msg(), "y large than 5");
}

TEST(StatusOr, String) {
  auto f = [](std::string x) -> StatusOr<std::string> {
    if (x.size() > 10) {
      return {Status::kNotOK, "string too long"};
    }

    return x + " hello";
  };

  auto g = [f](std::string x) -> StatusOr<std::string> {
    if (x.size() < 5) {
      return {Status::kNotOK, "string too short"};
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
  ASSERT_EQ(g("shrt").GetCode(), Status::kNotOK);
  ASSERT_EQ(g("shrt").Msg(), "string too short");
  ASSERT_EQ(g("loooooooooooog").GetCode(), Status::kNotOK);
  ASSERT_EQ(g("loooooooooooog").Msg(), "string too long");

  ASSERT_EQ(g("twice").ToStatus().GetCode(), Status::kOK);
  ASSERT_EQ(g("").ToStatus().GetCode(), Status::kNotOK);

  auto x = g("twice");
  ASSERT_EQ(x.ToStatus().GetCode(), Status::kOK);
  auto y = g("");
  ASSERT_EQ(y.ToStatus().GetCode(), Status::kNotOK);
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

TEST(StatusOr, UniquePtr) {
  StatusOr<std::unique_ptr<int>> x(new int(1));

  ASSERT_EQ(**x, 1);
}

TEST(StatusOr, ValueOr) {
  StatusOr<int> a(1), b(Status::kNotOK, "err");
  ASSERT_EQ(a.ValueOr(0), 1);
  ASSERT_EQ(b.ValueOr(233), 233);
  ASSERT_EQ(StatusOr<int>(1).ValueOr(0), 1);

  StatusOr<std::string> c("hello"), d(Status::kNotOK, "err");
  ASSERT_EQ(c.ValueOr("hi"), "hello");
  ASSERT_EQ(d.ValueOr("hi"), "hi");
  ASSERT_EQ(StatusOr<std::string>("hello").ValueOr("hi"), "hello");
  std::string s = "hi";
  ASSERT_EQ(StatusOr<std::string>(Status::kNotOK, "").ValueOr(s), "hi");
}
