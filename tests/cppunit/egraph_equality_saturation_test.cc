#include <gtest/gtest.h>
#include "search/ir.h"
#include "search/ir_pass.h"
#include "search/passes/egraph_equality_saturation.h"
#include "search/passes/manager.h"

namespace kqir {

class EGraphEqualitySaturationTest : public ::testing::Test {
 protected:
  void SetUp() override {
    // Set up any necessary test data or environment here
  }

  void TearDown() override {
    // Clean up any test data or environment here
  }
};

TEST_F(EGraphEqualitySaturationTest, BasicEqualitySaturation) {
  // Create a simple query expression
  auto field = std::make_unique<FieldRef>("field");
  auto literal = std::make_unique<NumericLiteral>(42);
  auto expr = std::make_unique<NumericCompareExpr>(NumericCompareExpr::EQ, std::move(field), std::move(literal));

  // Create a search expression
  auto index = std::make_unique<IndexRef>("index");
  auto select = std::make_unique<SelectClause>(std::vector<std::unique_ptr<FieldRef>>{});
  auto search_expr = std::make_unique<SearchExpr>(std::move(index), std::move(expr), nullptr, nullptr, std::move(select));

  // Apply the EGraphEqualitySaturation pass
  EGraphEqualitySaturation pass;
  auto result = pass.Transform(std::move(search_expr));

  // Verify the result
  ASSERT_NE(result, nullptr);
  ASSERT_TRUE(dynamic_cast<SearchExpr*>(result.get()) != nullptr);
}

TEST_F(EGraphEqualitySaturationTest, ComplexEqualitySaturation) {
  // Create a complex query expression
  auto field1 = std::make_unique<FieldRef>("field1");
  auto literal1 = std::make_unique<NumericLiteral>(42);
  auto expr1 = std::make_unique<NumericCompareExpr>(NumericCompareExpr::EQ, std::move(field1), std::move(literal1));

  auto field2 = std::make_unique<FieldRef>("field2");
  auto literal2 = std::make_unique<NumericLiteral>(100);
  auto expr2 = std::make_unique<NumericCompareExpr>(NumericCompareExpr::GT, std::move(field2), std::move(literal2));

  std::vector<std::unique_ptr<QueryExpr>> and_exprs;
  and_exprs.push_back(std::move(expr1));
  and_exprs.push_back(std::move(expr2));
  auto and_expr = std::make_unique<AndExpr>(std::move(and_exprs));

  // Create a search expression
  auto index = std::make_unique<IndexRef>("index");
  auto select = std::make_unique<SelectClause>(std::vector<std::unique_ptr<FieldRef>>{});
  auto search_expr = std::make_unique<SearchExpr>(std::move(index), std::move(and_expr), nullptr, nullptr, std::move(select));

  // Apply the EGraphEqualitySaturation pass
  EGraphEqualitySaturation pass;
  auto result = pass.Transform(std::move(search_expr));

  // Verify the result
  ASSERT_NE(result, nullptr);
  ASSERT_TRUE(dynamic_cast<SearchExpr*>(result.get()) != nullptr);
}

}  // namespace kqir
