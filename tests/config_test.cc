#include "config.h"
#include <vector>
#include <gtest/gtest.h>

TEST(Namespace, Add) {
  Config config;
  EXPECT_TRUE(!config.AddNamespace("ns", "t0").IsOK());
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(int i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(int i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for(int i = 0; i < namespaces.size(); i++) {
    auto s = config.AddNamespace(namespaces[i], tokens[i]);
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "the token has already exists");
  }
  auto s = config.AddNamespace("n1", "t0");
  EXPECT_FALSE(s.IsOK());
  EXPECT_EQ(s.Msg(), "the namespace has already exists");
}

TEST(Namespace, Set) {
  Config config;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  std::vector<std::string> new_tokens = {"nt1", "nt2'", "nt3", "nt4"};
  for(int i = 0; i < namespaces.size(); i++) {
    auto s = config.SetNamespace(namespaces[i], tokens[i]);
    EXPECT_FALSE(s.IsOK());
    EXPECT_EQ(s.Msg(), "the namespace was not found");
  }
  for(int i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(int i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for(int i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.SetNamespace(namespaces[i], new_tokens[i]).IsOK());
  }
  for(int i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, new_tokens[i]);
  }
}

TEST(Namespace, Delete) {
  Config config;
  config.requirepass = "foobared";
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(int i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  for(int i = 0; i < namespaces.size(); i++) {
    std::string token;
    config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  for (const auto &ns : namespaces) {
    config.DelNamespace(ns);
    std::string token;
    config.GetNamespace(ns, &token);
    EXPECT_TRUE(token.empty());
  }
}

TEST(Namespace, RewriteNamespaces) {
  const char *path = "test.conf";
  unlink(path);
  Config config;
  config.requirepass = "test";
  config.backup_dir = "test";
  config.Load(path) ;
  std::vector<std::string> namespaces= {"n1", "n2", "n3", "n4"};
  std::vector<std::string> tokens = {"t1", "t2", "t3", "t4"};
  for(int i = 0; i < namespaces.size(); i++) {
    EXPECT_TRUE(config.AddNamespace(namespaces[i], tokens[i]).IsOK());
  }
  EXPECT_TRUE(config.Rewrite().IsOK());
  Config new_config;
  auto s = new_config.Load(path) ;
  std::cout << s.Msg() << std::endl;
  for(int i = 0; i < namespaces.size(); i++) {
    std::string token;
    new_config.GetNamespace(namespaces[i], &token);
    EXPECT_EQ(token, tokens[i]);
  }
  unlink(path);
}
