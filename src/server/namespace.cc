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

#include "namespace.h"

#include "jsoncons/json.hpp"

// Error messages
constexpr const char* kErrNamespaceExists = "the namespace already exists";
constexpr const char* kErrTokenExists = "the token already exists";
constexpr const char* kErrNamespaceNotFound = "the namespace was not found";
constexpr const char* kErrRequiredPassEmpty = "forbidden to add namespace when requirepass was empty";
constexpr const char* kErrClusterModeEnabled = "forbidden to add namespace when cluster mode was enabled";
constexpr const char* kErrDeleteDefaultNamespace = "forbidden to delete the default namespace";
constexpr const char* kErrAddDefaultNamespace = "forbidden to add the default namespace";
constexpr const char* kErrInvalidToken = "the token is duplicated with requirepass or masterauth";
constexpr const char* kErrCantModifyNamespace =
    "modify namespace requires the server is running with a configuration file or enabled namespace replication";

Status IsNamespaceLegal(const std::string& ns) {
  if (ns.size() > UINT8_MAX) {
    return {Status::NotOK, fmt::format("size exceed limit {}", UINT8_MAX)};
  }
  char last_char = ns.back();
  if (last_char == std::numeric_limits<char>::max()) {
    return {Status::NotOK, "namespace contain illegal letter"};
  }
  return Status::OK();
}

bool Namespace::IsAllowModify() const {
  auto config = storage_->GetConfig();

  return config->HasConfigFile() || config->repl_namespace_enabled;
}

Status Namespace::LoadAndRewrite() {
  auto config = storage_->GetConfig();
  // Namespace is NOT allowed in the cluster mode, so we don't need to rewrite here.
  if (config->cluster_enabled) return Status::OK();

  // Load from the configuration file first
  tokens_ = config->load_tokens;

  // We would like to load namespaces from db even if repl_namespace_enabled is false,
  // this can avoid missing some namespaces when turn on/off repl_namespace_enabled.
  std::string value;
  auto s = storage_->Get(rocksdb::ReadOptions(), cf_, kNamespaceDBKey, &value);
  if (!s.ok() && !s.IsNotFound()) {
    return {Status::NotOK, s.ToString()};
  }
  if (s.ok()) {
    // The namespace db key is existed, so it doesn't allow to switch off repl_namespace_enabled
    if (!config->repl_namespace_enabled) {
      return {Status::NotOK, "cannot switch off repl_namespace_enabled when namespaces exist in db"};
    }

    jsoncons::json j = jsoncons::json::parse(value);
    for (const auto& iter : j.object_range()) {
      if (tokens_.find(iter.key()) == tokens_.end()) {
        // merge the namespace from db
        tokens_[iter.key()] = iter.value().as<std::string>();
      }
    }
  }

  return Rewrite();
}

StatusOr<std::string> Namespace::Get(const std::string& ns) const {
  for (const auto& iter : tokens_) {
    if (iter.second == ns) {
      return iter.first;
    }
  }
  return {Status::NotFound};
}

StatusOr<std::string> Namespace::GetByToken(const std::string& token) const {
  auto iter = tokens_.find(token);
  if (iter == tokens_.end()) {
    return {Status::NotFound};
  }
  return iter->second;
}

Status Namespace::Set(const std::string& ns, const std::string& token) {
  auto s = IsNamespaceLegal(ns);
  if (!s.IsOK()) return s;
  auto config = storage_->GetConfig();
  if (config->requirepass.empty()) {
    return {Status::NotOK, kErrRequiredPassEmpty};
  }
  if (config->cluster_enabled) {
    return {Status::NotOK, kErrClusterModeEnabled};
  }
  if (!IsAllowModify()) {
    return {Status::NotOK, kErrCantModifyNamespace};
  }
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, kErrAddDefaultNamespace};
  }
  if (token == config->requirepass || token == config->masterauth) {
    return {Status::NotOK, kErrInvalidToken};
  }

  for (const auto& iter : tokens_) {
    if (iter.second == ns) {  // need to delete the old token first
      tokens_.erase(iter.first);
      break;
    }
  }
  tokens_[token] = ns;

  s = Rewrite();
  if (!s.IsOK()) {
    tokens_.erase(token);
    return s;
  }
  return Status::OK();
}

Status Namespace::Add(const std::string& ns, const std::string& token) {
  // duplicate namespace
  for (const auto& iter : tokens_) {
    if (iter.second == ns) {
      if (iter.first == token) return Status::OK();
      return {Status::NotOK, kErrNamespaceExists};
    }
  }
  // duplicate token
  if (tokens_.find(token) != tokens_.end()) {
    return {Status::NotOK, kErrTokenExists};
  }
  return Set(ns, token);
}

Status Namespace::Del(const std::string& ns) {
  if (ns == kDefaultNamespace) {
    return {Status::NotOK, kErrDeleteDefaultNamespace};
  }
  if (!IsAllowModify()) {
    return {Status::NotOK, kErrCantModifyNamespace};
  }

  for (const auto& iter : tokens_) {
    if (iter.second == ns) {
      tokens_.erase(iter.first);
      auto s = Rewrite();
      if (!s.IsOK()) {
        tokens_[iter.first] = iter.second;
        return s;
      }
      return Status::OK();
    }
  }
  return {Status::NotOK, kErrNamespaceNotFound};
}

Status Namespace::Rewrite() {
  auto config = storage_->GetConfig();
  // Rewrite the configuration file only if it's running with the configuration file
  if (config->HasConfigFile()) {
    auto s = config->Rewrite(tokens_);
    if (!s.IsOK()) {
      return s;
    }
  }

  // Don't propagate write to DB if its role is slave to prevent from
  // increasing the DB sequence number.
  if (config->IsSlave()) {
    return Status::OK();
  }

  // Don't need to write to db if repl_namespace_enabled is false
  if (!config->repl_namespace_enabled) {
    return Status::OK();
  }
  jsoncons::json json;
  for (const auto& iter : tokens_) {
    json[iter.first] = iter.second;
  }
  return storage_->WriteToPropagateCF(kNamespaceDBKey, json.to_string());
}
