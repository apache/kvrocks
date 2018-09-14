#include "t_string.h"
#include <string>

rocksdb::Status RedisString::get(Slice key, std::string *raw_value, std::string *value) {
  if (value) value->clear();
  if (raw_value) {
    raw_value->clear();
    std::string md_bytes;
    Metadata(kRedisString).Encode(&md_bytes);
    raw_value->append(md_bytes);
  }

  std::string raw_bytes;
  rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, key, &raw_bytes);
  if (!s.ok()) return s;

  Metadata metadata(kRedisNone);
  metadata.Decode(raw_bytes);
  if (metadata.Type() != kRedisString) {
    return rocksdb::Status::InvalidArgument("WRONGTYPE Operation against a key holding the wrong kind of value");
  }
  if (metadata.Expired()) {
    return rocksdb::Status::NotFound("the key was expired");
  }
  if (value) value->assign(raw_bytes.substr(5, raw_bytes.size()-1));
  if (raw_value) raw_value->assign(raw_bytes.data(), raw_bytes.size());
  return rocksdb::Status::OK();
}

rocksdb::Status RedisString::update(Slice key, Slice raw_value, Slice new_value) {
  std::string metadata_bytes;
  if (raw_value.empty()) {
    Metadata(kRedisString).Encode(&metadata_bytes);
  } else {
    metadata_bytes = raw_value.ToString().substr(0, 5);
  }
  metadata_bytes.append(new_value.ToString());
  return db_->Put(rocksdb::WriteOptions(), metadata_cf_handle_, key, metadata_bytes);
}

rocksdb::Status RedisString::Append(Slice key, Slice value, int *ret) {
  std::string raw_value_bytes, value_bytes;
  rocksdb::Status s = get(key, &raw_value_bytes, &value_bytes);
  if (!s.ok() && !s.IsNotFound()) return s;

  value_bytes.append(value.ToString());
  *ret = int(value_bytes.size());
  return update(key, raw_value_bytes, value_bytes);
}

#include <iostream>
std::vector<rocksdb::Status> RedisString::MGet(std::vector<Slice> keys, std::vector<std::string> *values) {
  std::vector<rocksdb::ColumnFamilyHandle*> cf_handles;
  cf_handles.resize(keys.size(), metadata_cf_handle_);
  std::vector<rocksdb::Status> statuses = db_->MultiGet(rocksdb::ReadOptions(), cf_handles, keys, values);
  for(int i = 0; i < statuses.size(); i++) {
    // remove metadata prefix
    if (statuses[i].ok()) {
      (*values)[i] = (*values)[i].substr(5, (*values)[i].size() - 5);
    }
  }
  return statuses;
}

rocksdb::Status RedisString::Get(Slice key, std::string *value) {
  std::vector<Slice> keys{key};
  std::vector<std::string> values;
  std::vector<rocksdb::Status> statuses = MGet(keys, &values);
  *value = values[0];
  return statuses[0];
}
rocksdb::Status RedisString::GetSet(Slice key, Slice new_value, std::string *old_value) {
  std::string raw_value_bytes, value_bytes;
  rocksdb::Status s = get(key, &raw_value_bytes, &value_bytes);
  if (!s.ok() && !s.IsNotFound()) return s;
  return update(key, raw_value_bytes, new_value);
}

rocksdb::Status RedisString::Set(Slice key, Slice value) {
  std::vector<StringPair> pairs{StringPair{key, value}};
  MSet(pairs, 0);
  return rocksdb::Status::OK();
}

rocksdb::Status RedisString::SetEX(Slice key, Slice value, int ttl) {
  std::vector<StringPair> pairs{StringPair{key, value}};
  MSet(pairs, ttl);
  return rocksdb::Status::OK();
}

rocksdb::Status RedisString::SetNX(Slice key, Slice value, int *ret) {
  std::vector<StringPair> pairs{StringPair{key, value}};
  return MSetNX(pairs, ret);
}

rocksdb::Status RedisString::SetRange(Slice key, Slice value, int offset, int *ret) {
  std::string raw_value_bytes, value_bytes;
  rocksdb::Status s = get(key, &raw_value_bytes, &value_bytes);
  if (!s.ok() && !s.IsNotFound()) return s;
  if (offset > value_bytes.size()) {
    // padding the value with zero byte while offset is longer than value size
    int paddings = offset-int(value_bytes.size());
    value_bytes.append(paddings, '\0');
  }
  if (offset+value.size() >= value_bytes.size()) {
    value_bytes = value_bytes.substr(0, offset);
    value_bytes.append(value.ToString());
  } else {
    for(int i = 0; i < value.size(); i++) {
      value_bytes[i] = value[i];
    }
  }
  *ret = int(value_bytes.size());
  return update(key, raw_value_bytes, value_bytes);
}

rocksdb::Status RedisString::IncrBy(Slice key, int64_t increment, int64_t *ret) {
  std::string raw_value_bytes, value_bytes;
  rocksdb::Status s = get(key, &raw_value_bytes, &value_bytes);
  if (!s.ok() && !s.IsNotFound()) return s;

  int64_t value = 0;
  if (!value_bytes.empty()) {
    try {
      value = std::stoll(value_bytes);
    } catch(std::exception &e) {
      return rocksdb::Status::InvalidArgument("string old_value is not an integer");
    }
  }
  if ((increment < 0 && value < 0 && increment < (LLONG_MIN-value))
      || (increment > 0 && value > 0 && increment > (LLONG_MAX-value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }
  value += increment;
  *ret = value;
  return update(key, raw_value_bytes, std::to_string(value));
}

rocksdb::Status RedisString::MSet(std::vector<StringPair> pairs, int ttl) {
  uint32_t expire = 0;
  if (ttl > 0) {
    int64_t now;
    rocksdb::Env::Default()->GetCurrentTime(&now);
    expire = uint32_t(now) + ttl;
  }
  rocksdb::WriteBatch batch;
  for (StringPair pair : pairs) {
    std::string bytes;
    Metadata metadata(kRedisString);
    metadata.expire = expire;
    metadata.Encode(&bytes);
    bytes.append(pair.value.ToString());
    batch.Put(metadata_cf_handle_, pair.key, bytes);
  }
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisString::MSetNX(std::vector<StringPair> pairs, int *ret) {
  *ret = 0;
  std::string value;
  rocksdb::WriteBatch batch;
  for (StringPair pair : pairs) {
    rocksdb::Status s = db_->Get(rocksdb::ReadOptions(), metadata_cf_handle_, pair.key, &value);
    if (!s.IsNotFound()) return rocksdb::Status::OK();
    std::string bytes;
    Metadata(kRedisString).Encode(&bytes);
    bytes.append(pair.value.ToString());
    batch.Put(metadata_cf_handle_, pair.key, bytes);
  }
  *ret = 1;
  return db_->Write(rocksdb::WriteOptions(), &batch);
}