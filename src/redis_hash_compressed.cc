#include "redis_hash_compressed.h"
#include <rocksdb/status.h>
#include <limits>
#include <iostream>
#include <algorithm>

namespace Redis {
rocksdb::Status HashCompressed::Get(const HashMetadata &metadata, const Slice &field, std::string *value) {
  std::vector<FieldValue> field_values;
  auto s = GetAllFieldValues(metadata, &field_values);
  if (!s.ok()) return s;

  for (const auto fv : field_values) {
    if (fv.field == field) {
      *value = fv.value;
      return rocksdb::Status::OK();
    }
  }
  return rocksdb::Status::NotFound();
}

rocksdb::Status HashCompressed::IncrBy(HashMetadata *metadata,
                                       const Slice &ns_key, const Slice &field, int64_t increment, int64_t *ret) {
  std::vector<FieldValue> field_values;
  auto s = GetAllFieldValues(*metadata, &field_values);
  if (!s.ok()) return s;

  bool exists = false;
  int64_t old_value = 0;
  auto it = field_values.begin();
  while (it != field_values.end()) {
    if (it->field == field) {
      try {
        old_value = std::stoll(it->value);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      exists = true;
      break;
    } else {
      ++it;
    }
  }

  if ((increment < 0 && old_value < 0 && increment < (LLONG_MIN - old_value))
      || (increment > 0 && old_value > 0 && increment > (LLONG_MAX - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  if (it == field_values.end()) {
    field_values.push_back({field.ToString(), std::to_string(*ret)});
    if (field_values.size() > metadata->small_hash_compress_to_meta_threshold) {
      return Uncompressed(metadata, ns_key, field_values);
    }
  } else {
    it->value = std::to_string(*ret);
  }
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  if (!exists) {
    metadata->size += 1;
  }
  EncodeFieldValues(&field_values, &metadata->field_value_bytes);
  std::string bytes;
  metadata->Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status HashCompressed::IncrByFloat(HashMetadata *metadata,
                                            const Slice &ns_key, const Slice &field, float increment, float *ret) {
  std::vector<FieldValue> field_values;
  auto s = GetAllFieldValues(*metadata, &field_values);
  if (!s.ok()) return s;

  bool exists = false;
  float old_value = 0;
  auto it = field_values.begin();
  while (it != field_values.end()) {
    if (it->field == field) {
      try {
        old_value = std::stof(it->value);
      } catch (std::exception &e) {
        return rocksdb::Status::InvalidArgument(e.what());
      }
      exists = true;
      break;
    } else {
      ++it;
    }
  }

  if ((increment < 0 && old_value < 0 && increment < (std::numeric_limits<float>::lowest() - old_value))
      || (increment > 0 && old_value > 0 && increment > (std::numeric_limits<float>::max() - old_value))) {
    return rocksdb::Status::InvalidArgument("increment or decrement would overflow");
  }

  *ret = old_value + increment;
  if (it == field_values.end()) {
    field_values.push_back({field.ToString(), std::to_string(*ret)});
    if (field_values.size() > metadata->small_hash_compress_to_meta_threshold) {
      return Uncompressed(metadata, ns_key, field_values);
    }
  } else {
    it->value = std::to_string(*ret);
  }
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  if (!exists) {
    metadata->size += 1;
  }
  EncodeFieldValues(&field_values, &metadata->field_value_bytes);
  std::string bytes;
  metadata->Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status HashCompressed::MGet(const HashMetadata &metadata,
                                     const std::vector<Slice> &fields,
                                     std::vector<std::string> *values) {
  values->clear();

  std::vector<FieldValue> field_values;
  auto s = GetAllFieldValues(metadata, &field_values);
  if (!s.ok()) return s;

  std::string value;
  for (const auto field : fields) {
    value.clear();
    for (const auto fv : field_values) {
      if (fv.field == field) {
        value = fv.value;
        break;
      }
    }
    values->emplace_back(value);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HashCompressed::Delete(HashMetadata *metadata,
                                       const Slice &ns_key, const std::vector<Slice> &fields, int *ret) {
  *ret = 0;

  std::vector<FieldValue> field_values;
  auto s = GetAllFieldValues(*metadata, &field_values);
  if (!s.ok()) return s;

  std::string sub_key, value;
  for (const auto &field : fields) {
    auto it = field_values.begin();
    while (it != field_values.end()) {
      if (it->field == field) {
        *ret += 1;
        field_values.erase(it);
        break;
      } else {
        ++it;
      }
    }
  }

  if (*ret == 0) {
    return rocksdb::Status::OK();
  }
  metadata->size -= *ret;
  EncodeFieldValues(&field_values, &metadata->field_value_bytes);
  std::string bytes;
  metadata->Encode(&bytes);
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status HashCompressed::MSet(HashMetadata *metadata,
                                     const Slice &ns_key,
                                     const std::vector<FieldValue> &field_values,
                                     bool nx,
                                     int *ret) {
  *ret = 0;

  std::vector<FieldValue> all_field_values;
  auto s = GetAllFieldValues(*metadata, &all_field_values);
  if (!s.ok()) return s;

  int added = 0;
  for (const auto &fv : field_values) {
    bool exists = false;
    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata->version).Encode(&sub_key);
    if (metadata->size > 0) {
      auto it = all_field_values.begin();
      while (it != all_field_values.end()) {
        if (it->field == fv.field) {
          exists = true;
          if (nx) break;
          it->value = fv.value;
          break;
        } else {
          ++it;
        }
      }
    }
    if (!exists) {
      added++;
      all_field_values.push_back(fv);
    }
  }
  if (added > 0) {
    *ret = added;
    metadata->size += added;
  }
  if (all_field_values.size() > metadata->small_hash_compress_to_meta_threshold) {
    return Uncompressed(metadata, ns_key, all_field_values);
  }
  EncodeFieldValues(&all_field_values, &metadata->field_value_bytes);
  std::string bytes;
  metadata->Encode(&bytes);
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status HashCompressed::GetAll(const HashMetadata &metadata, std::vector<FieldValue> *field_values) {
  return GetAllFieldValues(metadata, field_values);
}

rocksdb::Status HashCompressed::Scan(const HashMetadata &metadata,
                                     const std::string &cursor,
                                     uint64_t limit,
                                     const std::string &field_prefix,
                                     std::vector<std::string> *fields) {
  uint64_t cnt = 0;
  std::vector<FieldValue> field_values;
  auto s = GetAllFieldValues(metadata, &field_values);
  if (!s.ok()) return s;

  for (const auto fv : field_values) {
    if (!cursor.empty() && fv.field <= cursor) {
      // if cursor is not empty, then we need to skip field <= cursor
      // because we already return those keys in the previous scan
      continue;
    }

    if (!field_prefix.empty() && !Util::HasPrefix(fv.field, field_prefix)) {
      break;
    }
    fields->emplace_back(fv.field);
    cnt++;
    if (limit > 0 && cnt >= limit) {
      break;
    }
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HashCompressed::Uncompressed(HashMetadata *metadata,
                                             const Slice &ns_key,
                                             const std::vector<FieldValue> &field_values) {
  metadata->small_hash_compress_to_meta_threshold = 0;
  metadata->field_value_bytes = std::string();
  metadata->size = field_values.size();
  rocksdb::WriteBatch batch;
  WriteBatchLogData log_data(kRedisHash);
  batch.PutLogData(log_data.Encode());
  for (const auto &fv : field_values) {
    std::string sub_key;
    InternalKey(ns_key, fv.field, metadata->version).Encode(&sub_key);
    batch.Put(sub_key, fv.value);
  }
  std::string bytes;
  metadata->Encode(&bytes);
  batch.Put(metadata_cf_handle_, ns_key, bytes);
  return storage_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status HashCompressed::GetAllFieldValues(const HashMetadata &metadata,
                                                  std::vector<FieldValue> *field_values) {
  field_values->clear();

  Slice input(metadata.field_value_bytes);
  uint32_t field_size, value_size;
  std::string field, value;
  auto size = metadata.size;
  while (size--) {
    if (input.size() < 4) return rocksdb::Status::Corruption();
    GetFixed32(&input, &field_size);
    if (!GetFixedSizeString(&input, field_size, &field)) return rocksdb::Status::Corruption();
    if (input.size() < 4) return rocksdb::Status::Corruption();
    GetFixed32(&input, &value_size);
    if (!GetFixedSizeString(&input, value_size, &value)) return rocksdb::Status::Corruption();
    field_values->push_back({field, value});
  }
  return rocksdb::Status::OK();
}

rocksdb::Status HashCompressed::EncodeFieldValues(std::vector<FieldValue> *field_values,
                                                  std::string *field_value_bytes) {
  field_value_bytes->clear();

  std::sort(field_values->begin(), field_values->end(), compareFieldValue);
  for (const auto fv : *field_values) {
    PutFixed32(field_value_bytes, fv.field.size());
    PutString(field_value_bytes, fv.field);
    PutFixed32(field_value_bytes, fv.value.size());
    PutString(field_value_bytes, fv.value);
  }
  return rocksdb::Status::OK();
}

bool HashCompressed::compareFieldValue(FieldValue fv1, FieldValue fv2) {
  return fv1.field < fv2.field;
}

}  // namespace Redis
