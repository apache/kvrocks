#include "t_zset.h"

rocksdb::Status RedisZSet::GetMetadata(Slice key, ZSetMetadata *metadata) {
  return RedisDB::GetMetadata(kRedisZSet, key, metadata);
}

rocksdb::Status RedisZSet::Add(Slice key, uint8_t flags, std::vector<MemberScore> &mscores, int *ret) {
  *ret = 0;
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok() && !s.IsNotFound()) return s;

  RWLocksGuard guard(storage->GetLocks(), key);
  int added = 0;
  rocksdb::WriteBatch batch;
  std::string member_key;
  for (int i = 0; i < mscores.size(); i++) {
    InternalKey(key, mscores[i].member, metadata.version).Encode(&member_key);
    if (metadata.size > 0) {
      std::string old_score_bytes;
      s = db_->Get(rocksdb::ReadOptions(), member_key, &old_score_bytes);
      if (!s.ok() && !s.IsNotFound()) return s;
      if (s.ok()) {
        double old_score = DecodeDouble(old_score_bytes.data());
        if (flags == ZSET_INCR) {
          mscores[i].score += old_score;
        }
        if (mscores[i].score != old_score) {
          old_score_bytes.append(mscores[i].member);
          std::string old_score_key;
          InternalKey(key, old_score_bytes, metadata.version).Encode(&old_score_key);
          batch.Delete(old_score_key);
          std::string new_score_bytes, new_score_key;
          PutDouble(&new_score_bytes, mscores[i].score);
          batch.Put(member_key, new_score_bytes);
          new_score_bytes.append(mscores[i].member);
          InternalKey(key, new_score_bytes, metadata.version).Encode(&new_score_key);
          batch.Put(score_cf_handle_, new_score_key, Slice());
        }
        continue;
      }
    }
    std::string score_bytes, score_key;
    PutDouble(&score_bytes, mscores[i].score);
    batch.Put(member_key, score_bytes);
    score_bytes.append(mscores[i].member);
    InternalKey(key, score_bytes, metadata.version).Encode(&score_key);
    batch.Put(score_cf_handle_, score_key, Slice());
    added++;
  }
  if (added > 0) {
    *ret = added;
    metadata.size += added;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisZSet::Card(Slice key, int *ret) {
  *ret = 0;
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;
  *ret = metadata.size;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisZSet::Count(Slice key, ZRangeSpec spec, int *ret) {
  *ret = 0;
  return RangeByScore(key, spec, nullptr, ret);
}

rocksdb::Status RedisZSet::IncrBy(Slice key, Slice member, double increment, double *score) {
  int ret;
  std::vector<MemberScore> mscores;
  mscores.emplace_back(MemberScore{member.ToString(), increment});
  rocksdb::Status s = Add(key, ZSET_INCR, mscores, &ret);
  if (!s.ok()) return s;
  *score = mscores[0].score;
  return rocksdb::Status::OK();
}

rocksdb::Status RedisZSet::Pop(Slice key, int count, bool min, std::vector<MemberScore> *mscores) {
  mscores->clear();
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound()? rocksdb::Status::OK():s;
  if (count <=0) return rocksdb::Status::OK();
  if (count > metadata.size) count = metadata.size;

  std::string score_bytes;
  double score = min ? std::numeric_limits<double>::lowest():std::numeric_limits<double>::max();
  PutDouble(&score_bytes, score);
  std::string start_key, prefix_key;
  InternalKey(key, score_bytes, metadata.version).Encode(&start_key);
  InternalKey(key, "", metadata.version).Encode(&prefix_key);

  RWLocksGuard guard(storage->GetLocks(), key);
  rocksdb::WriteBatch batch;
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, score_cf_handle_);
  for (min ? iter->Seek(start_key) : iter->SeekForPrev(start_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       min ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key());
    Slice score_key = ikey.GetSubKey();
    GetDouble(&score_key, &score);
    mscores->emplace_back(MemberScore{score_key.ToString(), score});
    batch.Delete(score_key);
    batch.Delete(score_cf_handle_, iter->key());
    if (mscores->size() >= count) break;
  }
  if (mscores->size() > 0) {
    metadata.size -= mscores->size();
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisZSet::Range(Slice key, int start, int stop, uint8_t flags, std::vector<MemberScore> *mscores) {
  mscores->clear();
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound()? rocksdb::Status::OK():s;

  if (start < 0) start += metadata.size;
  if (stop < 0) stop += metadata.size;
  if (start < 0 || stop < 0 || start > stop) {
    return rocksdb::Status::OK();
  }
  bool removed = (flags & (uint8_t)ZSET_REMOVED) != 0;
  bool reversed = (flags & (uint8_t)ZSET_REVERSED) != 0;

  std::string score_bytes;
  double score = !reversed ? std::numeric_limits<double>::lowest():std::numeric_limits<double>::max();
  PutDouble(&score_bytes, score);
  std::string start_key, prefix_key;
  InternalKey(key, score_bytes, metadata.version).Encode(&start_key);
  InternalKey(key, "", metadata.version).Encode(&prefix_key);

  if (removed) RWLocksGuard guard(storage->GetLocks(), key);
  int count = 0;
  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, score_cf_handle_);
  rocksdb::WriteBatch batch;
  for (!reversed? iter->Seek(start_key) : iter->SeekForPrev(start_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       !reversed? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key());
    Slice score_key = ikey.GetSubKey();
    GetDouble(&score_key, &score);
    if (count >= start) {
      if (removed) {
        std::string sub_key;
        InternalKey(key, score_key, metadata.version).Encode(&sub_key);
        batch.Delete(sub_key);
        batch.Delete(score_cf_handle_, iter->key());
      }
      mscores->emplace_back(MemberScore{score_key.ToString(), score});
    }
    if (count++ >= stop) break;
  }
  if (removed &&count > 0) {
    metadata.size -= count;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
    return db_->Write(rocksdb::WriteOptions(), &batch);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisZSet::RangeByScore(Slice key, ZRangeSpec spec, std::vector<MemberScore> *mscores, int *size) {
  if (size) *size = 0;
  if(mscores) mscores->clear();
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s.IsNotFound()? rocksdb::Status::OK():s;

  std::string start_score_bytes;
  PutDouble(&start_score_bytes, spec.min);
  std::string start_key, prefix_key;
  InternalKey(key, start_score_bytes, metadata.version).Encode(&start_key);
  InternalKey(key, "", metadata.version).Encode(&prefix_key);

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();
  read_options.fill_cache = false;

  int pos = 0;
  auto iter = db_->NewIterator(read_options, score_cf_handle_);
  if (spec.removed) RWLocksGuard guard(storage->GetLocks(), key);
  rocksdb::WriteBatch batch;
  for (iter->Seek(start_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       iter->Next()) {
    InternalKey ikey(iter->key());
    Slice score_key = ikey.GetSubKey();
    double score;
    GetDouble(&score_key, &score);
    if (spec.offset >= 0 && pos++ < spec.offset) continue;
    if (spec.minex && score <= spec.min) continue; // the min score was exclusive
    if ((spec.maxex && score >= spec.max) || score > spec.max) break;
    if (spec.removed) {
      std::string sub_key;
      InternalKey(key, score_key, metadata.version).Encode(&sub_key);
      batch.Delete(sub_key);
      batch.Delete(score_cf_handle_, iter->key());
    } else {
      if (mscores) mscores->emplace_back(MemberScore{score_key.ToString(), score});
    }
    if (size) *size += 1;
    if (spec.count > 0 && mscores && mscores->size() >= spec.count) break;
  }
  if (spec.removed && *size > 0) {
    metadata.size -= *size;
    std::string bytes;
    metadata.Encode(&bytes);
    batch.Put(metadata_cf_handle_, key, bytes);
    return db_->Write(rocksdb::WriteOptions(), &batch);
  }
  return rocksdb::Status::OK();
}

rocksdb::Status RedisZSet::Score(Slice key, Slice member, double *score) {
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();

  std::string member_key, score_bytes;
  InternalKey(key, member, metadata.version).Encode(&member_key);
  s = db_->Get(read_options, member_key, &score_bytes);
  if (!s.ok()) return s;
  *score = DecodeDouble(score_bytes.data());
  return rocksdb::Status::OK();
}

rocksdb::Status RedisZSet::Remove(Slice key, std::vector<Slice> members, int *ret) {
  *ret = 0;
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;

  RWLocksGuard guard(storage->GetLocks(), key);
  rocksdb::WriteBatch batch;
  int removed = 0;
  std::string member_key, score_key;
  for (auto member : members) {
    InternalKey(key, member, metadata.version).Encode(&member_key);
    std::string score_bytes;
    s = db_->Get(rocksdb::ReadOptions(), member_key, &score_bytes);
    if (s.ok()) {
      score_bytes.append(member.ToString());
      InternalKey(key, score_bytes, metadata.version).Encode(&score_key);
      batch.Delete(member_key);
      batch.Delete(score_cf_handle_, score_key);
      removed++;
    }
  }
  if (removed > 0) {
    *ret = removed;
    metadata.size -= removed;
    std::string bytes;
    batch.Put(metadata_cf_handle_, key, bytes);
  }
  return db_->Write(rocksdb::WriteOptions(), &batch);
}

rocksdb::Status RedisZSet::RemoveRangeByScore(Slice key, ZRangeSpec spec, int *ret) {
  spec.removed = true;
  return RangeByScore(key, spec, nullptr, ret);
}

rocksdb::Status RedisZSet::RemoveRangeByRank(Slice key, int start, int stop, bool reversed, int *ret) {
  uint8_t flags = ZSET_REMOVED;
  if (reversed) flags |= ZSET_REVERSED;
  std::vector<MemberScore> mscores;
  rocksdb::Status s = Range(key, start, stop, flags, &mscores);
  *ret = int(mscores.size());
  return s;
}

rocksdb::Status RedisZSet::Rank(Slice key, Slice member, bool reversed, int *ret) {
  *ret = 0;
  ZSetMetadata metadata;
  rocksdb::Status s = GetMetadata(key, &metadata);
  if (!s.ok()) return s;

  rocksdb::ReadOptions read_options;
  LatestSnapShot ss(db_);
  read_options.snapshot = ss.GetSnapShot();

  std::string score_bytes, member_key;
  InternalKey(key, member, metadata.version).Encode(&member_key);
  s = db_->Get(rocksdb::ReadOptions(), member_key, &score_bytes);
  if (!s.ok()) return s;
  double target_score = DecodeDouble(score_bytes.data());
  std::string start_score_bytes, start_key, prefix_key;
  double start_score = !reversed ? std::numeric_limits<double>::lowest():std::numeric_limits<double>::max();
  PutDouble(&start_score_bytes, start_score);
  InternalKey(key, start_score_bytes, metadata.version).Encode(&start_key);
  InternalKey(key, "", metadata.version).Encode(&prefix_key);

  int rank = 0;
  read_options.fill_cache = false;
  auto iter = db_->NewIterator(read_options, score_cf_handle_);
  for (!reversed ? iter->Seek(start_key) : iter->SeekForPrev(start_key);
       iter->Valid() && iter->key().starts_with(prefix_key);
       !reversed ? iter->Next() : iter->Prev()) {
    InternalKey ikey(iter->key());
    Slice score_key = ikey.GetSubKey();
    double score;
    GetDouble(&score_key, &score);
    if (score == target_score && score_key == member) break;
    rank++;
  }
  *ret = rank;
  return rocksdb::Status::OK();
}