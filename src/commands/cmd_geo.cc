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

#include "commander.h"
#include "error_constants.h"
#include "server/server.h"
#include "types/redis_geo.h"

namespace Redis {

class CommandGeoBase : public Commander {
 public:
  Status ParseDistanceUnit(const std::string &param) {
    if (Util::ToLower(param) == "m") {
      distance_unit_ = kDistanceMeter;
    } else if (Util::ToLower(param) == "km") {
      distance_unit_ = kDistanceKilometers;
    } else if (Util::ToLower(param) == "ft") {
      distance_unit_ = kDistanceFeet;
    } else if (Util::ToLower(param) == "mi") {
      distance_unit_ = kDistanceMiles;
    } else {
      return {Status::RedisParseErr, "unsupported unit provided. please use M, KM, FT, MI"};
    }
    return Status::OK();
  }

  static Status ParseLongLat(const std::string &longitude_para, const std::string &latitude_para, double *longitude,
                             double *latitude) {
    auto long_stat = ParseFloat(longitude_para);
    auto lat_stat = ParseFloat(latitude_para);
    if (!long_stat || !lat_stat) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    *longitude = *long_stat;
    *latitude = *lat_stat;

    if (*longitude < GEO_LONG_MIN || *longitude > GEO_LONG_MAX || *latitude < GEO_LAT_MIN || *latitude > GEO_LAT_MAX) {
      return {Status::RedisParseErr, "invalid longitude,latitude pair " + longitude_para + "," + latitude_para};
    }

    return Status::OK();
  }

  double GetDistanceByUnit(double distance) { return distance / GetUnitConversion(); }

  double GetRadiusMeters(double radius) { return radius * GetUnitConversion(); }

  double GetUnitConversion() {
    double conversion = 0;
    switch (distance_unit_) {
      case kDistanceMeter:
        conversion = 1;
        break;
      case kDistanceKilometers:
        conversion = 1000;
        break;
      case kDistanceFeet:
        conversion = 0.3048;
        break;
      case kDistanceMiles:
        conversion = 1609.34;
        break;
    }
    return conversion;
  }

 protected:
  DistanceUnit distance_unit_ = kDistanceMeter;
};

class CommandGeoAdd : public CommandGeoBase {
 public:
  CommandGeoAdd() : CommandGeoBase() {}
  Status Parse(const std::vector<std::string> &args) override {
    if ((args.size() - 5) % 3 != 0) {
      return {Status::RedisParseErr, errWrongNumOfArguments};
    }

    for (size_t i = 2; i < args.size(); i += 3) {
      double longitude = 0;
      double latitude = 0;
      auto s = ParseLongLat(args[i], args[i + 1], &longitude, &latitude);
      if (!s.IsOK()) return s;

      geo_points_.emplace_back(GeoPoint{longitude, latitude, args[i + 2]});
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    int ret = 0;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Add(args_[1], &geo_points_, &ret);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::Integer(ret);
    return Status::OK();
  }

 private:
  std::vector<GeoPoint> geo_points_;
};

class CommandGeoDist : public CommandGeoBase {
 public:
  CommandGeoDist() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    if (args.size() == 5) {
      auto s = ParseDistanceUnit(args[4]);
      if (!s.IsOK()) return s;
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    double distance = 0;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Dist(args_[1], args_[2], args_[3], &distance);
    if (!s.ok() && !s.IsNotFound()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (s.IsNotFound()) {
      *output = Redis::NilString();
    } else {
      *output = Redis::BulkString(Util::Float2String(GetDistanceByUnit(distance)));
    }
    return Status::OK();
  }
};

class CommandGeoHash : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<std::string> hashes;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Hash(args_[1], members_, &hashes);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = Redis::MultiBulkString(hashes);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoPos : public Commander {
 public:
  Status Parse(const std::vector<std::string> &args) override {
    for (size_t i = 2; i < args.size(); i++) {
      members_.emplace_back(args[i]);
    }
    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::map<std::string, GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Pos(args_[1], members_, &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    std::vector<std::string> list;
    for (const auto &member : members_) {
      auto iter = geo_points.find(member.ToString());
      if (iter == geo_points.end()) {
        list.emplace_back(Redis::NilString());
      } else {
        list.emplace_back(Redis::MultiBulkString(
            {Util::Float2String(iter->second.longitude), Util::Float2String(iter->second.latitude)}));
      }
    }
    *output = Redis::Array(list);
    return Status::OK();
  }

 private:
  std::vector<Slice> members_;
};

class CommandGeoRadius : public CommandGeoBase {
 public:
  CommandGeoRadius() : CommandGeoBase() {}

  Status Parse(const std::vector<std::string> &args) override {
    auto s = ParseLongLat(args[2], args[3], &longitude_, &latitude_);
    if (!s.IsOK()) return s;

    auto radius = ParseFloat(args[4]);
    if (!radius) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    radius_ = *radius;

    s = ParseDistanceUnit(args[5]);
    if (!s.IsOK()) return s;

    s = ParseRadiusExtraOption();
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status ParseRadiusExtraOption(size_t i = 6) {
    while (i < args_.size()) {
      if (Util::ToLower(args_[i]) == "withcoord") {
        with_coord_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "withdist") {
        with_dist_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "withhash") {
        with_hash_ = true;
        i++;
      } else if (Util::ToLower(args_[i]) == "asc") {
        sort_ = kSortASC;
        i++;
      } else if (Util::ToLower(args_[i]) == "desc") {
        sort_ = kSortDESC;
        i++;
      } else if (Util::ToLower(args_[i]) == "count" && i + 1 < args_.size()) {
        auto parse_result = ParseInt<int>(args_[i + 1], 10);
        if (!parse_result) {
          return {Status::RedisParseErr, errValueNotInteger};
        }

        count_ = *parse_result;
        i += 2;
      } else if (attributes_->is_write() &&
                 (Util::ToLower(args_[i]) == "store" || Util::ToLower(args_[i]) == "storedist") &&
                 i + 1 < args_.size()) {
        store_key_ = args_[i + 1];
        if (Util::ToLower(args_[i]) == "storedist") {
          store_distance_ = true;
        }
        i += 2;
      } else {
        return {Status::RedisParseErr, errInvalidSyntax};
      }
    }
    /* Trap options not compatible with STORE and STOREDIST. */
    if (!store_key_.empty() && (with_dist_ || with_hash_ || with_coord_)) {
      return {Status::RedisParseErr,
              "STORE option in GEORADIUS is not compatible with WITHDIST, WITHHASH and WITHCOORDS options"};
    }

    /* COUNT without ordering does not make much sense, force ASC
     * ordering if COUNT was specified but no sorting was requested.
     * */
    if (count_ != 0 && sort_ == kSortNone) {
      sort_ = kSortASC;
    }
    return Status::OK();
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.Radius(args_[1], longitude_, latitude_, GetRadiusMeters(radius_), count_, sort_, store_key_,
                           store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    if (store_key_.size() != 0) {
      *output = Redis::Integer(geo_points.size());
    } else {
      *output = GenerateOutput(geo_points);
    }
    return Status::OK();
  }

  std::string GenerateOutput(const std::vector<GeoPoint> &geo_points) {
    int result_length = static_cast<int>(geo_points.size());
    int returned_items_count = (count_ == 0 || result_length < count_) ? result_length : count_;
    std::vector<std::string> list;
    for (int i = 0; i < returned_items_count; i++) {
      auto geo_point = geo_points[i];
      if (!with_coord_ && !with_hash_ && !with_dist_) {
        list.emplace_back(Redis::BulkString(geo_point.member));
      } else {
        std::vector<std::string> one;
        one.emplace_back(Redis::BulkString(geo_point.member));
        if (with_dist_) {
          one.emplace_back(Redis::BulkString(Util::Float2String(GetDistanceByUnit(geo_point.dist))));
        }
        if (with_hash_) {
          one.emplace_back(Redis::BulkString(Util::Float2String(geo_point.score)));
        }
        if (with_coord_) {
          one.emplace_back(Redis::MultiBulkString(
              {Util::Float2String(geo_point.longitude), Util::Float2String(geo_point.latitude)}));
        }
        list.emplace_back(Redis::Array(one));
      }
    }
    return Redis::Array(list);
  }

 protected:
  double radius_ = 0;
  bool with_coord_ = false;
  bool with_dist_ = false;
  bool with_hash_ = false;
  int count_ = 0;
  DistanceSort sort_ = kSortNone;
  std::string store_key_;
  bool store_distance_ = false;

 private:
  double longitude_ = 0;
  double latitude_ = 0;
};

class CommandGeoRadiusByMember : public CommandGeoRadius {
 public:
  CommandGeoRadiusByMember() = default;

  Status Parse(const std::vector<std::string> &args) override {
    auto radius = ParseFloat(args[3]);
    if (!radius) {
      return {Status::RedisParseErr, errValueIsNotFloat};
    }
    radius_ = *radius;

    auto s = ParseDistanceUnit(args[4]);
    if (!s.IsOK()) return s;

    s = ParseRadiusExtraOption(5);
    if (!s.IsOK()) return s;

    return Commander::Parse(args);
  }

  Status Execute(Server *svr, Connection *conn, std::string *output) override {
    std::vector<GeoPoint> geo_points;
    Redis::Geo geo_db(svr->storage_, conn->GetNamespace());
    auto s = geo_db.RadiusByMember(args_[1], args_[2], GetRadiusMeters(radius_), count_, sort_, store_key_,
                                   store_distance_, GetUnitConversion(), &geo_points);
    if (!s.ok()) {
      return {Status::RedisExecErr, s.ToString()};
    }

    *output = GenerateOutput(geo_points);
    return Status::OK();
  }
};

class CommandGeoRadiusReadonly : public CommandGeoRadius {
 public:
  CommandGeoRadiusReadonly() = default;
};

class CommandGeoRadiusByMemberReadonly : public CommandGeoRadiusByMember {
 public:
  CommandGeoRadiusByMemberReadonly() = default;
};

REDIS_REGISTER_COMMANDS(MakeCmdAttr<CommandGeoAdd>("geoadd", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandGeoDist>("geodist", -4, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoHash>("geohash", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoPos>("geopos", -3, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoRadius>("georadius", -6, "write", 1, 1, 1),
                        MakeCmdAttr<CommandGeoRadiusByMember>("georadiusbymember", -5, "write", 1, 1, 1),
                        MakeCmdAttr<CommandGeoRadiusReadonly>("georadius_ro", -6, "read-only", 1, 1, 1),
                        MakeCmdAttr<CommandGeoRadiusByMemberReadonly>("georadiusbymember_ro", -5, "read-only", 1, 1,
                                                                      1), )

}  // namespace Redis
