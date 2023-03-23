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

#include <limits>
#include <map>
#include <string>
#include <vector>

#include "geohash.h"
#include "storage/redis_db.h"
#include "storage/redis_metadata.h"
#include "types/redis_zset.h"

enum DistanceUnit {
  kDistanceMeter,
  kDistanceKilometers,
  kDistanceMiles,
  kDistanceFeet,
};

enum DistanceSort {
  kSortNone,
  kSortASC,
  kSortDESC,
};

// Structures represent points and array of points on the earth.
struct GeoPoint {
  double longitude;
  double latitude;
  std::string member;
  double dist;
  double score;
};

namespace Redis {

class Geo : public ZSet {
 public:
  explicit Geo(Engine::Storage *storage, const std::string &ns) : ZSet(storage, ns) {}
  rocksdb::Status Add(const Slice &user_key, std::vector<GeoPoint> *geo_points, int *ret);
  rocksdb::Status Dist(const Slice &user_key, const Slice &member_1, const Slice &member_2, double *dist);
  rocksdb::Status Hash(const Slice &user_key, const std::vector<Slice> &members, std::vector<std::string> *geoHashes);
  rocksdb::Status Pos(const Slice &user_key, const std::vector<Slice> &members,
                      std::map<std::string, GeoPoint> *geo_points);
  rocksdb::Status Radius(const Slice &user_key, double longitude, double latitude, double radius_meters, int count,
                         DistanceSort sort, const std::string &store_key, bool store_distance, double unit_conversion,
                         std::vector<GeoPoint> *geo_points);
  rocksdb::Status RadiusByMember(const Slice &user_key, const Slice &member, double radius_meters, int count,
                                 DistanceSort sort, const std::string &store_key, bool store_distance,
                                 double unit_conversion, std::vector<GeoPoint> *geo_points);

  rocksdb::Status Get(const Slice &user_key, const Slice &member, GeoPoint *geo_point);
  rocksdb::Status MGet(const Slice &user_key, const std::vector<Slice> &members,
                       std::map<std::string, GeoPoint> *geo_points);
  static std::string EncodeGeoHash(double longitude, double latitude);

 private:
  static int decodeGeoHash(double bits, double *xy);
  int membersOfAllNeighbors(const Slice &user_key, GeoHashRadius n, double lon, double lat, double radius,
                            std::vector<GeoPoint> *geo_points);
  int membersOfGeoHashBox(const Slice &user_key, GeoHashBits hash, std::vector<GeoPoint> *geo_points, double lon,
                          double lat, double radius);
  static void scoresOfGeoHashBox(GeoHashBits hash, GeoHashFix52Bits *min, GeoHashFix52Bits *max);
  int getPointsInRange(const Slice &user_key, double min, double max, double lon, double lat, double radius,
                       std::vector<GeoPoint> *geo_points);
  static bool appendIfWithinRadius(std::vector<GeoPoint> *geo_points, double lon, double lat, double radius,
                                   double score, const std::string &member);

  static bool sortGeoPointASC(const GeoPoint &gp1, const GeoPoint &gp2);
  static bool sortGeoPointDESC(const GeoPoint &gp1, const GeoPoint &gp2);
};

}  // namespace Redis
