/*
 * Copyright (c) 2013-2014, yinqiwen <yinqiwen@gmail.com>
 * Copyright (c) 2014, Matt Stancliff <matt@genges.com>.
 * Copyright (c) 2015, Salvatore Sanfilippo <antirez@gmail.com>.
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are met:
 *
 *  * Redistributions of source code must retain the above copyright notice,
 *    this list of conditions and the following disclaimer.
 *  * Redistributions in binary form must reproduce the above copyright
 *    notice, this list of conditions and the following disclaimer in the
 *    documentation and/or other materials provided with the distribution.
 *  * Neither the name of Redis nor the names of its contributors may be used
 *    to endorse or promote products derived from this software without
 *    specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
 * AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
 * IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
 * ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT OWNER OR CONTRIBUTORS
 * BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
 * CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
 * SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
 * INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
 * CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
 * ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF
 * THE POSSIBILITY OF SUCH DAMAGE.
 */

/* This is a C to C++ conversion from the redis project.
 * This file started out as:
 * https://github.com/antirez/redis/blob/504ccad/src/geohash_helper.h
 * + https://github.com/antirez/redis/blob/356a630/src/geohash.h
 */

#pragma once

#include <stddef.h>
#include <stdint.h>

constexpr uint8_t GEO_STEP_MAX = 26; /* 26*2 = 52 bits. */

/* Limits from EPSG:900913 / EPSG:3785 / OSGEO:41001 */
constexpr double GEO_LAT_MIN = -85.05112878;
constexpr double GEO_LAT_MAX = 85.05112878;
constexpr double GEO_LONG_MIN = -180;
constexpr double GEO_LONG_MAX = 180;

enum GeoDirection {
  GEOHASH_NORTH = 0,
  GEOHASH_EAST,
  GEOHASH_WEST,
  GEOHASH_SOUTH,
  GEOHASH_SOUTH_WEST,
  GEOHASH_SOUTH_EAST,
  GEOHASH_NORT_WEST,
  GEOHASH_NORT_EAST
};

struct GeoHashBits {
  uint64_t bits = 0;
  uint8_t step = 0;
};

struct GeoHashRange {
  double min = 0;
  double max = 0;
};

struct GeoHashArea {
  GeoHashBits hash;
  GeoHashRange longitude;
  GeoHashRange latitude;
};

struct GeoHashNeighbors {
  GeoHashBits north;
  GeoHashBits east;
  GeoHashBits west;
  GeoHashBits south;
  GeoHashBits north_east;
  GeoHashBits south_east;
  GeoHashBits north_west;
  GeoHashBits south_west;
};

using GeoHashFix52Bits = uint64_t;

struct GeoHashRadius {
  GeoHashBits hash;
  GeoHashArea area;
  GeoHashNeighbors neighbors;
};

inline constexpr bool HASHISZERO(const GeoHashBits &r) { return !r.bits && !r.step; }
inline constexpr bool RANGEISZERO(const GeoHashRange &r) { return !bool(r.max) && !bool(r.min); }
inline constexpr bool RANGEPISZERO(const GeoHashRange *r) { return !r || RANGEISZERO(*r); }

inline constexpr void GZERO(GeoHashBits &s) { s.bits = s.step = 0; }
inline constexpr bool GISZERO(const GeoHashBits &s) { return (!s.bits && !s.step); }
inline constexpr bool GISNOTZERO(const GeoHashBits &s) { return (s.bits || s.step); }

/*
 * 0:success
 * -1:failed
 */
void GeohashGetCoordRange(GeoHashRange *long_range, GeoHashRange *lat_range);
int GeohashEncode(const GeoHashRange *long_range, const GeoHashRange *lat_range, double longitude, double latitude,
                  uint8_t step, GeoHashBits *hash);
int GeohashEncodeType(double longitude, double latitude, uint8_t step, GeoHashBits *hash);
int GeohashEncodeWGS84(double longitude, double latitude, uint8_t step, GeoHashBits *hash);
int GeohashDecode(const GeoHashRange &long_range, const GeoHashRange &lat_range, const GeoHashBits &hash,
                  GeoHashArea *area);
int GeohashDecodeType(const GeoHashBits &hash, GeoHashArea *area);
int GeohashDecodeAreaToLongLat(const GeoHashArea *area, double *xy);
int GeohashDecodeToLongLatType(const GeoHashBits &hash, double *xy);
int GeohashDecodeToLongLatWGS84(const GeoHashBits &hash, double *xy);
void GeohashNeighbors(const GeoHashBits *hash, GeoHashNeighbors *neighbors);

class GeoHashHelper {
 public:
  static uint8_t EstimateStepsByRadius(double range_meters, double lat);
  static int BoundingBox(double longitude, double latitude, double radius_meters, double *bounds);
  static GeoHashRadius GetAreasByRadius(double longitude, double latitude, double radius_meters);
  static GeoHashRadius GetAreasByRadiusWGS84(double longitude, double latitude, double radius_meters);
  static GeoHashFix52Bits Align52Bits(const GeoHashBits &hash);
  static double GetDistance(double lon1d, double lat1d, double lon2d, double lat2d);
  static int GetDistanceIfInRadius(double x1, double y1, double x2, double y2, double radius, double *distance);
  static int GetDistanceIfInRadiusWGS84(double x1, double y1, double x2, double y2, double radius, double *distance);
};
