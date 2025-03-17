oss_fuzz_compile_all()
{
    # Make sure we are in the root directory of the jsoncons
    DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" >/dev/null 2>&1 && pwd )"
    cd ${DIR}/..

    $CXX ./fuzzers/fuzz_parse.cpp -I./include $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_parse
    $CXX ./fuzzers/fuzz_csv.cpp -I./include $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_csv

    # With third party
    $CXX ./fuzzers/fuzz_cbor.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_cbor
    $CXX ./fuzzers/fuzz_bson.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_bson
    $CXX ./fuzzers/fuzz_msgpack.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_msgpack
    $CXX ./fuzzers/fuzz_ubjson.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_ubjson

    # Fuzzers with encoders
    $CXX ./fuzzers/fuzz_bson_encoder.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_bson_encoder
    $CXX ./fuzzers/fuzz_cbor_encoder.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_cbor_encoder
    $CXX ./fuzzers/fuzz_csv_encoder.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_csv_encoder
    $CXX ./fuzzers/fuzz_json_encoder.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_json_encoder
    $CXX ./fuzzers/fuzz_msgpack_encoder.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_msgpack_encoder
    $CXX ./fuzzers/fuzz_ubjson_encoder.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_ubjson_encoder

    # Fuzzers with max parser depth
    $CXX ./fuzzers/fuzz_bson_parser_max.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_bson_parser_max
    $CXX ./fuzzers/fuzz_cbor_parser_max.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_cbor_parser_max
    $CXX ./fuzzers/fuzz_json_parser_max.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_json_parser_max
    $CXX ./fuzzers/fuzz_msgpack_parser_max.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_msgpack_parser_max
    $CXX ./fuzzers/fuzz_ubjson_parser_max.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_ubjson_parser_max

    # Fuzzers that target the cursors
    $CXX ./fuzzers/fuzz_json_cursor.cpp -I./include -I./third_party $CXXFLAGS $LIB_FUZZING_ENGINE -o $OUT/fuzz_json_cursor
}

if [[ -z "${OUT}" ]]; then
  echo "This script assumes we run inside an oss-fuzz environment with the proper environment variables set"
  echo "Please set these environment variables for it to run properly"
else
  echo "Compiling the fuzzers"
  oss_fuzz_compile_all
fi
