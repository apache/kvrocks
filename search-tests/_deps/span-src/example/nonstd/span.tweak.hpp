// Example span-lite tweak file example/nonstd/span.tweak.hpp
// See also https://github.com/martinmoene/span-lite#configuration

//
// Select std::span or nonstd::span:
//

// #define span_CONFIG_SELECT_SPAN span_SPAN_DEFAULT
// #define span_CONFIG_SELECT_SPAN span_SPAN_STD
// #define span_CONFIG_SELECT_SPAN span_SPAN_NONSTD

//
// Define contract handling:
//

// #define span_CONFIG_CONTRACT_LEVEL_ON 1
// #define span_CONFIG_CONTRACT_LEVEL_OFF 1
// #define span_CONFIG_CONTRACT_LEVEL_EXPECTS_ONLY 1
// #define span_CONFIG_CONTRACT_LEVEL_ENSURES_ONLY 1
// #define span_CONFIG_CONTRACT_VIOLATION_TERMINATES 1
// #define span_CONFIG_CONTRACT_VIOLATION_THROWS 1

//
// Select available features:
//

// #define span_FEATURE_CONSTRUCTION_FROM_STDARRAY_ELEMENT_TYPE 1
// #define span_FEATURE_WITH_INITIALIZER_LIST_P2447 1
// #define span_FEATURE_WITH_CONTAINER_TO_STD 99
// #define span_FEATURE_MEMBER_CALL_OPERATOR 1
// #define span_FEATURE_MEMBER_AT 2
// #define span_FEATURE_MEMBER_BACK_FRONT 1
// #define span_FEATURE_MEMBER_SWAP 1
// #define span_FEATURE_COMPARISON 1
// #define span_FEATURE_SAME 1
// #define span_FEATURE_NON_MEMBER_FIRST_LAST_SUB 1
// #define span_FEATURE_NON_MEMBER_FIRST_LAST_SUB_SPAN 1
// #define span_FEATURE_NON_MEMBER_FIRST_LAST_SUB_CONTAINER 1
// #define span_FEATURE_MAKE_SPAN_TO_STD 99
// #define span_FEATURE_BYTE_SPAN 1

// Alternative flags:
#define span_FEATURE_WITH_CONTAINER 1   // takes precedence over span_FEATURE_WITH_CONTAINER_TO_STD
#define span_FEATURE_MAKE_SPAN 1        // takes precedence over span_FEATURE_MAKE_SPAN_TO_STD
