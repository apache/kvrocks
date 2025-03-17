set(CMAKE_FOLDER "${CMAKE_FOLDER}/utility")

if (RANGE_V3_COROUTINE_FLAGS)
  rv3_add_test(test.generator generator generator.cpp)
endif()
