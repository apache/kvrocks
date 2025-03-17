set(CMAKE_FOLDER "${CMAKE_FOLDER}/iterator")

rv3_add_test(test.iter.basic_iterator iter.basic_iterator basic_iterator.cpp)
rv3_add_test(test.iter.diffmax_t iter.diffmax_t diffmax_t.cpp)
rv3_add_test(test.iter.iterator iter.iterator iterator.cpp)
rv3_add_test(test.iter.common_iterator iter.common_iterator common_iterator.cpp)
rv3_add_test(test.iter.reverse_iterator iter.reverse_iterator reverse_iterator.cpp)
rv3_add_test(test.iter.unformatted_ostream_iterator iter.unformatted_ostream_iterator unformatted_ostream_iterator.cpp)
rv3_add_test(test.iter.counted_iterator iter.counted_iterator counted_iterator.cpp)
