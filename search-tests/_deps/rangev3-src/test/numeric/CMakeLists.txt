set(CMAKE_FOLDER "${CMAKE_FOLDER}/numeric")

rv3_add_test(test.num.accumulate num.accumulate accumulate.cpp)
rv3_add_test(test.num.adjacent_difference num.adjacent_difference adjacent_difference.cpp)
rv3_add_test(test.num.inner_product num.inner_product inner_product.cpp)
rv3_add_test(test.num.iota num.iota iota.cpp)
rv3_add_test(test.num.partial_sum num.partial_sum partial_sum.cpp)
