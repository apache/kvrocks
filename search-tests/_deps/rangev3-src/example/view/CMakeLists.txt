set(CMAKE_FOLDER "${CMAKE_FOLDER}/view")

rv3_add_test(example.view.transform example.view.transform transform.cpp)
rv3_add_test(example.view.ints example.view.ints ints.cpp)
rv3_add_test(example.view.filter example.view.filter filter.cpp)

# NOTE: Make sure the output matches the contents of the corresponding golden file
set_tests_properties(range.v3.example.view.transform PROPERTIES PASS_REGULAR_EXPRESSION "\\[0.5,1,1.5\\]")
set_tests_properties(range.v3.example.view.ints PROPERTIES PASS_REGULAR_EXPRESSION "\\[3,4,5,6\\]")
set_tests_properties(range.v3.example.view.filter PROPERTIES PASS_REGULAR_EXPRESSION "\\[2,4\\]")

