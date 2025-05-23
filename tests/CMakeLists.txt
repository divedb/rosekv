add_executable(segment_test "wal/segment_test.cc")
target_compile_options(segment_test PRIVATE ${KIWI_DEFAULT_COPTS} -g)
target_include_directories(segment_test PRIVATE ${ROSEKV_INCLUDE_DIR} ${KIWI_COMMON_INCLUDE_DIRS})
target_link_libraries(segment_test PRIVATE kiwi::io kiwi::metrics GTest::gtest GTest::gtest_main)