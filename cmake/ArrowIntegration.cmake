# Apache Arrow Output Integration for simdcsv
# Include this file AFTER defining simdcsv_lib and GTest

if(SIMDCSV_ENABLE_ARROW)
    message(STATUS "Apache Arrow output integration enabled")
    find_package(Arrow REQUIRED)
    set(SIMDCSV_HAS_ARROW TRUE PARENT_SCOPE)

    target_sources(simdcsv_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_output.cpp)
    target_link_libraries(simdcsv_lib PUBLIC Arrow::arrow_shared)
    target_compile_definitions(simdcsv_lib PUBLIC SIMDCSV_ENABLE_ARROW)

    add_executable(arrow_output_test test/arrow_output_test.cpp)
    target_link_libraries(arrow_output_test PRIVATE simdcsv_lib GTest::gtest_main pthread)
    target_include_directories(arrow_output_test PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    add_custom_command(TARGET arrow_output_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/test/data ${CMAKE_CURRENT_BINARY_DIR}/test/data)
    gtest_discover_tests(arrow_output_test)
endif()
