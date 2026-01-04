# Apache Arrow Output Integration for simdcsv
# Include this file AFTER defining simdcsv_lib

if(SIMDCSV_ENABLE_ARROW)
    message(STATUS "Apache Arrow output integration enabled")
    find_package(Arrow REQUIRED)
    set(SIMDCSV_HAS_ARROW TRUE PARENT_SCOPE)

    target_sources(simdcsv_lib PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_output.cpp)
    target_link_libraries(simdcsv_lib PUBLIC Arrow::arrow_shared)
    target_compile_definitions(simdcsv_lib PUBLIC SIMDCSV_ENABLE_ARROW)

    # Note: arrow_output_test is defined separately after GoogleTest is configured.
    # This file only handles adding Arrow support to simdcsv_lib.
endif()
