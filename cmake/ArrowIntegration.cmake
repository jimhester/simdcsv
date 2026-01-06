# Apache Arrow Output Integration for libvroom
# Include this file AFTER defining vroom

if(LIBVROOM_ENABLE_ARROW)
    message(STATUS "Apache Arrow output integration enabled")
    find_package(Arrow REQUIRED)
    set(LIBVROOM_HAS_ARROW TRUE PARENT_SCOPE)

    target_sources(vroom PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/src/arrow_output.cpp)
    target_link_libraries(vroom PUBLIC Arrow::arrow_shared)
    target_compile_definitions(vroom PUBLIC LIBVROOM_ENABLE_ARROW)

    # Note: arrow_output_test is defined separately after GoogleTest is configured.
    # This file only handles adding Arrow support to vroom.
endif()
