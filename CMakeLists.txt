cmake_minimum_required(VERSION 3.14)
project(simdcsv VERSION 0.1.0 LANGUAGES CXX)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Code coverage option
option(ENABLE_COVERAGE "Enable code coverage reporting" OFF)

# Compiler flags
# Only use -march=native on x86_64 platforms (AVX2 implementation)
# ARM support (NEON/SVE) is planned for future releases
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64)|(X86)")
    message(STATUS "x86_64 platform detected - enabling -march=native for AVX2")
    add_compile_options(-march=native)
else()
    message(STATUS "Non-x86_64 platform detected (${CMAKE_SYSTEM_PROCESSOR}) - skipping SIMD optimizations")
    message(STATUS "ARM SIMD support (NEON/SVE) is planned for future releases")
endif()

if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g)
else()
    add_compile_options(-O3)
endif()

# Code coverage flags
if(ENABLE_COVERAGE)
    message(STATUS "Code coverage enabled")
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        add_compile_options(--coverage -fprofile-arcs -ftest-coverage)
        add_link_options(--coverage)
    else()
        message(WARNING "Code coverage not supported for compiler: ${CMAKE_CXX_COMPILER_ID}")
    endif()
endif()

# Library target
add_library(simdcsv_lib STATIC
    src/io_util.cpp
    src/error.cpp
)

target_include_directories(simdcsv_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Main executable (only on x86_64 - requires AVX2)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64)|(X86)")
    add_executable(simdcsv
        src/main.cpp
    )

    target_link_libraries(simdcsv PRIVATE
        simdcsv_lib
        pthread
    )
else()
    message(STATUS "Skipping simdcsv main executable on non-x86_64 platform")
    message(STATUS "Test executables will still be built (no SIMD required)")
endif()

# Testing
enable_testing()

# Download and configure Google Test
include(FetchContent)
FetchContent_Declare(
  googletest
  GIT_REPOSITORY https://github.com/google/googletest.git
  GIT_TAG v1.14.0
)

# For Windows: Prevent overriding the parent project's compiler/linker settings
set(gtest_force_shared_crt ON CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(googletest)

# Test executable
add_executable(simdcsv_test
    test/csv_parser_test.cpp
)

target_link_libraries(simdcsv_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(simdcsv_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory
add_custom_command(TARGET simdcsv_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Error handling test executable
add_executable(error_handling_test
    test/error_handling_test.cpp
)

target_link_libraries(error_handling_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(error_handling_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for error handling tests
add_custom_command(TARGET error_handling_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register tests with CTest
include(GoogleTest)
gtest_discover_tests(simdcsv_test)
gtest_discover_tests(error_handling_test)

# CSV parsing test executable (only on x86_64 - requires AVX2 SIMD)
if(CMAKE_SYSTEM_PROCESSOR MATCHES "(x86_64)|(AMD64)|(X86)")
    add_executable(csv_parsing_test
        test/csv_parsing_test.cpp
    )

    target_link_libraries(csv_parsing_test PRIVATE
        simdcsv_lib
        GTest::gtest_main
        pthread
    )

    target_include_directories(csv_parsing_test PRIVATE
        ${CMAKE_CURRENT_SOURCE_DIR}/include
    )

    # Copy test data to build directory for parsing tests
    add_custom_command(TARGET csv_parsing_test POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E copy_directory
        ${CMAKE_CURRENT_SOURCE_DIR}/test/data
        ${CMAKE_CURRENT_BINARY_DIR}/test/data
    )

    # Register parsing tests with CTest
    gtest_discover_tests(csv_parsing_test)
else()
    message(STATUS "Skipping csv_parsing_test on non-x86_64 platform (requires AVX2)")
endif()
