cmake_minimum_required(VERSION 3.14)
project(simdcsv VERSION 0.1.0 LANGUAGES CXX C)

# Set C++ standard
set(CMAKE_CXX_STANDARD 17)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_CXX_EXTENSIONS OFF)

# Code coverage option
option(ENABLE_COVERAGE "Enable code coverage reporting" OFF)

# Compiler flags
if(CMAKE_BUILD_TYPE STREQUAL "Debug")
    add_compile_options(-O0 -g)
else()
    add_compile_options(-O3)
endif()

# Code coverage flags
if(ENABLE_COVERAGE)
    message(STATUS "Code coverage enabled")
    if(CMAKE_CXX_COMPILER_ID MATCHES "GNU|Clang")
        # Use atomic profile updates for thread-safe coverage in multi-threaded code
        add_compile_options(--coverage -fprofile-arcs -ftest-coverage -fprofile-update=atomic)
        add_link_options(--coverage)
    else()
        message(WARNING "Code coverage not supported for compiler: ${CMAKE_CXX_COMPILER_ID}")
    endif()
endif()

# Dependencies
include(FetchContent)

# Google Highway for portable SIMD
FetchContent_Declare(
  highway
  GIT_REPOSITORY https://github.com/google/highway.git
  GIT_TAG 1.3.0
)
set(HWY_ENABLE_TESTS OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_EXAMPLES OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_CONTRIB OFF CACHE BOOL "" FORCE)
set(HWY_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(highway)

# Google Benchmark for performance testing
FetchContent_Declare(
  benchmark
  GIT_REPOSITORY https://github.com/google/benchmark.git
  GIT_TAG v1.8.3
)
set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_INSTALL OFF CACHE BOOL "" FORCE)
set(BENCHMARK_ENABLE_DOXYGEN OFF CACHE BOOL "" FORCE)
set(BENCHMARK_INSTALL_DOCS OFF CACHE BOOL "" FORCE)
FetchContent_MakeAvailable(benchmark)

# External CSV parser benchmarks (optional)
option(ENABLE_DUCKDB_BENCHMARK "Enable DuckDB CSV parser benchmarks" OFF)
option(ENABLE_ZSV_BENCHMARK "Enable zsv CSV parser benchmarks" OFF)
option(ENABLE_ARROW_BENCHMARK "Enable Apache Arrow CSV parser benchmarks" OFF)

# zsv integration (lightweight, SIMD CSV parser)
# Note: zsv uses Makefile, so we download source only and build it as part of our project
if(ENABLE_ZSV_BENCHMARK)
  message(STATUS "Enabling zsv CSV parser benchmarks")

  # Download zsv source - use FetchContent_Populate manually to avoid CMake configure
  set(ZSV_SOURCE_DIR "${CMAKE_BINARY_DIR}/_deps/zsv-src")
  set(ZSV_DOWNLOAD_DIR "${CMAKE_BINARY_DIR}/_deps/zsv-subbuild")

  if(NOT EXISTS "${ZSV_SOURCE_DIR}/src/zsv.c")
    message(STATUS "Downloading zsv...")
    file(MAKE_DIRECTORY ${ZSV_DOWNLOAD_DIR})
    execute_process(
      COMMAND ${CMAKE_COMMAND} -E env GIT_TERMINAL_PROMPT=0
              git clone --depth 1 --branch v1.3.0
              https://github.com/liquidaty/zsv.git
              ${ZSV_SOURCE_DIR}
      RESULT_VARIABLE GIT_RESULT
    )
    if(NOT GIT_RESULT EQUAL 0)
      message(FATAL_ERROR "Failed to clone zsv repository")
    endif()
  endif()

  # Generate zsv.h from zsv.h.in (zsv uses sed to do this in their Makefile)
  if(NOT EXISTS "${ZSV_SOURCE_DIR}/include/zsv.h")
    message(STATUS "Generating zsv.h...")
    file(READ "${ZSV_SOURCE_DIR}/include/zsv.h.in" ZSV_H_IN)
    # Remove the __ZSV_EXTRAS__DEFINE__ placeholder (we don't need ZSV_EXTRAS)
    string(REPLACE "__ZSV_EXTRAS__DEFINE__" "" ZSV_H_OUT "${ZSV_H_IN}")
    file(WRITE "${ZSV_SOURCE_DIR}/include/zsv.h" "${ZSV_H_OUT}")
  endif()

  # Build zsv as a static library from source
  add_library(zsv_lib STATIC
    ${ZSV_SOURCE_DIR}/src/zsv.c
  )
  target_include_directories(zsv_lib PUBLIC
    ${ZSV_SOURCE_DIR}/include
  )
  # zsv source files use relative includes, need to add include dir
  target_include_directories(zsv_lib PRIVATE
    ${ZSV_SOURCE_DIR}/include/zsv
  )
  # zsv uses C11/GNU11
  set_target_properties(zsv_lib PROPERTIES
    C_STANDARD 11
    C_STANDARD_REQUIRED ON
  )
  # Add compiler flags matching zsv's Makefile
  target_compile_options(zsv_lib PRIVATE
    -O3
    -fsigned-char
    -DNDEBUG
    -fPIC
  )
  # Suppress some warnings from zsv code
  target_compile_options(zsv_lib PRIVATE
    -Wno-missing-braces
    -Wno-gnu-statement-expression
  )

  set(HAVE_ZSV TRUE)
  set(ZSV_INCLUDE_DIR ${ZSV_SOURCE_DIR}/include)
endif()

# DuckDB integration (full database with CSV reader)
if(ENABLE_DUCKDB_BENCHMARK)
  message(STATUS "Enabling DuckDB CSV parser benchmarks (this may take a while to build)")
  FetchContent_Declare(
    duckdb
    GIT_REPOSITORY https://github.com/duckdb/duckdb.git
    GIT_TAG v1.1.3
    GIT_SHALLOW TRUE
  )
  set(BUILD_SHELL OFF CACHE BOOL "" FORCE)
  set(BUILD_UNITTESTS OFF CACHE BOOL "" FORCE)
  set(ENABLE_SANITIZER OFF CACHE BOOL "" FORCE)
  set(BUILD_PARQUET_EXTENSION OFF CACHE BOOL "" FORCE)
  set(BUILD_HTTPFS_EXTENSION OFF CACHE BOOL "" FORCE)
  set(BUILD_JSON_EXTENSION OFF CACHE BOOL "" FORCE)
  FetchContent_MakeAvailable(duckdb)
  set(HAVE_DUCKDB TRUE)
endif()

# Apache Arrow integration (columnar CSV reader)
if(ENABLE_ARROW_BENCHMARK)
  message(STATUS "Enabling Apache Arrow CSV parser benchmarks")
  find_package(Arrow REQUIRED)
  set(HAVE_ARROW TRUE)
endif()

# Library target
add_library(simdcsv_lib STATIC
    src/io_util.cpp
    src/error.cpp
    src/dialect.cpp
    src/simdcsv_c.cpp
    src/value_extraction.cpp
    src/streaming.cpp
)

target_include_directories(simdcsv_lib PUBLIC
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(simdcsv_lib PUBLIC
    hwy
)

# Main executable (portable SIMD via Highway)
add_executable(simdcsv
    src/main.cpp
)

target_link_libraries(simdcsv PRIVATE
    simdcsv_lib
    pthread
)

# CLI utility (user-friendly CSV tool)
add_executable(scsv
    src/cli.cpp
)

target_link_libraries(scsv PRIVATE
    simdcsv_lib
    pthread
)

# Suppress debug output for CLI
target_compile_definitions(scsv PRIVATE SIMDCSV_BENCHMARK_MODE)

# Testing
enable_testing()

# Download and configure Google Test
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

# CSV parsing test executable (portable SIMD via Highway)
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

# CSV parser error detection test executable
add_executable(csv_parser_errors_test
    test/csv_parser_errors_test.cpp
)

target_link_libraries(csv_parser_errors_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(csv_parser_errors_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for parser error tests
add_custom_command(TARGET csv_parser_errors_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register parser error tests with CTest
gtest_discover_tests(csv_parser_errors_test)

# Extended CSV test executable (additional coverage from zsv/duckdb)
add_executable(csv_extended_test
    test/csv_extended_test.cpp
)

target_link_libraries(csv_extended_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(csv_extended_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for extended tests
add_custom_command(TARGET csv_extended_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register extended tests with CTest
gtest_discover_tests(csv_extended_test)

# Dialect detection test executable
add_executable(dialect_detection_test
    test/dialect_detection_test.cpp
)

target_link_libraries(dialect_detection_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(dialect_detection_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for dialect tests
add_custom_command(TARGET dialect_detection_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register dialect detection tests with CTest
gtest_discover_tests(dialect_detection_test)

# Type detection test executable
add_executable(type_detection_test
    test/type_detection_test.cpp
)

target_link_libraries(type_detection_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(type_detection_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for type detection tests
add_custom_command(TARGET type_detection_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register type detection tests with CTest
gtest_discover_tests(type_detection_test)

# C API test executable
add_executable(c_api_test
    test/c_api_test.cpp
)

target_link_libraries(c_api_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(c_api_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for C API tests
add_custom_command(TARGET c_api_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register C API tests with CTest
gtest_discover_tests(c_api_test)

# Debug mode test executable
add_executable(debug_test
    test/debug_test.cpp
)

target_link_libraries(debug_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(debug_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Register debug tests with CTest
gtest_discover_tests(debug_test)

# Simplified API test executable
add_executable(api_test
    test/api_test.cpp
)

target_link_libraries(api_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(api_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for API tests
add_custom_command(TARGET api_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register API tests with CTest
gtest_discover_tests(api_test)

# Quote mask unit tests (verifies PCLMULQDQ/PMULL implementation)
add_executable(quote_mask_test
    test/quote_mask_test.cpp
)

target_link_libraries(quote_mask_test PRIVATE
    hwy
    GTest::gtest_main
    pthread
)

target_include_directories(quote_mask_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

gtest_discover_tests(quote_mask_test)

# Value extraction test executable
add_executable(value_extraction_test
    test/value_extraction_test.cpp
)

target_link_libraries(value_extraction_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(value_extraction_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for value extraction tests
add_custom_command(TARGET value_extraction_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register value extraction tests with CTest
gtest_discover_tests(value_extraction_test)

# Streaming parser test executable
add_executable(streaming_test
    test/streaming_test.cpp
)

target_link_libraries(streaming_test PRIVATE
    simdcsv_lib
    GTest::gtest_main
    pthread
)

target_include_directories(streaming_test PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Copy test data to build directory for streaming tests
add_custom_command(TARGET streaming_test POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/test/data
)

# Register streaming tests with CTest
gtest_discover_tests(streaming_test)

# Benchmark executable - base sources
set(BENCHMARK_SOURCES
    benchmark/benchmark_main.cpp
    benchmark/basic_benchmarks.cpp
    benchmark/comparison_benchmarks.cpp
    benchmark/real_world_benchmarks.cpp
    benchmark/dimensions_benchmarks.cpp
    benchmark/performance_metrics.cpp
    benchmark/simd_benchmarks.cpp
    benchmark/energy_benchmarks.cpp
)

# Add external parser benchmarks if any external parser is enabled
if(HAVE_ZSV OR HAVE_DUCKDB OR HAVE_ARROW)
    list(APPEND BENCHMARK_SOURCES benchmark/external_parser_benchmarks.cpp)
endif()

add_executable(simdcsv_benchmark ${BENCHMARK_SOURCES})

target_link_libraries(simdcsv_benchmark PRIVATE
    simdcsv_lib
    benchmark::benchmark
    benchmark::benchmark_main
    pthread
)

target_include_directories(simdcsv_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# Define benchmark mode to suppress debug output during benchmarking
target_compile_definitions(simdcsv_benchmark PRIVATE SIMDCSV_BENCHMARK_MODE)

# Conditionally link external CSV parsers
if(HAVE_ZSV)
    target_link_libraries(simdcsv_benchmark PRIVATE zsv_lib)
    target_compile_definitions(simdcsv_benchmark PRIVATE HAVE_ZSV)
endif()

if(HAVE_DUCKDB)
    target_link_libraries(simdcsv_benchmark PRIVATE duckdb)
    target_compile_definitions(simdcsv_benchmark PRIVATE HAVE_DUCKDB)
endif()

if(HAVE_ARROW)
    target_link_libraries(simdcsv_benchmark PRIVATE Arrow::arrow_shared)
    target_compile_definitions(simdcsv_benchmark PRIVATE HAVE_ARROW)
endif()

# Copy test data to build directory for benchmarks
add_custom_command(TARGET simdcsv_benchmark POST_BUILD
    COMMAND ${CMAKE_COMMAND} -E copy_directory
    ${CMAKE_CURRENT_SOURCE_DIR}/test/data
    ${CMAKE_CURRENT_BINARY_DIR}/benchmark/data
)

# Quote mask micro-benchmark (scalar vs CLMul comparison)
# This is a standalone benchmark for measuring the performance improvement
# of the PCLMULQDQ/PMULL-based quote mask implementation vs the scalar loop.
# Not part of the main benchmark suite - run separately with ./quote_mask_benchmark
add_executable(quote_mask_benchmark benchmark/quote_mask_benchmark.cpp)
target_link_libraries(quote_mask_benchmark PRIVATE
    hwy
    benchmark::benchmark
)
target_include_directories(quote_mask_benchmark PRIVATE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

# =============================================================================
# Fuzz Testing Support
# =============================================================================
option(ENABLE_FUZZING "Enable fuzz testing targets (requires Clang with libFuzzer)" OFF)

if(ENABLE_FUZZING)
    message(STATUS "Fuzz testing enabled")

    if(NOT CMAKE_CXX_COMPILER_ID MATCHES "Clang")
        message(FATAL_ERROR "Fuzz testing requires Clang compiler")
    endif()

    set(FUZZ_FLAGS -fsanitize=fuzzer,address,undefined -fno-omit-frame-pointer -g)
    set(FUZZ_LINK_FLAGS -fsanitize=fuzzer,address,undefined)

    add_executable(fuzz_csv_parser fuzz/fuzz_csv_parser.cpp)
    target_link_libraries(fuzz_csv_parser PRIVATE simdcsv_lib)
    target_include_directories(fuzz_csv_parser PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(fuzz_csv_parser PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_csv_parser PRIVATE ${FUZZ_LINK_FLAGS})
    target_compile_definitions(fuzz_csv_parser PRIVATE SIMDCSV_BENCHMARK_MODE)

    add_executable(fuzz_dialect_detection fuzz/fuzz_dialect_detection.cpp)
    target_link_libraries(fuzz_dialect_detection PRIVATE simdcsv_lib)
    target_include_directories(fuzz_dialect_detection PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(fuzz_dialect_detection PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_dialect_detection PRIVATE ${FUZZ_LINK_FLAGS})
    target_compile_definitions(fuzz_dialect_detection PRIVATE SIMDCSV_BENCHMARK_MODE)

    add_executable(fuzz_parse_auto fuzz/fuzz_parse_auto.cpp)
    target_link_libraries(fuzz_parse_auto PRIVATE simdcsv_lib)
    target_include_directories(fuzz_parse_auto PRIVATE ${CMAKE_CURRENT_SOURCE_DIR}/include)
    target_compile_options(fuzz_parse_auto PRIVATE ${FUZZ_FLAGS})
    target_link_options(fuzz_parse_auto PRIVATE ${FUZZ_LINK_FLAGS})
    target_compile_definitions(fuzz_parse_auto PRIVATE SIMDCSV_BENCHMARK_MODE)

    add_custom_command(TARGET fuzz_csv_parser POST_BUILD
        COMMAND ${CMAKE_COMMAND} -E make_directory ${CMAKE_CURRENT_BINARY_DIR}/fuzz_corpus
        COMMAND ${CMAKE_COMMAND} -E copy_directory ${CMAKE_CURRENT_SOURCE_DIR}/test/data ${CMAKE_CURRENT_BINARY_DIR}/fuzz_corpus
        COMMENT "Setting up fuzz corpus from test data"
    )
endif()
