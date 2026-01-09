# CUDA GPU acceleration library for libvroom
# Experimental feature - requires CUDA Toolkit 11.x+

# GPU parser library (CUDA kernels + host code)
add_library(vroom_gpu STATIC
    csv_gpu.cu
    gpu_parser.cpp
)

# Define LIBVROOM_ENABLE_GPU so the header exposes GPU types
target_compile_definitions(vroom_gpu PUBLIC LIBVROOM_ENABLE_GPU)

# Include directories
target_include_directories(vroom_gpu PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
)

# Link CUDA runtime
target_link_libraries(vroom_gpu PUBLIC
    CUDA::cudart
)

# CUDA-specific compile options
target_compile_options(vroom_gpu PRIVATE
    $<$<COMPILE_LANGUAGE:CUDA>:
        --expt-relaxed-constexpr
        -Xcompiler=-Wall
    >
)

# Don't use separable compilation - keeps linking simpler
set_target_properties(vroom_gpu PROPERTIES
    CUDA_SEPARABLE_COMPILATION OFF
    POSITION_INDEPENDENT_CODE ON
    CUDA_RESOLVE_DEVICE_SYMBOLS ON
)

# GPU benchmark (only if benchmarks enabled)
if(BUILD_BENCHMARKS)
    add_executable(gpu_benchmark
        gpu_benchmark.cpp
    )

    target_link_libraries(gpu_benchmark PRIVATE
        vroom
        vroom_gpu
        benchmark::benchmark
        benchmark::benchmark_main
        pthread
    )

    target_include_directories(gpu_benchmark PRIVATE
        ${CMAKE_SOURCE_DIR}/include
    )

    target_compile_definitions(gpu_benchmark PRIVATE LIBVROOM_BENCHMARK_MODE)
endif()

# GPU tests (only if testing enabled)
if(BUILD_TESTING)
    add_executable(gpu_parser_test
        gpu_parser_test.cpp
    )

    target_link_libraries(gpu_parser_test PRIVATE
        vroom
        vroom_gpu
        GTest::gtest_main
        pthread
    )

    target_include_directories(gpu_parser_test PRIVATE
        ${CMAKE_SOURCE_DIR}/include
    )

    # Register GPU tests with CTest
    include(GoogleTest)
    gtest_discover_tests(gpu_parser_test)
endif()
