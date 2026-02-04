# CUDA GPU acceleration library for libvroom
# Experimental feature - requires CUDA Toolkit 11.x+

include(CheckLanguage)
check_language(CUDA)
if(NOT CMAKE_CUDA_COMPILER)
    message(FATAL_ERROR "ENABLE_GPU=ON but no CUDA compiler found. Install CUDA Toolkit.")
endif()
enable_language(CUDA)
find_package(CUDAToolkit REQUIRED)

set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

if(NOT DEFINED CMAKE_CUDA_ARCHITECTURES)
    set(CMAKE_CUDA_ARCHITECTURES native)
endif()
message(STATUS "CUDA architectures: ${CMAKE_CUDA_ARCHITECTURES}")

# WSL CUDA support
if(EXISTS "/usr/lib/wsl/lib/libcuda.so")
    link_directories(/usr/lib/wsl/lib)
    message(STATUS "WSL CUDA detected, adding /usr/lib/wsl/lib")
endif()

# GPU parsing library
add_library(vroom_gpu STATIC
    csv_gpu.cu
    gpu_parser.cpp
)
target_include_directories(vroom_gpu PUBLIC
    ${CMAKE_SOURCE_DIR}/include
    ${CMAKE_CURRENT_SOURCE_DIR}
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
)
target_compile_definitions(vroom_gpu PUBLIC LIBVROOM_ENABLE_GPU)
target_link_libraries(vroom_gpu PRIVATE CUDA::cudart)

target_compile_options(vroom_gpu PRIVATE
    $<$<COMPILE_LANGUAGE:CUDA>:
        --expt-relaxed-constexpr
        -Xcompiler=-Wall
    >
)

set_target_properties(vroom_gpu PROPERTIES
    CUDA_SEPARABLE_COMPILATION OFF
    POSITION_INDEPENDENT_CODE ON
    CUDA_RESOLVE_DEVICE_SYMBOLS ON
)
