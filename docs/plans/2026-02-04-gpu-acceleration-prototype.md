# GPU-Accelerated CSV Parsing Prototype

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Benchmark three CSV parsing approaches (full GPU, hybrid CPU+GPU, CPU baseline) to determine if GPU acceleration is worthwhile for libvroom.

**Architecture:** Optional CUDA module in `cuda/` directory, enabled via `-DENABLE_GPU=ON`. GPU kernels handle field boundary detection using CUB prefix XOR scan for quote state tracking. A benchmark harness generates CSV files at various sizes and measures throughput (GB/s) broken down by phase.

**Tech Stack:** CUDA 13.1, CUB (bundled), Thrust, Google Test, Google Benchmark. Target: RTX 5070 Ti (sm_120, 16GB, 70 SMs).

**Reference:** Kumaigorodski et al., "Fast CSV Loading Using GPUs and RDMA" (BTW 2021) — achieves 76 GB/s using multi-pass GPU pipeline with prefix sums.

---

## Environment Setup

```bash
# Source CUDA environment
source ~/.zshenv.local

# Working directory
cd /home/jimhester/.worktrees/libvroom/gpu-acceleration

# Build with GPU enabled
cmake -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=ON
cmake --build build -j$(nproc)

# Build without GPU (verify no breakage)
cmake -B build-nogpu -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=OFF
cmake --build build-nogpu -j$(nproc)

# Run tests (should pass with GPU on or off)
cd build && ctest --output-on-failure -j$(nproc)

# Run GPU benchmark
./build/cuda/gpu_benchmark
```

**IMPORTANT:** Always pass `-L/usr/lib/wsl/lib -lcuda` for WSL CUDA linking. The CMakeLists.txt handles this automatically when `ENABLE_GPU=ON`. Must use `-arch=sm_120` (or `CMAKE_CUDA_ARCHITECTURES=120`) for the RTX 5070 Ti.

---

## Task 1: CMake CUDA Integration

**Files:**
- Modify: `CMakeLists.txt` (add GPU option and cuda/ subdirectory)
- Create: `cuda/CMakeLists.txt` (CUDA build rules)

**Step 1: Add GPU option to root CMakeLists.txt**

Add after the existing options block (around line 37):

```cmake
# GPU acceleration (experimental, requires CUDA Toolkit)
option(ENABLE_GPU "Enable CUDA GPU-accelerated parsing (experimental)" OFF)
```

Add at the end of CMakeLists.txt (before the closing comments, after test/benchmark sections):

```cmake
# =============================================================================
# GPU acceleration (optional)
# =============================================================================
if(ENABLE_GPU)
    add_subdirectory(cuda)
endif()
```

**Step 2: Create cuda/CMakeLists.txt**

```cmake
# Require CUDA
include(CheckLanguage)
check_language(CUDA)
if(NOT CMAKE_CUDA_COMPILER)
    message(FATAL_ERROR "ENABLE_GPU=ON but no CUDA compiler found. Install CUDA Toolkit.")
endif()
enable_language(CUDA)

# CUDA standard and architecture
set(CMAKE_CUDA_STANDARD 17)
set(CMAKE_CUDA_STANDARD_REQUIRED ON)

# Auto-detect GPU architecture, with manual override
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
    ${CMAKE_CUDA_TOOLKIT_INCLUDE_DIRECTORIES}
)
target_compile_definitions(vroom_gpu PUBLIC LIBVROOM_ENABLE_GPU)
target_link_libraries(vroom_gpu PRIVATE cuda)

# GPU benchmark
add_executable(gpu_benchmark gpu_benchmark.cpp)
target_link_libraries(gpu_benchmark PRIVATE
    vroom_gpu
    libvroom
    benchmark::benchmark
    benchmark::benchmark_main
)
target_compile_definitions(gpu_benchmark PRIVATE LIBVROOM_ENABLE_GPU)

# GPU tests
add_executable(gpu_parser_test gpu_parser_test.cpp)
target_link_libraries(gpu_parser_test PRIVATE
    vroom_gpu
    libvroom
    GTest::gtest_main
)
target_compile_definitions(gpu_parser_test PRIVATE LIBVROOM_ENABLE_GPU)
add_test(NAME gpu_parser_test COMMAND gpu_parser_test)
```

**Step 3: Verify build works with ENABLE_GPU=OFF (no breakage)**

```bash
source ~/.zshenv.local
cd /home/jimhester/.worktrees/libvroom/gpu-acceleration
cmake -B build -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=OFF -DBUILD_TESTING=ON
cmake --build build -j$(nproc)
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: All existing tests pass, no CUDA code compiled.

**Step 4: Verify build configures with ENABLE_GPU=ON**

```bash
source ~/.zshenv.local
cmake -B build-gpu -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=ON -DBUILD_TESTING=ON
```

Expected: CMake configures successfully, finds CUDA compiler. Build will fail because cuda/*.cu files don't exist yet — that's fine.

**Step 5: Commit**

```bash
git add CMakeLists.txt cuda/CMakeLists.txt
git commit -m "build: Add optional CUDA GPU support via -DENABLE_GPU=ON"
```

---

## Task 2: GPU Kernel — Field Boundary Detection

**Files:**
- Create: `cuda/csv_gpu.cuh` (data structures and declarations)
- Create: `cuda/csv_gpu.cu` (CUDA kernels)

This implements the core GPU parsing: character scanning, quote state tracking via CUB prefix XOR scan, and field boundary extraction.

**Step 1: Create cuda/csv_gpu.cuh — data structures**

```cpp
#ifndef VROOM_CSV_GPU_CUH
#define VROOM_CSV_GPU_CUH

#include <cstddef>
#include <cstdint>

namespace libvroom {
namespace gpu {

// Configuration for GPU parsing
struct GpuParseConfig {
  char delimiter = ',';
  char quote_char = '"';
  bool handle_quotes = true;
  int block_size = 256;
  int max_blocks = 0; // 0 = auto-calculate
};

// Timing breakdown for benchmarking
struct GpuTimings {
  float h2d_ms = 0.0f;
  float kernel_ms = 0.0f;
  float d2h_ms = 0.0f;
  float total_ms = 0.0f;
};

// GPU device info
struct GpuInfo {
  bool available = false;
  int device_count = 0;
  char device_name[256] = {};
  size_t total_memory = 0;
  size_t free_memory = 0;
  int compute_major = 0;
  int compute_minor = 0;
  int sm_count = 0;
  int max_threads_per_block = 0;
};

// Result of GPU field boundary detection
// Positions are byte offsets into the original CSV data where field separators
// (delimiters and newlines outside quotes) occur.
struct GpuIndexResult {
  bool success = false;
  const char* error_message = nullptr;

  // Host-side results (populated after D2H copy)
  uint64_t* field_positions = nullptr; // Caller-owned, allocated by caller
  uint64_t num_fields = 0;
  uint64_t num_lines = 0;

  // Timing info
  GpuTimings timings;
};

// Query GPU info
GpuInfo query_gpu_info();

// Should we use GPU for this data size?
bool should_use_gpu(size_t data_size, const GpuInfo& info);

// Full GPU pipeline: scan entire CSV, find all field boundaries
// Caller must provide field_positions buffer of sufficient size,
// or pass nullptr to just get counts (two-phase usage).
//
// Phase 1: Call with field_positions=nullptr to get num_fields/num_lines counts
// Phase 2: Allocate and call again with field_positions buffer
//
// Or single-phase: pre-allocate conservatively (data_size / 2 is safe upper bound)
GpuIndexResult gpu_find_field_boundaries(
    const char* data,
    size_t len,
    const GpuParseConfig& config,
    uint64_t* field_positions = nullptr,
    size_t field_positions_capacity = 0
);

// Free any GPU-side resources from a result
void gpu_cleanup(GpuIndexResult& result);

} // namespace gpu
} // namespace libvroom

#endif // VROOM_CSV_GPU_CUH
```

**Step 2: Create cuda/csv_gpu.cu — GPU kernels**

The implementation follows the Kumaigorodski approach: multiple clean passes trading bandwidth for eliminated warp divergence.

```cuda
#include "csv_gpu.cuh"

#include <cub/cub.cuh>
#include <cuda_runtime.h>
#include <cstdio>

namespace libvroom {
namespace gpu {

// ---------------------------------------------------------------------------
// Error checking
// ---------------------------------------------------------------------------
#define CUDA_CHECK(call)                                                     \
  do {                                                                       \
    cudaError_t err_ = (call);                                               \
    if (err_ != cudaSuccess) {                                               \
      fprintf(stderr, "CUDA error %s:%d: %s\n", __FILE__, __LINE__,          \
              cudaGetErrorString(err_));                                      \
      result.error_message = cudaGetErrorString(err_);                       \
      goto cleanup;                                                          \
    }                                                                        \
  } while (0)

#define CUDA_CHECK_KERNEL()                                                  \
  do {                                                                       \
    cudaError_t err_ = cudaGetLastError();                                   \
    if (err_ != cudaSuccess) {                                               \
      fprintf(stderr, "Kernel error %s:%d: %s\n", __FILE__, __LINE__,        \
              cudaGetErrorString(err_));                                      \
      result.error_message = cudaGetErrorString(err_);                       \
      goto cleanup;                                                          \
    }                                                                        \
  } while (0)

// ---------------------------------------------------------------------------
// Kernel 1: Build quote flag array (1 byte per input byte: 1=quote, 0=not)
// ---------------------------------------------------------------------------
__global__ void build_quote_flags_kernel(
    const char* __restrict__ data,
    size_t len,
    char quote_char,
    uint8_t* __restrict__ quote_flags
) {
  size_t idx = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
  size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;
  for (size_t i = idx; i < len; i += stride) {
    quote_flags[i] = (data[i] == quote_char) ? 1 : 0;
  }
}

// ---------------------------------------------------------------------------
// XOR operator for CUB inclusive scan (quote parity)
// ---------------------------------------------------------------------------
struct XorOp {
  __device__ __forceinline__ uint8_t operator()(uint8_t a, uint8_t b) const {
    return a ^ b;
  }
};

// ---------------------------------------------------------------------------
// Kernel 2: Find field boundaries (delimiters + newlines outside quotes)
// Two outputs: count only (positions=nullptr) or count + write positions
// ---------------------------------------------------------------------------
__global__ void find_boundaries_kernel(
    const char* __restrict__ data,
    size_t len,
    char delimiter,
    const uint8_t* __restrict__ quote_state, // 0=outside, 1=inside
    uint64_t* __restrict__ positions,         // nullptr for count-only mode
    uint32_t* __restrict__ field_count,
    uint32_t* __restrict__ line_count
) {
  size_t idx = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
  size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

  for (size_t i = idx; i < len; i += stride) {
    bool outside_quote = (quote_state == nullptr) || (quote_state[i] == 0);
    if (!outside_quote) continue;

    char c = data[i];
    if (c == delimiter) {
      uint32_t pos = atomicAdd(field_count, 1);
      if (positions != nullptr) {
        positions[pos] = static_cast<uint64_t>(i);
      }
    } else if (c == '\n') {
      atomicAdd(field_count, 1);
      uint32_t line_pos = atomicAdd(line_count, 1);
      if (positions != nullptr) {
        // Line endings are also field boundaries
        uint32_t pos = atomicAdd(field_count, 0) - 1; // Already incremented
        // Actually, let's use a unified counter
      }
    }
  }
}

// Simpler version: unified boundary detection
// A "boundary" is any delimiter or newline that's outside quotes.
// This produces a flat sorted array of byte offsets.
__global__ void find_all_boundaries_kernel(
    const char* __restrict__ data,
    size_t len,
    char delimiter,
    const uint8_t* __restrict__ quote_state,
    uint64_t* __restrict__ positions,
    uint32_t* __restrict__ count
) {
  size_t idx = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
  size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

  for (size_t i = idx; i < len; i += stride) {
    bool outside = (quote_state == nullptr) || (quote_state[i] == 0);
    if (!outside) continue;

    char c = data[i];
    if (c == delimiter || c == '\n') {
      uint32_t pos = atomicAdd(count, 1);
      if (positions != nullptr) {
        positions[pos] = static_cast<uint64_t>(i);
      }
    }
  }
}

// ---------------------------------------------------------------------------
// Kernel 3: Count newlines outside quotes (for line count)
// ---------------------------------------------------------------------------
__global__ void count_lines_kernel(
    const char* __restrict__ data,
    size_t len,
    const uint8_t* __restrict__ quote_state,
    uint32_t* __restrict__ line_count
) {
  size_t idx = static_cast<size_t>(blockIdx.x) * blockDim.x + threadIdx.x;
  size_t stride = static_cast<size_t>(blockDim.x) * gridDim.x;

  uint32_t local_count = 0;
  for (size_t i = idx; i < len; i += stride) {
    bool outside = (quote_state == nullptr) || (quote_state[i] == 0);
    if (outside && data[i] == '\n') {
      local_count++;
    }
  }

  // Warp reduction
  for (int offset = 16; offset > 0; offset /= 2) {
    local_count += __shfl_down_sync(0xffffffff, local_count, offset);
  }
  if (threadIdx.x % 32 == 0) {
    atomicAdd(line_count, local_count);
  }
}

// ---------------------------------------------------------------------------
// Host functions
// ---------------------------------------------------------------------------

GpuInfo query_gpu_info() {
  GpuInfo info;
  int count = 0;
  if (cudaGetDeviceCount(&count) != cudaSuccess || count == 0) {
    return info;
  }

  info.available = true;
  info.device_count = count;

  cudaDeviceProp prop;
  if (cudaGetDeviceProperties(&prop, 0) == cudaSuccess) {
    strncpy(info.device_name, prop.name, sizeof(info.device_name) - 1);
    info.total_memory = prop.totalGlobalMem;
    info.compute_major = prop.major;
    info.compute_minor = prop.minor;
    info.sm_count = prop.multiProcessorCount;
    info.max_threads_per_block = prop.maxThreadsPerBlock;
  }

  size_t free_mem = 0, total_mem = 0;
  if (cudaMemGetInfo(&free_mem, &total_mem) == cudaSuccess) {
    info.free_memory = free_mem;
  }

  return info;
}

bool should_use_gpu(size_t data_size, const GpuInfo& info) {
  if (!info.available) return false;
  // Empirical threshold from prior benchmarking
  constexpr size_t MIN_SIZE = 10 * 1024 * 1024; // 10 MB
  if (data_size < MIN_SIZE) return false;
  // Need ~3x data size for GPU buffers
  if (data_size * 3 > info.free_memory) return false;
  return true;
}

GpuIndexResult gpu_find_field_boundaries(
    const char* data,
    size_t len,
    const GpuParseConfig& config,
    uint64_t* field_positions,
    size_t field_positions_capacity
) {
  GpuIndexResult result;

  // Device pointers — all initialized to nullptr for safe cleanup
  char* d_data = nullptr;
  uint8_t* d_quote_flags = nullptr;
  uint8_t* d_quote_state = nullptr;
  void* d_cub_temp = nullptr;
  uint64_t* d_positions = nullptr;
  uint32_t* d_field_count = nullptr;
  uint32_t* d_line_count = nullptr;

  // CUDA timing events
  cudaEvent_t ev_start = nullptr, ev_h2d = nullptr, ev_kernel = nullptr, ev_d2h = nullptr;

  // Create timing events
  CUDA_CHECK(cudaEventCreate(&ev_start));
  CUDA_CHECK(cudaEventCreate(&ev_h2d));
  CUDA_CHECK(cudaEventCreate(&ev_kernel));
  CUDA_CHECK(cudaEventCreate(&ev_d2h));
  CUDA_CHECK(cudaEventRecord(ev_start));

  // Compute grid dimensions
  int block_size = config.block_size;
  int max_blocks = config.max_blocks;
  if (max_blocks <= 0) {
    // Heuristic: enough blocks to keep all SMs busy
    GpuInfo info = query_gpu_info();
    max_blocks = info.sm_count * 4;
    if (max_blocks <= 0) max_blocks = 256;
  }
  int num_blocks = std::min(max_blocks,
      static_cast<int>((len + block_size - 1) / block_size));

  // --- H2D Transfer ---
  CUDA_CHECK(cudaMalloc(&d_data, len));
  CUDA_CHECK(cudaMemcpy(d_data, data, len, cudaMemcpyHostToDevice));
  CUDA_CHECK(cudaEventRecord(ev_h2d));

  // --- Quote State Computation ---
  if (config.handle_quotes) {
    CUDA_CHECK(cudaMalloc(&d_quote_flags, len));
    CUDA_CHECK(cudaMalloc(&d_quote_state, len));

    // Pass 1: Build quote flags (1=quote char, 0=other)
    build_quote_flags_kernel<<<num_blocks, block_size>>>(
        d_data, len, config.quote_char, d_quote_flags);
    CUDA_CHECK_KERNEL();

    // Pass 2: Prefix XOR scan to compute quote state
    // After scan: position i has value 1 if inside quotes, 0 if outside
    size_t cub_temp_bytes = 0;
    cub::DeviceScan::InclusiveScan(
        nullptr, cub_temp_bytes,
        d_quote_flags, d_quote_state, XorOp(), len);

    CUDA_CHECK(cudaMalloc(&d_cub_temp, cub_temp_bytes));
    cub::DeviceScan::InclusiveScan(
        d_cub_temp, cub_temp_bytes,
        d_quote_flags, d_quote_state, XorOp(), len);
    CUDA_CHECK_KERNEL();
  }

  // --- Boundary Detection ---
  CUDA_CHECK(cudaMalloc(&d_field_count, sizeof(uint32_t)));
  CUDA_CHECK(cudaMalloc(&d_line_count, sizeof(uint32_t)));
  CUDA_CHECK(cudaMemset(d_field_count, 0, sizeof(uint32_t)));
  CUDA_CHECK(cudaMemset(d_line_count, 0, sizeof(uint32_t)));

  if (field_positions != nullptr && field_positions_capacity > 0) {
    // Full mode: count + write positions
    CUDA_CHECK(cudaMalloc(&d_positions, field_positions_capacity * sizeof(uint64_t)));

    find_all_boundaries_kernel<<<num_blocks, block_size>>>(
        d_data, len, config.delimiter,
        config.handle_quotes ? d_quote_state : nullptr,
        d_positions, d_field_count);
    CUDA_CHECK_KERNEL();
  } else {
    // Count-only mode
    find_all_boundaries_kernel<<<num_blocks, block_size>>>(
        d_data, len, config.delimiter,
        config.handle_quotes ? d_quote_state : nullptr,
        nullptr, d_field_count);
    CUDA_CHECK_KERNEL();
  }

  // Count lines
  count_lines_kernel<<<num_blocks, block_size>>>(
      d_data, len,
      config.handle_quotes ? d_quote_state : nullptr,
      d_line_count);
  CUDA_CHECK_KERNEL();

  CUDA_CHECK(cudaEventRecord(ev_kernel));

  // --- D2H Transfer ---
  uint32_t h_field_count = 0, h_line_count = 0;
  CUDA_CHECK(cudaMemcpy(&h_field_count, d_field_count, sizeof(uint32_t), cudaMemcpyDeviceToHost));
  CUDA_CHECK(cudaMemcpy(&h_line_count, d_line_count, sizeof(uint32_t), cudaMemcpyDeviceToHost));

  result.num_fields = h_field_count;
  result.num_lines = h_line_count;

  if (field_positions != nullptr && h_field_count > 0) {
    size_t copy_count = std::min(static_cast<size_t>(h_field_count), field_positions_capacity);
    CUDA_CHECK(cudaMemcpy(field_positions, d_positions,
                          copy_count * sizeof(uint64_t), cudaMemcpyDeviceToHost));
    result.field_positions = field_positions;
  }

  CUDA_CHECK(cudaEventRecord(ev_d2h));
  CUDA_CHECK(cudaEventSynchronize(ev_d2h));

  // Compute timings
  cudaEventElapsedTime(&result.timings.h2d_ms, ev_start, ev_h2d);
  cudaEventElapsedTime(&result.timings.kernel_ms, ev_h2d, ev_kernel);
  cudaEventElapsedTime(&result.timings.d2h_ms, ev_kernel, ev_d2h);
  cudaEventElapsedTime(&result.timings.total_ms, ev_start, ev_d2h);

  result.success = true;

cleanup:
  // Free all device memory
  if (d_data) cudaFree(d_data);
  if (d_quote_flags) cudaFree(d_quote_flags);
  if (d_quote_state) cudaFree(d_quote_state);
  if (d_cub_temp) cudaFree(d_cub_temp);
  if (d_positions) cudaFree(d_positions);
  if (d_field_count) cudaFree(d_field_count);
  if (d_line_count) cudaFree(d_line_count);
  if (ev_start) cudaEventDestroy(ev_start);
  if (ev_h2d) cudaEventDestroy(ev_h2d);
  if (ev_kernel) cudaEventDestroy(ev_kernel);
  if (ev_d2h) cudaEventDestroy(ev_d2h);

  return result;
}

void gpu_cleanup(GpuIndexResult& result) {
  // Host-side positions are caller-owned, nothing to free here
  result.field_positions = nullptr;
  result.num_fields = 0;
  result.num_lines = 0;
}

} // namespace gpu
} // namespace libvroom
```

**Step 3: Verify CUDA compilation**

```bash
source ~/.zshenv.local
cmake -B build-gpu -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=ON -DBUILD_TESTING=ON
cmake --build build-gpu -j$(nproc) --target vroom_gpu
```

Expected: `vroom_gpu` static library builds successfully.

**Step 4: Commit**

```bash
git add cuda/csv_gpu.cuh cuda/csv_gpu.cu
git commit -m "feat(gpu): Add CUDA kernels for field boundary detection with CUB prefix XOR"
```

---

## Task 3: GPU Parser C++ Wrapper

**Files:**
- Create: `include/libvroom/gpu_parser.h` (public header with stubs when GPU disabled)
- Create: `cuda/gpu_parser.cpp` (implementation calling CUDA kernels)

**Step 1: Create include/libvroom/gpu_parser.h**

```cpp
#ifndef LIBVROOM_GPU_PARSER_H
#define LIBVROOM_GPU_PARSER_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace libvroom {
namespace gpu {

#ifdef LIBVROOM_ENABLE_GPU

// Forward declarations (full definitions in csv_gpu.cuh)
struct GpuParseConfig;
struct GpuTimings;
struct GpuInfo;

// Query GPU capabilities
GpuInfo query_gpu_info();

// Check if GPU should be used for given data size
bool should_use_gpu(size_t data_size, const GpuInfo& info);

// High-level GPU CSV index builder
// Scans CSV data on GPU, returns sorted field boundary positions
class GpuCsvIndex {
public:
  // Parse CSV data on GPU. Returns false on error.
  bool build(const char* data, size_t len,
             char delimiter = ',', char quote_char = '"',
             bool handle_quotes = true);

  bool is_valid() const { return valid_; }
  const std::string& error() const { return error_; }

  uint64_t num_fields() const { return num_fields_; }
  uint64_t num_lines() const { return num_lines_; }
  const std::vector<uint64_t>& positions() const { return positions_; }

  // Timing breakdown (milliseconds)
  float h2d_ms() const { return h2d_ms_; }
  float kernel_ms() const { return kernel_ms_; }
  float d2h_ms() const { return d2h_ms_; }
  float total_ms() const { return total_ms_; }

private:
  bool valid_ = false;
  std::string error_;
  uint64_t num_fields_ = 0;
  uint64_t num_lines_ = 0;
  std::vector<uint64_t> positions_;
  float h2d_ms_ = 0, kernel_ms_ = 0, d2h_ms_ = 0, total_ms_ = 0;
};

// Utility
std::string gpu_info_string();
bool cuda_available();

#else // GPU not enabled

inline bool cuda_available() { return false; }
inline std::string gpu_info_string() {
  return "GPU support not compiled. Build with -DENABLE_GPU=ON";
}

#endif // LIBVROOM_ENABLE_GPU

} // namespace gpu
} // namespace libvroom

#endif // LIBVROOM_GPU_PARSER_H
```

**Step 2: Create cuda/gpu_parser.cpp**

```cpp
#include "libvroom/gpu_parser.h"
#include "csv_gpu.cuh"

#include <algorithm>
#include <sstream>

namespace libvroom {
namespace gpu {

bool GpuCsvIndex::build(const char* data, size_t len,
                        char delimiter, char quote_char,
                        bool handle_quotes) {
  valid_ = false;
  error_.clear();
  positions_.clear();
  num_fields_ = 0;
  num_lines_ = 0;

  GpuParseConfig config;
  config.delimiter = delimiter;
  config.quote_char = quote_char;
  config.handle_quotes = handle_quotes;

  // Phase 1: count boundaries
  auto count_result = gpu_find_field_boundaries(data, len, config);
  if (!count_result.success) {
    error_ = count_result.error_message ? count_result.error_message : "GPU parsing failed";
    return false;
  }

  num_fields_ = count_result.num_fields;
  num_lines_ = count_result.num_lines;

  if (num_fields_ == 0) {
    valid_ = true;
    return true;
  }

  // Phase 2: allocate and get positions
  positions_.resize(num_fields_);
  auto result = gpu_find_field_boundaries(
      data, len, config,
      positions_.data(), positions_.size());

  if (!result.success) {
    error_ = result.error_message ? result.error_message : "GPU position extraction failed";
    return false;
  }

  // Sort positions (GPU atomic writes are unordered)
  std::sort(positions_.begin(), positions_.end());

  // Store timings from the position-extraction pass
  h2d_ms_ = result.timings.h2d_ms;
  kernel_ms_ = result.timings.kernel_ms;
  d2h_ms_ = result.timings.d2h_ms;
  total_ms_ = result.timings.total_ms;

  valid_ = true;
  return true;
}

std::string gpu_info_string() {
  auto info = query_gpu_info();
  if (!info.available) {
    return "No CUDA GPU available";
  }
  std::ostringstream ss;
  ss << info.device_name
     << " (compute " << info.compute_major << "." << info.compute_minor
     << ", " << (info.total_memory / (1024 * 1024)) << " MB"
     << ", " << info.sm_count << " SMs)";
  return ss.str();
}

bool cuda_available() {
  auto info = query_gpu_info();
  return info.available;
}

} // namespace gpu
} // namespace libvroom
```

**Step 3: Build and verify compilation**

```bash
source ~/.zshenv.local
cmake -B build-gpu -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=ON -DBUILD_TESTING=ON
cmake --build build-gpu -j$(nproc) --target vroom_gpu
```

**Step 4: Commit**

```bash
git add include/libvroom/gpu_parser.h cuda/gpu_parser.cpp
git commit -m "feat(gpu): Add GpuCsvIndex C++ wrapper for GPU field boundary detection"
```

---

## Task 4: GPU Parser Tests

**Files:**
- Create: `cuda/gpu_parser_test.cpp`

**Step 1: Write failing tests**

```cpp
#include "libvroom/gpu_parser.h"
#include <gtest/gtest.h>

#include <string>

using namespace libvroom::gpu;

class GpuParserTest : public ::testing::Test {
protected:
  void SetUp() override {
    if (!cuda_available()) {
      GTEST_SKIP() << "No CUDA GPU available";
    }
  }
};

TEST_F(GpuParserTest, GpuInfoAvailable) {
  auto info_str = gpu_info_string();
  EXPECT_FALSE(info_str.empty());
  EXPECT_NE(info_str.find("compute"), std::string::npos);
}

TEST_F(GpuParserTest, SimpleCSV) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n";

  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build(csv.data(), csv.size()));
  EXPECT_TRUE(idx.is_valid());
  EXPECT_EQ(idx.num_lines(), 3u); // 3 newlines
  // 6 commas + 3 newlines = 9 boundaries
  EXPECT_EQ(idx.num_fields(), 9u);
}

TEST_F(GpuParserTest, QuotedFields) {
  std::string csv = "a,\"b,c\",d\n1,\"2,3\",4\n";

  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build(csv.data(), csv.size()));
  EXPECT_TRUE(idx.is_valid());
  EXPECT_EQ(idx.num_lines(), 2u);
  // Commas inside quotes should NOT count as boundaries:
  // Row 1: a | "b,c" | d  => 2 delim commas + 1 newline = 3
  // Row 2: 1 | "2,3" | 4  => 2 delim commas + 1 newline = 3
  // Total: 6
  EXPECT_EQ(idx.num_fields(), 6u);
}

TEST_F(GpuParserTest, NoQuoteMode) {
  std::string csv = "a,\"b,c\",d\n";

  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build(csv.data(), csv.size(), ',', '"', false));
  // Without quote handling, all commas count:
  // a | "b | c" | d + newline = 4
  EXPECT_EQ(idx.num_fields(), 4u);
}

TEST_F(GpuParserTest, SortedPositions) {
  std::string csv = "a,b,c\n1,2,3\n";

  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build(csv.data(), csv.size()));

  const auto& pos = idx.positions();
  ASSERT_EQ(pos.size(), idx.num_fields());

  // Verify sorted
  for (size_t i = 1; i < pos.size(); i++) {
    EXPECT_LT(pos[i - 1], pos[i]) << "Positions must be sorted";
  }

  // Verify positions point to actual delimiters/newlines
  for (auto p : pos) {
    ASSERT_LT(p, csv.size());
    char c = csv[p];
    EXPECT_TRUE(c == ',' || c == '\n')
        << "Position " << p << " points to '" << c << "', expected delimiter or newline";
  }
}

TEST_F(GpuParserTest, EmptyInput) {
  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build("", 0));
  EXPECT_EQ(idx.num_fields(), 0u);
  EXPECT_EQ(idx.num_lines(), 0u);
}

TEST_F(GpuParserTest, TimingsPopulated) {
  // Generate ~1MB of data for meaningful timing
  std::string csv;
  csv.reserve(1024 * 1024);
  csv += "a,b,c\n";
  while (csv.size() < 1024 * 1024) {
    csv += "1,2,3\n";
  }

  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build(csv.data(), csv.size()));
  EXPECT_GT(idx.total_ms(), 0.0f);
  EXPECT_GE(idx.h2d_ms(), 0.0f);
  EXPECT_GE(idx.kernel_ms(), 0.0f);
  EXPECT_GE(idx.d2h_ms(), 0.0f);
}

TEST_F(GpuParserTest, LargeFile) {
  // Generate ~10MB for a more realistic test
  std::string csv;
  csv.reserve(10 * 1024 * 1024);
  csv += "col1,col2,col3,col4,col5\n";
  while (csv.size() < 10 * 1024 * 1024) {
    csv += "12345,67890,abcde,fghij,klmno\n";
  }

  GpuCsvIndex idx;
  ASSERT_TRUE(idx.build(csv.data(), csv.size()));
  EXPECT_TRUE(idx.is_valid());
  EXPECT_GT(idx.num_lines(), 0u);
  EXPECT_GT(idx.num_fields(), 0u);

  // Each row has 4 commas + 1 newline = 5 boundaries
  // So num_fields should be ~5 * num_lines
  EXPECT_NEAR(static_cast<double>(idx.num_fields()),
              static_cast<double>(idx.num_lines()) * 5.0, 10.0);
}
```

**Step 2: Build and run tests**

```bash
source ~/.zshenv.local
cmake --build build-gpu -j$(nproc)
./build-gpu/cuda/gpu_parser_test
```

Expected: Tests should fail initially (implementation bugs), then pass after fixes.

**Step 3: Debug and fix any failures**

Iterate until all tests pass.

**Step 4: Commit**

```bash
git add cuda/gpu_parser_test.cpp
git commit -m "test(gpu): Add GPU parser correctness tests"
```

---

## Task 5: Benchmark Harness

**Files:**
- Create: `cuda/gpu_benchmark.cpp`

This benchmarks all three approaches: Full GPU, Hybrid CPU+GPU, CPU-only baseline.

**Step 1: Write benchmark**

```cpp
#include "libvroom/gpu_parser.h"
#include "csv_gpu.cuh"
#include "libvroom/vroom.h"

#include <benchmark/benchmark.h>

#include <algorithm>
#include <random>
#include <string>

using namespace libvroom;

// ---------------------------------------------------------------------------
// Test data generation
// ---------------------------------------------------------------------------
static std::string generate_numeric_csv(size_t target_bytes, int num_cols = 10) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, 99999);

  std::string csv;
  csv.reserve(target_bytes + 4096);

  // Header
  for (int c = 0; c < num_cols; c++) {
    if (c > 0) csv += ',';
    csv += "col_" + std::to_string(c);
  }
  csv += '\n';

  // Data rows
  while (csv.size() < target_bytes) {
    for (int c = 0; c < num_cols; c++) {
      if (c > 0) csv += ',';
      csv += std::to_string(dist(rng));
    }
    csv += '\n';
  }
  return csv;
}

static std::string generate_quoted_csv(size_t target_bytes, int num_cols = 10) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> dist(0, 99999);

  std::string csv;
  csv.reserve(target_bytes + 4096);

  for (int c = 0; c < num_cols; c++) {
    if (c > 0) csv += ',';
    csv += "col_" + std::to_string(c);
  }
  csv += '\n';

  int row = 0;
  while (csv.size() < target_bytes) {
    for (int c = 0; c < num_cols; c++) {
      if (c > 0) csv += ',';
      // Every 3rd field is quoted, some with embedded commas
      if (c % 3 == 0) {
        csv += "\"value_" + std::to_string(dist(rng));
        if (row % 5 == 0) csv += ",embedded";
        csv += "\"";
      } else {
        csv += std::to_string(dist(rng));
      }
    }
    csv += '\n';
    row++;
  }
  return csv;
}

// Cache generated data across benchmark iterations
struct CsvCache {
  std::string data;
  size_t actual_size;
};

static CsvCache& get_numeric_csv(size_t target_bytes) {
  static std::map<size_t, CsvCache> cache;
  auto it = cache.find(target_bytes);
  if (it == cache.end()) {
    auto csv = generate_numeric_csv(target_bytes);
    cache[target_bytes] = {std::move(csv), target_bytes};
  }
  return cache[target_bytes];
}

static CsvCache& get_quoted_csv(size_t target_bytes) {
  static std::map<size_t, CsvCache> cache;
  auto it = cache.find(target_bytes);
  if (it == cache.end()) {
    auto csv = generate_quoted_csv(target_bytes);
    cache[target_bytes] = {std::move(csv), target_bytes};
  }
  return cache[target_bytes];
}

// ---------------------------------------------------------------------------
// Approach A: Full GPU pipeline
// ---------------------------------------------------------------------------
static void BM_GPU_Full_Numeric(benchmark::State& state) {
  size_t target_size = state.range(0);
  auto& csv = get_numeric_csv(target_size);

  for (auto _ : state) {
    gpu::GpuCsvIndex idx;
    bool ok = idx.build(csv.data.data(), csv.data.size());
    if (!ok) {
      state.SkipWithError(idx.error().c_str());
      return;
    }
    benchmark::DoNotOptimize(idx.num_fields());
  }

  state.SetBytesProcessed(state.iterations() * csv.data.size());
  state.SetLabel(std::to_string(csv.data.size() / (1024 * 1024)) + " MB");
}

static void BM_GPU_Full_Quoted(benchmark::State& state) {
  size_t target_size = state.range(0);
  auto& csv = get_quoted_csv(target_size);

  for (auto _ : state) {
    gpu::GpuCsvIndex idx;
    bool ok = idx.build(csv.data.data(), csv.data.size());
    if (!ok) {
      state.SkipWithError(idx.error().c_str());
      return;
    }
    benchmark::DoNotOptimize(idx.num_fields());
  }

  state.SetBytesProcessed(state.iterations() * csv.data.size());
  state.SetLabel(std::to_string(csv.data.size() / (1024 * 1024)) + " MB");
}

// ---------------------------------------------------------------------------
// Approach B: Hybrid CPU+GPU (CPU chunk finding, GPU field detection)
// ---------------------------------------------------------------------------
static void BM_Hybrid_Numeric(benchmark::State& state) {
  size_t target_size = state.range(0);
  auto& csv = get_numeric_csv(target_size);

  CsvOptions options;
  options.separator = ',';
  options.quote = '"';

  for (auto _ : state) {
    // Phase 1: CPU chunk finding (reuse existing infrastructure)
    ChunkFinder finder(options.separator, options.quote);
    auto [row_count, last_row_end] = finder.count_rows(csv.data.data(), csv.data.size());

    // Phase 2: GPU field boundary detection
    gpu::GpuCsvIndex idx;
    bool ok = idx.build(csv.data.data(), csv.data.size());
    if (!ok) {
      state.SkipWithError(idx.error().c_str());
      return;
    }
    benchmark::DoNotOptimize(idx.num_fields());
    benchmark::DoNotOptimize(row_count);
  }

  state.SetBytesProcessed(state.iterations() * csv.data.size());
  state.SetLabel(std::to_string(csv.data.size() / (1024 * 1024)) + " MB");
}

static void BM_Hybrid_Quoted(benchmark::State& state) {
  size_t target_size = state.range(0);
  auto& csv = get_quoted_csv(target_size);

  CsvOptions options;
  options.separator = ',';
  options.quote = '"';

  for (auto _ : state) {
    ChunkFinder finder(options.separator, options.quote);
    auto [row_count, last_row_end] = finder.count_rows(csv.data.data(), csv.data.size());

    gpu::GpuCsvIndex idx;
    bool ok = idx.build(csv.data.data(), csv.data.size());
    if (!ok) {
      state.SkipWithError(idx.error().c_str());
      return;
    }
    benchmark::DoNotOptimize(idx.num_fields());
    benchmark::DoNotOptimize(row_count);
  }

  state.SetBytesProcessed(state.iterations() * csv.data.size());
  state.SetLabel(std::to_string(csv.data.size() / (1024 * 1024)) + " MB");
}

// ---------------------------------------------------------------------------
// Approach C: CPU baseline (full CsvReader pipeline)
// ---------------------------------------------------------------------------
static void BM_CPU_Baseline_Numeric(benchmark::State& state) {
  size_t target_size = state.range(0);
  auto& csv = get_numeric_csv(target_size);

  for (auto _ : state) {
    // Use CsvReader for full CPU pipeline
    CsvOptions options;
    options.separator = ',';
    options.quote = '"';
    options.has_header = true;

    CsvReader reader(options);
    auto open_result = reader.open_from_buffer(
        AlignedBuffer(csv.data.data(), csv.data.size()));
    if (!open_result) {
      state.SkipWithError(open_result.error.c_str());
      return;
    }
    auto result = reader.read_all();
    if (!result) {
      state.SkipWithError(result.error.c_str());
      return;
    }
    benchmark::DoNotOptimize(result.value.total_rows);
  }

  state.SetBytesProcessed(state.iterations() * csv.data.size());
  state.SetLabel(std::to_string(csv.data.size() / (1024 * 1024)) + " MB");
}

static void BM_CPU_Baseline_Quoted(benchmark::State& state) {
  size_t target_size = state.range(0);
  auto& csv = get_quoted_csv(target_size);

  for (auto _ : state) {
    CsvOptions options;
    options.separator = ',';
    options.quote = '"';
    options.has_header = true;

    CsvReader reader(options);
    auto open_result = reader.open_from_buffer(
        AlignedBuffer(csv.data.data(), csv.data.size()));
    if (!open_result) {
      state.SkipWithError(open_result.error.c_str());
      return;
    }
    auto result = reader.read_all();
    if (!result) {
      state.SkipWithError(result.error.c_str());
      return;
    }
    benchmark::DoNotOptimize(result.value.total_rows);
  }

  state.SetBytesProcessed(state.iterations() * csv.data.size());
  state.SetLabel(std::to_string(csv.data.size() / (1024 * 1024)) + " MB");
}

// ---------------------------------------------------------------------------
// Register benchmarks at various sizes
// ---------------------------------------------------------------------------
#define SIZES \
  Arg(1 * 1024 * 1024) \
  ->Arg(10 * 1024 * 1024) \
  ->Arg(50 * 1024 * 1024) \
  ->Arg(100 * 1024 * 1024) \
  ->Arg(500 * 1024 * 1024)

// GPU Full
BENCHMARK(BM_GPU_Full_Numeric)->SIZES->Unit(benchmark::kMillisecond);
BENCHMARK(BM_GPU_Full_Quoted)->SIZES->Unit(benchmark::kMillisecond);

// Hybrid
BENCHMARK(BM_Hybrid_Numeric)->SIZES->Unit(benchmark::kMillisecond);
BENCHMARK(BM_Hybrid_Quoted)->SIZES->Unit(benchmark::kMillisecond);

// CPU Baseline
BENCHMARK(BM_CPU_Baseline_Numeric)->SIZES->Unit(benchmark::kMillisecond);
BENCHMARK(BM_CPU_Baseline_Quoted)->SIZES->Unit(benchmark::kMillisecond);

#undef SIZES
```

**Note:** The benchmark code above references `CsvReader::open_from_buffer` and related APIs. These may need adjustment based on the exact API available on main. Check `include/libvroom/vroom.h` for the actual `CsvReader` and `AlignedBuffer` signatures during implementation.

**Step 2: Build and run benchmarks**

```bash
source ~/.zshenv.local
cmake --build build-gpu -j$(nproc)
./build-gpu/cuda/gpu_benchmark --benchmark_format=console
```

**Step 3: Capture results to a file**

```bash
./build-gpu/cuda/gpu_benchmark --benchmark_format=json --benchmark_out=gpu_benchmark_results.json
```

**Step 4: Commit**

```bash
git add cuda/gpu_benchmark.cpp
git commit -m "bench(gpu): Add GPU vs CPU benchmark harness at 1MB-500MB sizes"
```

---

## Task 6: Run Benchmarks and Analyze Results

**Step 1: Run the full benchmark suite**

```bash
source ~/.zshenv.local
./build-gpu/cuda/gpu_benchmark --benchmark_format=console 2>&1 | tee gpu_benchmark_output.txt
```

**Step 2: Analyze results**

Look at:
- **Crossover point**: At what file size does GPU beat CPU?
- **Peak throughput**: GB/s for each approach at 100MB and 500MB
- **Quote overhead**: How much does quote handling cost on GPU vs CPU?
- **Phase breakdown**: H2D transfer vs kernel vs D2H — what dominates?

**Step 3: Write results into PR description**

Format as a table:

```markdown
| File Size | GPU Full (GB/s) | Hybrid (GB/s) | CPU (GB/s) | GPU Speedup |
|-----------|-----------------|---------------|------------|-------------|
| 1 MB      | ...             | ...           | ...        | ...         |
| 10 MB     | ...             | ...           | ...        | ...         |
| 50 MB     | ...             | ...           | ...        | ...         |
| 100 MB    | ...             | ...           | ...        | ...         |
| 500 MB    | ...             | ...           | ...        | ...         |
```

---

## Task 7: Verify, Review, and Create PR

**Step 1: Run all existing tests (ensure no breakage)**

```bash
source ~/.zshenv.local
# Build without GPU — verify existing tests still pass
cmake -B build-check -DCMAKE_BUILD_TYPE=Release -DENABLE_GPU=OFF -DBUILD_TESTING=ON
cmake --build build-check -j$(nproc)
cd build-check && ctest --output-on-failure -j$(nproc)
```

**Step 2: Run GPU tests**

```bash
cd build-gpu && ctest --output-on-failure -j$(nproc)
```

**Step 3: Self-review with superpowers:verification-before-completion**

**Step 4: Create PR**

```bash
gh pr create \
  --repo jimhester/libvroom \
  --title "feat(gpu): GPU-accelerated CSV parsing prototype with benchmarks" \
  --body "$(cat <<'EOF'
## Summary

Adds an experimental CUDA GPU-accelerated CSV parsing prototype for libvroom.
Benchmarks three approaches (full GPU, hybrid CPU+GPU, CPU baseline) at various
file sizes to determine if GPU acceleration is worthwhile.

Addresses #438

## Key Changes

- **CUDA kernels** (`cuda/csv_gpu.cu`): Field boundary detection using CUB
  prefix XOR scan for quote parity tracking
- **C++ wrapper** (`include/libvroom/gpu_parser.h`): `GpuCsvIndex` class
- **Benchmark suite** (`cuda/gpu_benchmark.cpp`): Fair comparison at 1MB-500MB
- **CMake integration**: Optional `-DENABLE_GPU=ON`, disabled by default

## Benchmark Results

[INSERT RESULTS TABLE]

## Test plan

- [ ] GPU tests pass on CUDA-capable system
- [ ] Existing tests pass with `-DENABLE_GPU=OFF`
- [ ] Benchmark results collected and analyzed
EOF
)"
```

---

## File Summary

| File | Action | Purpose |
|------|--------|---------|
| `CMakeLists.txt` | Modify | Add `ENABLE_GPU` option and `cuda/` subdirectory |
| `cuda/CMakeLists.txt` | Create | CUDA build rules for library, tests, benchmarks |
| `cuda/csv_gpu.cuh` | Create | Data structures and kernel declarations |
| `cuda/csv_gpu.cu` | Create | CUDA kernels: quote flags, prefix XOR, boundary detection |
| `include/libvroom/gpu_parser.h` | Create | Public API header (with stubs when GPU disabled) |
| `cuda/gpu_parser.cpp` | Create | GpuCsvIndex implementation wrapping CUDA kernels |
| `cuda/gpu_parser_test.cpp` | Create | Correctness tests for GPU parser |
| `cuda/gpu_benchmark.cpp` | Create | Benchmark harness: GPU full, hybrid, CPU baseline |
