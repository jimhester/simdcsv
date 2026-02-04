#include "csv_gpu.cuh"

#include <algorithm>
#include <cstdio>
#include <cstring>
#include <cuda_runtime.h>
#include <cub/cub.cuh>

namespace libvroom {
namespace gpu {

// =============================================================================
// Error checking macro (goto-based cleanup)
// =============================================================================

#define CUDA_CHECK(call)                                                         \
  do {                                                                           \
    cudaError_t err_ = (call);                                                   \
    if (err_ != cudaSuccess) {                                                   \
      fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__,           \
              cudaGetErrorString(err_));                                          \
      result.error_message = cudaGetErrorString(err_);                           \
      goto cleanup;                                                              \
    }                                                                            \
  } while (0)

// =============================================================================
// Kernel 1: Build per-byte quote flags (1 = quote char, 0 = other)
// =============================================================================

__global__ void build_quote_flags_kernel(
    const char* __restrict__ data,
    size_t len,
    char quote_char,
    uint8_t* __restrict__ flags) {
  const size_t tid = blockIdx.x * blockDim.x + threadIdx.x;
  const size_t stride = blockDim.x * gridDim.x;

  for (size_t i = tid; i < len; i += stride) {
    flags[i] = (data[i] == quote_char) ? 1 : 0;
  }
}

// =============================================================================
// Kernel 2: Find all boundaries (delimiters + newlines) outside quotes
// quote_state[i] is the prefix-XOR of quote flags; odd = inside quotes.
// =============================================================================

__global__ void find_all_boundaries_kernel(
    const char* __restrict__ data,
    size_t len,
    char delimiter,
    const uint8_t* __restrict__ quote_state,
    uint32_t* __restrict__ positions,
    uint32_t* __restrict__ count) {
  const size_t tid = blockIdx.x * blockDim.x + threadIdx.x;
  const size_t stride = blockDim.x * gridDim.x;

  for (size_t i = tid; i < len; i += stride) {
    bool outside = (quote_state[i] & 1) == 0;
    char ch = data[i];
    if (outside && (ch == delimiter || ch == '\n')) {
      uint32_t idx = atomicAdd(count, 1);
      if (positions) {
        positions[idx] = static_cast<uint32_t>(i);
      }
    }
  }
}

// =============================================================================
// Kernel 3: Count newlines outside quotes (warp-level reduction)
// =============================================================================

__global__ void count_lines_kernel(
    const char* __restrict__ data,
    size_t len,
    const uint8_t* __restrict__ quote_state,
    uint32_t* __restrict__ line_count) {
  const size_t tid = blockIdx.x * blockDim.x + threadIdx.x;
  const size_t stride = blockDim.x * gridDim.x;

  uint32_t local = 0;
  for (size_t i = tid; i < len; i += stride) {
    if (data[i] == '\n' && (quote_state[i] & 1) == 0) {
      local++;
    }
  }

  for (int offset = 16; offset > 0; offset /= 2) {
    local += __shfl_down_sync(0xffffffff, local, offset);
  }

  if ((threadIdx.x & 31) == 0) {
    atomicAdd(line_count, local);
  }
}

// =============================================================================
// Custom XOR functor for CUB inclusive scan
// =============================================================================

struct XorOp {
  __device__ __forceinline__ uint8_t operator()(uint8_t a, uint8_t b) const {
    return a ^ b;
  }
};

// =============================================================================
// Host-callable helpers
// =============================================================================

GpuInfo query_gpu_info() {
  GpuInfo info;

  int device_count = 0;
  cudaError_t err = cudaGetDeviceCount(&device_count);
  if (err != cudaSuccess || device_count == 0) {
    info.cuda_available = false;
    return info;
  }

  info.cuda_available = true;
  info.device_count = device_count;

  cudaDeviceProp prop;
  err = cudaGetDeviceProperties(&prop, 0);
  if (err == cudaSuccess) {
    strncpy(info.device_name, prop.name, sizeof(info.device_name) - 1);
    info.total_memory = prop.totalGlobalMem;
    info.compute_capability_major = prop.major;
    info.compute_capability_minor = prop.minor;
    info.max_threads_per_block = prop.maxThreadsPerBlock;
    info.max_blocks_per_sm = prop.maxBlocksPerMultiProcessor;
    info.sm_count = prop.multiProcessorCount;
  }

  size_t free_mem = 0, total_mem = 0;
  if (cudaMemGetInfo(&free_mem, &total_mem) == cudaSuccess) {
    info.free_memory = free_mem;
  }

  return info;
}

bool should_use_gpu(size_t data_size, const GpuInfo& info) {
  if (!info.cuda_available) return false;
  constexpr size_t MIN_SIZE_FOR_GPU = 10 * 1024 * 1024;
  if (data_size < MIN_SIZE_FOR_GPU) return false;
  if (data_size * 3 > info.free_memory) return false;
  return true;
}

// =============================================================================
// Main entry point: gpu_find_field_boundaries
// Two-phase: count-only pass, then allocate and write pass.
// =============================================================================

GpuIndexResult gpu_find_field_boundaries(
    const char* data,
    size_t len,
    const GpuParseConfig& config,
    GpuTimings* timings) {

  GpuIndexResult result;
  result.success = false;

  if (len == 0) {
    result.success = true;
    result.count = 0;
    result.num_lines = 0;
    return result;
  }

  // Positions are stored as uint32_t, so we can't handle files > 4 GB
  if (len > UINT32_MAX) {
    result.error_message = "File too large for GPU parsing (>4 GB)";
    return result;
  }

  // Device pointers (all nullptr for safe cleanup)
  char*     d_data         = nullptr;
  uint8_t*  d_quote_flags  = nullptr;
  uint8_t*  d_quote_state  = nullptr;
  void*     d_cub_temp     = nullptr;
  uint32_t* d_count        = nullptr;
  uint32_t* d_line_count   = nullptr;
  uint32_t* d_positions    = nullptr;

  int block_size = config.block_size;
  int num_blocks = std::min(config.max_blocks,
                            static_cast<int>((len + block_size - 1) / block_size));

  cudaEvent_t ev_start = nullptr, ev_h2d = nullptr, ev_kernel = nullptr, ev_d2h = nullptr;

  if (timings) {
    cudaEventCreate(&ev_start);
    cudaEventCreate(&ev_h2d);
    cudaEventCreate(&ev_kernel);
    cudaEventCreate(&ev_d2h);
    cudaEventRecord(ev_start);
  }

  // H2D transfer
  CUDA_CHECK(cudaMalloc(&d_data, len));
  CUDA_CHECK(cudaMemcpy(d_data, data, len, cudaMemcpyHostToDevice));

  if (timings) {
    cudaEventRecord(ev_h2d);
  }

  // Quote state computation
  CUDA_CHECK(cudaMalloc(&d_quote_flags, len));
  CUDA_CHECK(cudaMalloc(&d_quote_state, len));

  if (config.handle_quotes) {
    build_quote_flags_kernel<<<num_blocks, block_size>>>(
        d_data, len, config.quote_char, d_quote_flags);
    CUDA_CHECK(cudaGetLastError());

    size_t cub_temp_bytes = 0;
    cub::DeviceScan::InclusiveScan(
        nullptr, cub_temp_bytes, d_quote_flags, d_quote_state, XorOp(), len);
    CUDA_CHECK(cudaMalloc(&d_cub_temp, cub_temp_bytes));
    cub::DeviceScan::InclusiveScan(
        d_cub_temp, cub_temp_bytes, d_quote_flags, d_quote_state, XorOp(), len);
    CUDA_CHECK(cudaGetLastError());
  } else {
    CUDA_CHECK(cudaMemset(d_quote_state, 0, len));
  }

  // Count-only pass
  CUDA_CHECK(cudaMalloc(&d_count, sizeof(uint32_t)));
  CUDA_CHECK(cudaMemset(d_count, 0, sizeof(uint32_t)));

  find_all_boundaries_kernel<<<num_blocks, block_size>>>(
      d_data, len, config.delimiter, d_quote_state, nullptr, d_count);
  CUDA_CHECK(cudaGetLastError());

  {
    uint32_t h_count = 0;
    CUDA_CHECK(cudaMemcpy(&h_count, d_count, sizeof(uint32_t), cudaMemcpyDeviceToHost));
    result.count = h_count;
  }

  // Write pass
  if (result.count > 0) {
    CUDA_CHECK(cudaMalloc(&d_positions, result.count * sizeof(uint32_t)));
    CUDA_CHECK(cudaMemset(d_count, 0, sizeof(uint32_t)));

    find_all_boundaries_kernel<<<num_blocks, block_size>>>(
        d_data, len, config.delimiter, d_quote_state, d_positions, d_count);
    CUDA_CHECK(cudaGetLastError());
  }

  // Count newlines
  CUDA_CHECK(cudaMalloc(&d_line_count, sizeof(uint32_t)));
  CUDA_CHECK(cudaMemset(d_line_count, 0, sizeof(uint32_t)));

  count_lines_kernel<<<num_blocks, block_size>>>(
      d_data, len, d_quote_state, d_line_count);
  CUDA_CHECK(cudaGetLastError());

  {
    uint32_t h_lines = 0;
    CUDA_CHECK(cudaMemcpy(&h_lines, d_line_count, sizeof(uint32_t), cudaMemcpyDeviceToHost));
    result.num_lines = h_lines;
  }

  // Copy positions to host and sort
  if (result.count > 0) {
    result.positions = new uint32_t[result.count];
    CUDA_CHECK(cudaMemcpy(result.positions, d_positions,
                           result.count * sizeof(uint32_t), cudaMemcpyDeviceToHost));
    std::sort(result.positions, result.positions + result.count);
  }

  if (timings) {
    cudaEventRecord(ev_kernel);
    cudaEventRecord(ev_d2h);
    cudaEventSynchronize(ev_d2h);

    cudaEventElapsedTime(&timings->h2d_transfer_ms, ev_start, ev_h2d);
    cudaEventElapsedTime(&timings->kernel_exec_ms, ev_h2d, ev_kernel);
    cudaEventElapsedTime(&timings->d2h_transfer_ms, ev_kernel, ev_d2h);
    cudaEventElapsedTime(&timings->total_ms, ev_start, ev_d2h);
  }

  result.success = true;

cleanup:
  // On error, free any host-allocated positions to prevent memory leak
  if (!result.success && result.positions) {
    delete[] result.positions;
    result.positions = nullptr;
    result.count = 0;
  }

  if (d_data)        cudaFree(d_data);
  if (d_quote_flags) cudaFree(d_quote_flags);
  if (d_quote_state) cudaFree(d_quote_state);
  if (d_cub_temp)    cudaFree(d_cub_temp);
  if (d_count)       cudaFree(d_count);
  if (d_line_count)  cudaFree(d_line_count);
  if (d_positions)   cudaFree(d_positions);

  if (ev_start)  cudaEventDestroy(ev_start);
  if (ev_h2d)    cudaEventDestroy(ev_h2d);
  if (ev_kernel) cudaEventDestroy(ev_kernel);
  if (ev_d2h)    cudaEventDestroy(ev_d2h);

  return result;
}

void gpu_cleanup(GpuIndexResult& result) {
  delete[] result.positions;
  result.positions = nullptr;
  result.count = 0;
  result.num_lines = 0;
}

}  // namespace gpu
}  // namespace libvroom
