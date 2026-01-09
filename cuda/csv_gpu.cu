#include "csv_gpu.cuh"
#include <cuda_runtime.h>
#include <cub/cub.cuh>
#include <cstdio>

namespace vroom {
namespace gpu {

// =============================================================================
// Error checking macro
// =============================================================================
#define CUDA_CHECK(call)                                                       \
    do {                                                                       \
        cudaError_t err = call;                                                \
        if (err != cudaSuccess) {                                              \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__,   \
                    cudaGetErrorString(err));                                  \
            return;                                                            \
        }                                                                      \
    } while (0)

#define CUDA_CHECK_RETURN(call, ret)                                           \
    do {                                                                       \
        cudaError_t err = call;                                                \
        if (err != cudaSuccess) {                                              \
            fprintf(stderr, "CUDA error at %s:%d: %s\n", __FILE__, __LINE__,   \
                    cudaGetErrorString(err));                                  \
            return ret;                                                        \
        }                                                                      \
    } while (0)

// =============================================================================
// Phase 1: Newline Detection Kernel
// =============================================================================

// Each thread processes multiple bytes for better memory coalescing
// Uses warp-level reduction and atomic operations for counting
__global__ void find_newlines_kernel(
    const char* __restrict__ data,
    size_t len,
    uint32_t* __restrict__ newline_positions,
    uint32_t* __restrict__ count
) {
    // Shared memory for warp-level reduction
    __shared__ uint32_t warp_counts[32];  // One per warp (max 32 warps per block)
    __shared__ uint32_t block_offset;

    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;
    const uint32_t warp_id = threadIdx.x / 32;
    const uint32_t lane_id = threadIdx.x % 32;

    // Initialize warp counts
    if (lane_id == 0) {
        warp_counts[warp_id] = 0;
    }
    __syncthreads();

    // First pass: count newlines per thread
    uint32_t local_count = 0;
    for (size_t i = tid; i < len; i += stride) {
        if (data[i] == '\n') {
            local_count++;
        }
    }

    // Warp-level reduction using shuffle
    for (int offset = 16; offset > 0; offset /= 2) {
        local_count += __shfl_down_sync(0xffffffff, local_count, offset);
    }

    // First thread in each warp stores the warp total
    if (lane_id == 0) {
        warp_counts[warp_id] = local_count;
    }
    __syncthreads();

    // Block-level reduction (first warp only)
    if (warp_id == 0) {
        uint32_t block_count = (lane_id < blockDim.x / 32) ? warp_counts[lane_id] : 0;
        for (int offset = 16; offset > 0; offset /= 2) {
            block_count += __shfl_down_sync(0xffffffff, block_count, offset);
        }

        // First thread atomically reserves space in output array
        if (lane_id == 0) {
            block_offset = atomicAdd(count, block_count);
        }
    }
    __syncthreads();

    // Second pass: write positions with known offsets
    // Use prefix sum within block to determine exact write positions
    uint32_t write_idx = block_offset;
    for (size_t i = tid; i < len; i += stride) {
        if (data[i] == '\n') {
            uint32_t pos = atomicAdd(&newline_positions[0], 1);  // Temporary: use simple atomic
            if (pos < len) {  // Bounds check
                newline_positions[pos + 1] = static_cast<uint32_t>(i);  // +1 to leave room for count
            }
        }
    }
}

// Optimized version: count first, then write (two-phase)
__global__ void count_newlines_kernel(
    const char* __restrict__ data,
    size_t len,
    uint32_t* __restrict__ count
) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;

    uint32_t local_count = 0;

    // Process 4 bytes at a time for better memory bandwidth
    // Align to 4-byte boundary
    size_t aligned_start = (tid * 4);
    for (size_t i = aligned_start; i + 3 < len; i += stride * 4) {
        // Load 4 bytes at once
        uint32_t word;
        memcpy(&word, data + i, sizeof(uint32_t));

        // Check each byte for newline (0x0A)
        if ((word & 0xFF) == '\n') local_count++;
        if (((word >> 8) & 0xFF) == '\n') local_count++;
        if (((word >> 16) & 0xFF) == '\n') local_count++;
        if (((word >> 24) & 0xFF) == '\n') local_count++;
    }

    // Handle remaining bytes
    size_t remainder_start = (len / 4) * 4;
    for (size_t i = remainder_start + tid; i < len; i += stride) {
        if (data[i] == '\n') local_count++;
    }

    // Warp-level reduction
    for (int offset = 16; offset > 0; offset /= 2) {
        local_count += __shfl_down_sync(0xffffffff, local_count, offset);
    }

    // First thread in each warp adds to global count
    if ((threadIdx.x % 32) == 0) {
        atomicAdd(count, local_count);
    }
}

// Write newline positions after counting
__global__ void write_newline_positions_kernel(
    const char* __restrict__ data,
    size_t len,
    uint32_t* __restrict__ positions,
    uint32_t* __restrict__ write_idx
) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;

    for (size_t i = tid; i < len; i += stride) {
        if (data[i] == '\n') {
            uint32_t idx = atomicAdd(write_idx, 1);
            positions[idx] = static_cast<uint32_t>(i);
        }
    }
}

// =============================================================================
// Phase 1b: Delimiter Detection Kernel
// =============================================================================

__global__ void find_delimiters_kernel(
    const char* __restrict__ data,
    size_t len,
    char delimiter,
    const uint32_t* __restrict__ line_starts,
    uint32_t num_lines,
    uint32_t* __restrict__ field_positions,
    uint32_t* __restrict__ field_counts
) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;

    for (size_t i = tid; i < len; i += stride) {
        if (data[i] == delimiter) {
            uint32_t idx = atomicAdd(field_counts, 1);
            field_positions[idx] = static_cast<uint32_t>(i);
        }
    }
}

// =============================================================================
// Phase 2: Quote Detection Kernel
// =============================================================================

__global__ void find_quotes_kernel(
    const char* __restrict__ data,
    size_t len,
    char quote_char,
    uint32_t* __restrict__ quote_bitmap
) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;

    // Each thread processes 32 positions to fill one uint32_t
    for (size_t base = tid * 32; base < len; base += stride * 32) {
        uint32_t bits = 0;
        for (int j = 0; j < 32 && base + j < len; j++) {
            if (data[base + j] == quote_char) {
                bits |= (1u << j);
            }
        }
        if (base / 32 < (len + 31) / 32) {
            quote_bitmap[base / 32] = bits;
        }
    }
}

// Custom XOR operator for CUB scan
struct XorOp {
    __device__ __forceinline__ uint8_t operator()(uint8_t a, uint8_t b) const {
        return a ^ b;
    }
};

// Expand quote bitmap to per-byte state and run prefix XOR scan
__global__ void expand_quote_bitmap_kernel(
    const uint32_t* __restrict__ quote_bitmap,
    size_t len,
    uint8_t* __restrict__ quote_flags  // 1 if position has quote, 0 otherwise
) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;

    for (size_t i = tid; i < len; i += stride) {
        uint32_t word_idx = i / 32;
        uint32_t bit_idx = i % 32;
        uint32_t word = quote_bitmap[word_idx];
        quote_flags[i] = (word >> bit_idx) & 1;
    }
}

// =============================================================================
// Phase 2b: Filter Delimiters by Quote State
// =============================================================================

__global__ void filter_delimiters_by_quote_state_kernel(
    const char* __restrict__ data,
    size_t len,
    char delimiter,
    const uint8_t* __restrict__ quote_state,
    uint32_t* __restrict__ filtered_positions,
    uint32_t* __restrict__ count
) {
    const uint32_t tid = blockIdx.x * blockDim.x + threadIdx.x;
    const uint32_t stride = blockDim.x * gridDim.x;

    for (size_t i = tid; i < len; i += stride) {
        // Only count delimiter if outside quotes (quote_state == 0)
        if (data[i] == delimiter && quote_state[i] == 0) {
            uint32_t idx = atomicAdd(count, 1);
            filtered_positions[idx] = static_cast<uint32_t>(i);
        }
        // Also count newlines outside quotes as field separators
        if (data[i] == '\n' && quote_state[i] == 0) {
            uint32_t idx = atomicAdd(count, 1);
            filtered_positions[idx] = static_cast<uint32_t>(i);
        }
    }
}

// =============================================================================
// Host-callable wrapper functions
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
    err = cudaGetDeviceProperties(&prop, 0);  // Use first device
    if (err == cudaSuccess) {
        strncpy(info.device_name, prop.name, sizeof(info.device_name) - 1);
        info.total_memory = prop.totalGlobalMem;
        info.compute_capability_major = prop.major;
        info.compute_capability_minor = prop.minor;
        info.max_threads_per_block = prop.maxThreadsPerBlock;
        info.max_blocks_per_sm = prop.maxBlocksPerMultiProcessor;
        info.sm_count = prop.multiProcessorCount;
    }

    size_t free_mem, total_mem;
    if (cudaMemGetInfo(&free_mem, &total_mem) == cudaSuccess) {
        info.free_memory = free_mem;
    }

    return info;
}

bool should_use_gpu(size_t data_size, const GpuInfo& info) {
    if (!info.cuda_available) {
        return false;
    }

    // Don't use GPU for small files - transfer overhead dominates
    // This threshold should be determined empirically via benchmarking
    constexpr size_t MIN_SIZE_FOR_GPU = 10 * 1024 * 1024;  // 10 MB minimum

    if (data_size < MIN_SIZE_FOR_GPU) {
        return false;
    }

    // Check if data fits in GPU memory (with some headroom for results)
    if (data_size * 3 > info.free_memory) {  // 3x for data + results + temp
        return false;
    }

    return true;
}

GpuParseResult parse_csv_gpu(
    const char* data,
    size_t len,
    const GpuParseConfig& config,
    GpuTimings* timings
) {
    GpuParseResult result;
    result.success = false;

    // Create CUDA events for timing
    cudaEvent_t start, h2d_done, kernel_done, d2h_done;
    if (timings) {
        cudaEventCreate(&start);
        cudaEventCreate(&h2d_done);
        cudaEventCreate(&kernel_done);
        cudaEventCreate(&d2h_done);
        cudaEventRecord(start);
    }

    // Allocate device memory for input data
    char* d_data = nullptr;
    cudaError_t err = cudaMalloc(&d_data, len);
    if (err != cudaSuccess) {
        result.error_message = "Failed to allocate device memory for input data";
        return result;
    }

    // Transfer data to device
    err = cudaMemcpy(d_data, data, len, cudaMemcpyHostToDevice);
    if (err != cudaSuccess) {
        cudaFree(d_data);
        result.error_message = "Failed to copy data to device";
        return result;
    }

    if (timings) {
        cudaEventRecord(h2d_done);
    }

    // Allocate counter for newlines (on device)
    uint32_t* d_newline_count = nullptr;
    err = cudaMalloc(&d_newline_count, sizeof(uint32_t));
    if (err != cudaSuccess) {
        cudaFree(d_data);
        result.error_message = "Failed to allocate newline counter";
        return result;
    }
    cudaMemset(d_newline_count, 0, sizeof(uint32_t));

    // Phase 1: Count newlines
    int block_size = config.block_size;
    int num_blocks = min(config.max_blocks, (int)((len + block_size - 1) / block_size));

    count_newlines_kernel<<<num_blocks, block_size>>>(d_data, len, d_newline_count);

    // Copy count back to host to allocate result array
    uint32_t h_newline_count = 0;
    cudaMemcpy(&h_newline_count, d_newline_count, sizeof(uint32_t), cudaMemcpyDeviceToHost);

    // Allocate array for newline positions
    err = cudaMalloc(&result.d_newline_positions, (h_newline_count + 1) * sizeof(uint32_t));
    if (err != cudaSuccess) {
        cudaFree(d_data);
        cudaFree(d_newline_count);
        result.error_message = "Failed to allocate newline positions array";
        return result;
    }

    // Reset counter and write positions
    cudaMemset(d_newline_count, 0, sizeof(uint32_t));
    write_newline_positions_kernel<<<num_blocks, block_size>>>(
        d_data, len, result.d_newline_positions, d_newline_count
    );

    result.num_lines = h_newline_count;

    // Phase 2: Handle quotes if enabled
    if (config.handle_quotes) {
        // Allocate quote bitmap
        size_t bitmap_size = (len + 31) / 32;
        uint32_t* d_quote_bitmap = nullptr;
        err = cudaMalloc(&d_quote_bitmap, bitmap_size * sizeof(uint32_t));
        if (err != cudaSuccess) {
            cudaFree(d_data);
            cudaFree(d_newline_count);
            result.error_message = "Failed to allocate quote bitmap";
            return result;
        }
        cudaMemset(d_quote_bitmap, 0, bitmap_size * sizeof(uint32_t));

        // Find quote positions
        find_quotes_kernel<<<num_blocks, block_size>>>(
            d_data, len, config.quote_char, d_quote_bitmap
        );

        // Expand bitmap to per-byte flags
        uint8_t* d_quote_flags = nullptr;
        err = cudaMalloc(&d_quote_flags, len);
        if (err != cudaSuccess) {
            cudaFree(d_data);
            cudaFree(d_newline_count);
            cudaFree(d_quote_bitmap);
            result.error_message = "Failed to allocate quote flags";
            return result;
        }

        expand_quote_bitmap_kernel<<<num_blocks, block_size>>>(
            d_quote_bitmap, len, d_quote_flags
        );

        // Run prefix XOR scan using CUB
        uint8_t* d_quote_state = nullptr;
        err = cudaMalloc(&d_quote_state, len);
        if (err != cudaSuccess) {
            cudaFree(d_data);
            cudaFree(d_newline_count);
            cudaFree(d_quote_bitmap);
            cudaFree(d_quote_flags);
            result.error_message = "Failed to allocate quote state";
            return result;
        }

        // Determine temporary storage requirements for CUB scan
        void* d_temp_storage = nullptr;
        size_t temp_storage_bytes = 0;
        cub::DeviceScan::InclusiveScan(
            d_temp_storage, temp_storage_bytes,
            d_quote_flags, d_quote_state, XorOp(), len
        );

        err = cudaMalloc(&d_temp_storage, temp_storage_bytes);
        if (err != cudaSuccess) {
            cudaFree(d_data);
            cudaFree(d_newline_count);
            cudaFree(d_quote_bitmap);
            cudaFree(d_quote_flags);
            cudaFree(d_quote_state);
            result.error_message = "Failed to allocate CUB temp storage";
            return result;
        }

        // Run the actual scan
        cub::DeviceScan::InclusiveScan(
            d_temp_storage, temp_storage_bytes,
            d_quote_flags, d_quote_state, XorOp(), len
        );

        // Find delimiters outside quotes
        uint32_t* d_field_count = nullptr;
        cudaMalloc(&d_field_count, sizeof(uint32_t));
        cudaMemset(d_field_count, 0, sizeof(uint32_t));

        // First pass: count fields
        filter_delimiters_by_quote_state_kernel<<<num_blocks, block_size>>>(
            d_data, len, config.delimiter, d_quote_state,
            nullptr, d_field_count  // Just counting
        );

        uint32_t h_field_count = 0;
        cudaMemcpy(&h_field_count, d_field_count, sizeof(uint32_t), cudaMemcpyDeviceToHost);

        // Allocate field positions array
        err = cudaMalloc(&result.d_field_positions, (h_field_count + 1) * sizeof(uint32_t));
        if (err != cudaSuccess) {
            // Cleanup
            cudaFree(d_data);
            cudaFree(d_newline_count);
            cudaFree(d_quote_bitmap);
            cudaFree(d_quote_flags);
            cudaFree(d_quote_state);
            cudaFree(d_temp_storage);
            cudaFree(d_field_count);
            result.error_message = "Failed to allocate field positions";
            return result;
        }

        // Second pass: write positions
        cudaMemset(d_field_count, 0, sizeof(uint32_t));
        filter_delimiters_by_quote_state_kernel<<<num_blocks, block_size>>>(
            d_data, len, config.delimiter, d_quote_state,
            result.d_field_positions, d_field_count
        );

        result.num_fields = h_field_count;

        // Cleanup temporary allocations
        cudaFree(d_quote_bitmap);
        cudaFree(d_quote_flags);
        cudaFree(d_quote_state);
        cudaFree(d_temp_storage);
        cudaFree(d_field_count);
    }

    if (timings) {
        cudaEventRecord(kernel_done);
    }

    // Cleanup
    cudaFree(d_data);
    cudaFree(d_newline_count);

    if (timings) {
        cudaEventRecord(d2h_done);
        cudaEventSynchronize(d2h_done);

        cudaEventElapsedTime(&timings->h2d_transfer_ms, start, h2d_done);
        cudaEventElapsedTime(&timings->kernel_exec_ms, h2d_done, kernel_done);
        cudaEventElapsedTime(&timings->d2h_transfer_ms, kernel_done, d2h_done);
        cudaEventElapsedTime(&timings->total_ms, start, d2h_done);

        cudaEventDestroy(start);
        cudaEventDestroy(h2d_done);
        cudaEventDestroy(kernel_done);
        cudaEventDestroy(d2h_done);
    }

    result.success = true;
    return result;
}

void free_gpu_result(GpuParseResult& result) {
    if (result.d_newline_positions) {
        cudaFree(result.d_newline_positions);
        result.d_newline_positions = nullptr;
    }
    if (result.d_field_positions) {
        cudaFree(result.d_field_positions);
        result.d_field_positions = nullptr;
    }
    result.num_lines = 0;
    result.num_fields = 0;
}

bool copy_newline_positions_to_host(
    const GpuParseResult& result,
    uint32_t* host_positions
) {
    if (!result.d_newline_positions || result.num_lines == 0) {
        return false;
    }

    cudaError_t err = cudaMemcpy(
        host_positions,
        result.d_newline_positions,
        result.num_lines * sizeof(uint32_t),
        cudaMemcpyDeviceToHost
    );

    return err == cudaSuccess;
}

bool copy_field_positions_to_host(
    const GpuParseResult& result,
    uint32_t* host_positions
) {
    if (!result.d_field_positions || result.num_fields == 0) {
        return false;
    }

    cudaError_t err = cudaMemcpy(
        host_positions,
        result.d_field_positions,
        result.num_fields * sizeof(uint32_t),
        cudaMemcpyDeviceToHost
    );

    return err == cudaSuccess;
}

}  // namespace gpu
}  // namespace vroom
