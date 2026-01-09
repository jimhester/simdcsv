#ifndef CSV_GPU_CUH
#define CSV_GPU_CUH

#include <cstdint>
#include <cstddef>

namespace vroom {
namespace gpu {

// =============================================================================
// Configuration and Result Structures (C++ compatible)
// =============================================================================

// Configuration for GPU parsing
struct GpuParseConfig {
    char delimiter = ',';
    char quote_char = '"';
    bool handle_quotes = true;
    int block_size = 256;        // Threads per block
    int max_blocks = 65535;      // Maximum number of blocks
};

// Result structure for GPU parsing
struct GpuParseResult {
    uint32_t* d_newline_positions = nullptr;  // Device pointer
    uint32_t* d_field_positions = nullptr;    // Device pointer
    uint32_t num_lines = 0;
    uint32_t num_fields = 0;
    bool success = false;
    const char* error_message = nullptr;
};

// Timing information for benchmarking
struct GpuTimings {
    float h2d_transfer_ms = 0.0f;     // Host to device transfer
    float kernel_exec_ms = 0.0f;      // Kernel execution time
    float d2h_transfer_ms = 0.0f;     // Device to host transfer
    float total_ms = 0.0f;            // Total time including all overhead
};

// GPU capabilities information
struct GpuInfo {
    bool cuda_available = false;
    int device_count = 0;
    char device_name[256] = {0};
    size_t total_memory = 0;
    size_t free_memory = 0;
    int compute_capability_major = 0;
    int compute_capability_minor = 0;
    int max_threads_per_block = 0;
    int max_blocks_per_sm = 0;
    int sm_count = 0;
};

// =============================================================================
// Host-callable functions (implemented in csv_gpu.cu)
// =============================================================================

// Query GPU capabilities
GpuInfo query_gpu_info();

// Check if GPU parsing is available and worthwhile for given data size
bool should_use_gpu(size_t data_size, const GpuInfo& info);

// Main entry point for GPU CSV parsing
GpuParseResult parse_csv_gpu(
    const char* data,
    size_t len,
    const GpuParseConfig& config,
    GpuTimings* timings  // default nullptr defined in gpu_parser.h
);

// Free GPU resources from a parse result
void free_gpu_result(GpuParseResult& result);

// Copy results from device to host
bool copy_newline_positions_to_host(const GpuParseResult& result, uint32_t* host);
bool copy_field_positions_to_host(const GpuParseResult& result, uint32_t* host);

}  // namespace gpu
}  // namespace vroom

#endif  // CSV_GPU_CUH
