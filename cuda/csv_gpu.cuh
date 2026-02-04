#ifndef CSV_GPU_CUH
#define CSV_GPU_CUH

#include <cstddef>
#include <cstdint>

namespace libvroom {
namespace gpu {

/// Configuration for GPU CSV parsing kernels.
struct GpuParseConfig {
  char delimiter = ',';
  char quote_char = '"';
  bool handle_quotes = true;
  int block_size = 256;
  int max_blocks = 65535;
};

/// Timing information collected via CUDA events.
struct GpuTimings {
  float h2d_transfer_ms = 0.0f;
  float kernel_exec_ms = 0.0f;
  float d2h_transfer_ms = 0.0f;
  float total_ms = 0.0f;
};

/// GPU hardware capabilities.
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

/// Result of gpu_find_field_boundaries().
/// Positions are byte offsets where delimiters or newlines occur outside quotes.
/// Caller owns the host allocation in `positions` (allocated with new[]).
struct GpuIndexResult {
  uint32_t* positions = nullptr;
  uint32_t count = 0;
  uint32_t num_lines = 0;
  bool success = false;
  const char* error_message = nullptr;
};

/// Query GPU capabilities (device 0).
GpuInfo query_gpu_info();

/// Heuristic: should we use GPU for this data size?
bool should_use_gpu(size_t data_size, const GpuInfo& info);

/// Find all field boundaries (delimiters and newlines outside quotes).
/// Returns sorted host-side positions. Caller must call gpu_cleanup().
GpuIndexResult gpu_find_field_boundaries(
    const char* data,
    size_t len,
    const GpuParseConfig& config,
    GpuTimings* timings = nullptr);

/// Free positions returned by gpu_find_field_boundaries.
void gpu_cleanup(GpuIndexResult& result);

}  // namespace gpu
}  // namespace libvroom

#endif  // CSV_GPU_CUH
