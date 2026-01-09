#ifndef LIBVROOM_GPU_PARSER_H
#define LIBVROOM_GPU_PARSER_H

// GPU-accelerated CSV parsing (experimental)
//
// This module provides optional GPU acceleration for CSV parsing using CUDA.
// It is only available when libvroom is built with -DENABLE_GPU=ON.

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace vroom {
namespace gpu {

#ifdef LIBVROOM_ENABLE_GPU

// Forward declarations - full definitions in csv_gpu.cuh
struct GpuParseConfig;
struct GpuParseResult;
struct GpuTimings;
struct GpuInfo;

// Query GPU capabilities
GpuInfo query_gpu_info();

// Check if GPU parsing should be used for given data size
bool should_use_gpu(size_t data_size, const GpuInfo& info);

// Main parsing entry point
GpuParseResult parse_csv_gpu(const char* data, size_t len, const GpuParseConfig& config,
                             GpuTimings* timings = nullptr);

// Free GPU resources
void free_gpu_result(GpuParseResult& result);

// Copy results to host memory
bool copy_newline_positions_to_host(const GpuParseResult& result, uint32_t* host);
bool copy_field_positions_to_host(const GpuParseResult& result, uint32_t* host);

// =============================================================================
// High-level C++ wrapper for easier integration
// =============================================================================

class GpuCsvIndex {
public:
  GpuCsvIndex() = default;
  ~GpuCsvIndex();

  // Non-copyable
  GpuCsvIndex(const GpuCsvIndex&) = delete;
  GpuCsvIndex& operator=(const GpuCsvIndex&) = delete;

  // Movable
  GpuCsvIndex(GpuCsvIndex&& other) noexcept;
  GpuCsvIndex& operator=(GpuCsvIndex&& other) noexcept;

  // Parse CSV data on GPU
  bool parse(const char* data, size_t len, char delimiter = ',', char quote_char = '"',
             bool handle_quotes = true);

  // Accessors
  bool is_valid() const { return valid_; }
  const char* error_message() const { return error_msg_.c_str(); }
  uint32_t num_lines() const { return num_lines_; }
  uint32_t num_fields() const { return num_fields_; }

  // Get positions (copies from GPU on first call)
  const std::vector<uint32_t>& line_positions();
  const std::vector<uint32_t>& field_positions();

  // Timing info
  float h2d_transfer_ms() const { return h2d_ms_; }
  float kernel_exec_ms() const { return kernel_ms_; }
  float d2h_transfer_ms() const { return d2h_ms_; }
  float total_ms() const { return total_ms_; }

private:
  void release();

  bool valid_ = false;
  std::string error_msg_;
  uint32_t num_lines_ = 0;
  uint32_t num_fields_ = 0;
  void* d_line_positions_ = nullptr;
  void* d_field_positions_ = nullptr;
  std::vector<uint32_t> h_line_positions_;
  std::vector<uint32_t> h_field_positions_;
  bool lines_cached_ = false;
  bool fields_cached_ = false;
  float h2d_ms_ = 0.0f;
  float kernel_ms_ = 0.0f;
  float d2h_ms_ = 0.0f;
  float total_ms_ = 0.0f;
};

// Utility functions
std::string gpu_info_string();
bool cuda_available();
size_t min_gpu_file_size();

#else // LIBVROOM_ENABLE_GPU not defined

// Stub implementations when GPU support is not compiled in
inline bool cuda_available() {
  return false;
}

inline std::string gpu_info_string() {
  return "GPU support not compiled in. Build with -DENABLE_GPU=ON";
}

inline size_t min_gpu_file_size() {
  return 0;
}

#endif // LIBVROOM_ENABLE_GPU

} // namespace gpu
} // namespace vroom

#endif // LIBVROOM_GPU_PARSER_H
