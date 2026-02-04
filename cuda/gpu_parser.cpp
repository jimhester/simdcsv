#include "libvroom/gpu_parser.h"

#include "csv_gpu.cuh"

#include <sstream>
#include <vector>

namespace libvroom {
namespace gpu {

GpuCsvIndex::~GpuCsvIndex() = default;

GpuCsvIndex::GpuCsvIndex(GpuCsvIndex&& other) noexcept
    : valid_(other.valid_), error_msg_(std::move(other.error_msg_)), num_fields_(other.num_fields_),
      num_lines_(other.num_lines_), positions_(std::move(other.positions_)), h2d_ms_(other.h2d_ms_),
      kernel_ms_(other.kernel_ms_), d2h_ms_(other.d2h_ms_), total_ms_(other.total_ms_) {
  other.valid_ = false;
  other.num_fields_ = 0;
  other.num_lines_ = 0;
}

GpuCsvIndex& GpuCsvIndex::operator=(GpuCsvIndex&& other) noexcept {
  if (this != &other) {
    valid_ = other.valid_;
    error_msg_ = std::move(other.error_msg_);
    num_fields_ = other.num_fields_;
    num_lines_ = other.num_lines_;
    positions_ = std::move(other.positions_);
    h2d_ms_ = other.h2d_ms_;
    kernel_ms_ = other.kernel_ms_;
    d2h_ms_ = other.d2h_ms_;
    total_ms_ = other.total_ms_;

    other.valid_ = false;
    other.num_fields_ = 0;
    other.num_lines_ = 0;
  }
  return *this;
}

bool GpuCsvIndex::build(const char* data, size_t len, char delimiter, char quote_char,
                        bool handle_quotes) {
  valid_ = false;
  error_msg_.clear();
  positions_.clear();
  num_fields_ = 0;
  num_lines_ = 0;

  GpuParseConfig config;
  config.delimiter = delimiter;
  config.quote_char = quote_char;
  config.handle_quotes = handle_quotes;

  GpuTimings timings;
  GpuIndexResult res = gpu_find_field_boundaries(data, len, config, &timings);

  if (!res.success) {
    error_msg_ = res.error_message ? res.error_message : "Unknown GPU error";
    gpu_cleanup(res);
    return false;
  }

  if (res.count > 0 && res.positions) {
    positions_.assign(res.positions, res.positions + res.count);
  }

  num_fields_ = res.count;
  num_lines_ = res.num_lines;

  h2d_ms_ = timings.h2d_transfer_ms;
  kernel_ms_ = timings.kernel_exec_ms;
  d2h_ms_ = timings.d2h_transfer_ms;
  total_ms_ = timings.total_ms;

  gpu_cleanup(res);
  valid_ = true;
  return true;
}

std::string gpu_info_string() {
  GpuInfo info = query_gpu_info();
  if (!info.cuda_available) {
    return "CUDA not available";
  }

  std::ostringstream oss;
  oss << "GPU: " << info.device_name << "\n"
      << "  Compute capability: " << info.compute_capability_major << "."
      << info.compute_capability_minor << "\n"
      << "  SMs: " << info.sm_count << "\n"
      << "  Max threads/block: " << info.max_threads_per_block << "\n"
      << "  Total memory: " << (info.total_memory / (1024 * 1024)) << " MB\n"
      << "  Free memory: " << (info.free_memory / (1024 * 1024)) << " MB";
  return oss.str();
}

bool cuda_available() {
  GpuInfo info = query_gpu_info();
  return info.cuda_available;
}

size_t min_gpu_file_size() {
  return 10 * 1024 * 1024;
}

} // namespace gpu
} // namespace libvroom
