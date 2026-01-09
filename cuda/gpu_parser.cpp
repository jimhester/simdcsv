#include "csv_gpu.cuh"

#include <gpu_parser.h>
#include <sstream>
#include <vector>

namespace vroom {
namespace gpu {

// =============================================================================
// GpuCsvIndex implementation
// =============================================================================

GpuCsvIndex::~GpuCsvIndex() {
  release();
}

GpuCsvIndex::GpuCsvIndex(GpuCsvIndex&& other) noexcept
    : valid_(other.valid_), error_msg_(std::move(other.error_msg_)), num_lines_(other.num_lines_),
      num_fields_(other.num_fields_), d_line_positions_(other.d_line_positions_),
      d_field_positions_(other.d_field_positions_),
      h_line_positions_(std::move(other.h_line_positions_)),
      h_field_positions_(std::move(other.h_field_positions_)), lines_cached_(other.lines_cached_),
      fields_cached_(other.fields_cached_), h2d_ms_(other.h2d_ms_), kernel_ms_(other.kernel_ms_),
      d2h_ms_(other.d2h_ms_), total_ms_(other.total_ms_) {
  other.valid_ = false;
  other.d_line_positions_ = nullptr;
  other.d_field_positions_ = nullptr;
}

GpuCsvIndex& GpuCsvIndex::operator=(GpuCsvIndex&& other) noexcept {
  if (this != &other) {
    release();

    valid_ = other.valid_;
    error_msg_ = std::move(other.error_msg_);
    num_lines_ = other.num_lines_;
    num_fields_ = other.num_fields_;
    d_line_positions_ = other.d_line_positions_;
    d_field_positions_ = other.d_field_positions_;
    h_line_positions_ = std::move(other.h_line_positions_);
    h_field_positions_ = std::move(other.h_field_positions_);
    lines_cached_ = other.lines_cached_;
    fields_cached_ = other.fields_cached_;
    h2d_ms_ = other.h2d_ms_;
    kernel_ms_ = other.kernel_ms_;
    d2h_ms_ = other.d2h_ms_;
    total_ms_ = other.total_ms_;

    other.valid_ = false;
    other.d_line_positions_ = nullptr;
    other.d_field_positions_ = nullptr;
  }
  return *this;
}

void GpuCsvIndex::release() {
  if (d_line_positions_ || d_field_positions_) {
    GpuParseResult result;
    result.d_newline_positions = static_cast<uint32_t*>(d_line_positions_);
    result.d_field_positions = static_cast<uint32_t*>(d_field_positions_);
    free_gpu_result(result);
  }
  d_line_positions_ = nullptr;
  d_field_positions_ = nullptr;
  h_line_positions_.clear();
  h_field_positions_.clear();
  lines_cached_ = false;
  fields_cached_ = false;
  valid_ = false;
}

bool GpuCsvIndex::parse(const char* data, size_t len, char delimiter, char quote_char,
                        bool handle_quotes) {
  release();

  // Configure GPU parsing
  GpuParseConfig config;
  config.delimiter = delimiter;
  config.quote_char = quote_char;
  config.handle_quotes = handle_quotes;

  // Collect timing information
  GpuTimings timings;

  // Run GPU parsing
  GpuParseResult result = parse_csv_gpu(data, len, config, &timings);

  if (!result.success) {
    error_msg_ = result.error_message ? result.error_message : "Unknown GPU error";
    return false;
  }

  // Store results
  valid_ = true;
  num_lines_ = result.num_lines;
  num_fields_ = result.num_fields;
  d_line_positions_ = result.d_newline_positions;
  d_field_positions_ = result.d_field_positions;

  // Store timing
  h2d_ms_ = timings.h2d_transfer_ms;
  kernel_ms_ = timings.kernel_exec_ms;
  d2h_ms_ = timings.d2h_transfer_ms;
  total_ms_ = timings.total_ms;

  // Clear ownership from result (we own the pointers now)
  result.d_newline_positions = nullptr;
  result.d_field_positions = nullptr;

  return true;
}

const std::vector<uint32_t>& GpuCsvIndex::line_positions() {
  if (!lines_cached_ && valid_ && d_line_positions_) {
    h_line_positions_.resize(num_lines_);

    GpuParseResult result;
    result.d_newline_positions = static_cast<uint32_t*>(d_line_positions_);
    result.num_lines = num_lines_;

    copy_newline_positions_to_host(result, h_line_positions_.data());
    lines_cached_ = true;
  }
  return h_line_positions_;
}

const std::vector<uint32_t>& GpuCsvIndex::field_positions() {
  if (!fields_cached_ && valid_ && d_field_positions_) {
    h_field_positions_.resize(num_fields_);

    GpuParseResult result;
    result.d_field_positions = static_cast<uint32_t*>(d_field_positions_);
    result.num_fields = num_fields_;

    copy_field_positions_to_host(result, h_field_positions_.data());
    fields_cached_ = true;
  }
  return h_field_positions_;
}

// =============================================================================
// Utility functions
// =============================================================================

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
  // This threshold should be determined empirically via benchmarking
  // For now, use a conservative default of 10 MB
  return 10 * 1024 * 1024;
}

} // namespace gpu
} // namespace vroom
