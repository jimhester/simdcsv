#ifndef LIBVROOM_GPU_PARSER_H
#define LIBVROOM_GPU_PARSER_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <vector>

namespace libvroom {
namespace gpu {

#ifdef LIBVROOM_ENABLE_GPU

struct GpuParseConfig;
struct GpuTimings;
struct GpuInfo;
struct GpuIndexResult;

GpuInfo query_gpu_info();
bool should_use_gpu(size_t data_size, const GpuInfo& info);

class GpuCsvIndex {
public:
  GpuCsvIndex() = default;
  ~GpuCsvIndex();

  GpuCsvIndex(const GpuCsvIndex&) = delete;
  GpuCsvIndex& operator=(const GpuCsvIndex&) = delete;

  GpuCsvIndex(GpuCsvIndex&& other) noexcept;
  GpuCsvIndex& operator=(GpuCsvIndex&& other) noexcept;

  bool build(const char* data, size_t len, char delimiter = ',', char quote_char = '"',
             bool handle_quotes = true);

  bool is_valid() const { return valid_; }
  const std::string& error() const { return error_msg_; }
  uint32_t num_fields() const { return num_fields_; }
  uint32_t num_lines() const { return num_lines_; }
  const std::vector<uint32_t>& positions() const { return positions_; }

  float h2d_transfer_ms() const { return h2d_ms_; }
  float kernel_exec_ms() const { return kernel_ms_; }
  float d2h_transfer_ms() const { return d2h_ms_; }
  float total_ms() const { return total_ms_; }

private:
  bool valid_ = false;
  std::string error_msg_;
  uint32_t num_fields_ = 0;
  uint32_t num_lines_ = 0;
  std::vector<uint32_t> positions_;
  float h2d_ms_ = 0.0f;
  float kernel_ms_ = 0.0f;
  float d2h_ms_ = 0.0f;
  float total_ms_ = 0.0f;
};

std::string gpu_info_string();
bool cuda_available();
size_t min_gpu_file_size();

#else

inline bool cuda_available() {
  return false;
}
inline std::string gpu_info_string() {
  return "GPU support not compiled in. Build with -DENABLE_GPU=ON";
}
inline size_t min_gpu_file_size() {
  return 0;
}

#endif

} // namespace gpu
} // namespace libvroom

#endif // LIBVROOM_GPU_PARSER_H
