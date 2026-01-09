#include "csv_gpu.cuh"

#include <benchmark/benchmark.h>
#include <cstring>
#include <fstream>
#include <gpu_parser.h>
#include <libvroom.h>
#include <sstream>
#include <string>
#include <thread>
#include <vector>

namespace {

// Padding required for SIMD operations
constexpr size_t SIMD_PADDING = 64;

// =============================================================================
// Test Data Generation
// =============================================================================

std::string generate_csv(size_t rows, size_t cols, bool with_quotes = false) {
  std::string csv;
  csv.reserve(rows * cols * 10);

  // Header
  for (size_t c = 0; c < cols; c++) {
    if (c > 0)
      csv += ',';
    csv += "col" + std::to_string(c);
  }
  csv += '\n';

  // Data
  for (size_t r = 0; r < rows; r++) {
    for (size_t c = 0; c < cols; c++) {
      if (c > 0)
        csv += ',';
      if (with_quotes && c % 3 == 0) {
        csv += "\"value" + std::to_string(r * cols + c) + "\"";
      } else {
        csv += std::to_string(r * cols + c);
      }
    }
    csv += '\n';
  }

  return csv;
}

// Generate CSV of approximately target_size bytes
std::string generate_csv_by_size(size_t target_size, bool with_quotes = false) {
  const size_t cols = 10;
  // Estimate ~8 chars per field + comma/newline
  size_t estimated_row_size = cols * 9;
  size_t rows = target_size / estimated_row_size;
  return generate_csv(rows, cols, with_quotes);
}

// Aligned buffer for CPU benchmarks (matches libvroom's AlignedBuffer)
struct AlignedTestBuffer {
  uint8_t* data = nullptr;
  size_t size = 0;

  AlignedTestBuffer(const std::string& csv) {
    size = csv.size();
    // Allocate 64-byte aligned memory with padding
    data = static_cast<uint8_t*>(aligned_alloc(64, size + SIMD_PADDING));
    if (data) {
      std::memcpy(data, csv.data(), size);
      // Zero the padding
      std::memset(data + size, 0, SIMD_PADDING);
    }
  }

  ~AlignedTestBuffer() {
    if (data) {
      free(data);
    }
  }

  // Non-copyable
  AlignedTestBuffer(const AlignedTestBuffer&) = delete;
  AlignedTestBuffer& operator=(const AlignedTestBuffer&) = delete;

  // Movable
  AlignedTestBuffer(AlignedTestBuffer&& other) noexcept : data(other.data), size(other.size) {
    other.data = nullptr;
    other.size = 0;
  }
};

// =============================================================================
// GPU Parsing Benchmarks
// =============================================================================

static void BM_GpuParseSimple(benchmark::State& state) {
  auto info = vroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, false);

  vroom::gpu::GpuParseConfig config;
  config.handle_quotes = false;

  for (auto _ : state) {
    vroom::gpu::GpuTimings timings;
    auto result = vroom::gpu::parse_csv_gpu(csv.data(), csv.size(), config, &timings);

    if (!result.success) {
      state.SkipWithError(result.error_message);
      return;
    }

    benchmark::DoNotOptimize(result.num_lines);
    vroom::gpu::free_gpu_result(result);
  }

  state.SetBytesProcessed(state.iterations() * csv.size());
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

static void BM_GpuParseWithQuotes(benchmark::State& state) {
  auto info = vroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, true);

  vroom::gpu::GpuParseConfig config;
  config.handle_quotes = true;

  for (auto _ : state) {
    vroom::gpu::GpuTimings timings;
    auto result = vroom::gpu::parse_csv_gpu(csv.data(), csv.size(), config, &timings);

    if (!result.success) {
      state.SkipWithError(result.error_message);
      return;
    }

    benchmark::DoNotOptimize(result.num_lines);
    vroom::gpu::free_gpu_result(result);
  }

  state.SetBytesProcessed(state.iterations() * csv.size());
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

// =============================================================================
// CPU Parsing Benchmarks (for comparison)
// =============================================================================

static void BM_CpuParse(benchmark::State& state) {
  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, false);

  // Use aligned memory like the production benchmarks
  AlignedTestBuffer buffer(csv);
  if (!buffer.data) {
    state.SkipWithError("Failed to allocate aligned buffer");
    return;
  }

  // Use hardware thread count for fair comparison with GPU
  size_t num_threads = std::thread::hardware_concurrency();
  libvroom::Parser parser(num_threads);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data, buffer.size);
    benchmark::DoNotOptimize(result.num_rows());
  }

  state.SetBytesProcessed(state.iterations() * buffer.size);
  state.SetLabel(std::to_string(buffer.size / (1024 * 1024)) + " MB, " +
                 std::to_string(num_threads) + " threads");
}

static void BM_CpuParseWithQuotes(benchmark::State& state) {
  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, true);

  // Use aligned memory like the production benchmarks
  AlignedTestBuffer buffer(csv);
  if (!buffer.data) {
    state.SkipWithError("Failed to allocate aligned buffer");
    return;
  }

  // Use hardware thread count for fair comparison with GPU
  size_t num_threads = std::thread::hardware_concurrency();
  libvroom::Parser parser(num_threads);

  for (auto _ : state) {
    auto result = parser.parse(buffer.data, buffer.size);
    benchmark::DoNotOptimize(result.num_rows());
  }

  state.SetBytesProcessed(state.iterations() * buffer.size);
  state.SetLabel(std::to_string(buffer.size / (1024 * 1024)) + " MB, " +
                 std::to_string(num_threads) + " threads");
}

// Single-threaded CPU benchmark for baseline comparison
static void BM_CpuParseSingleThread(benchmark::State& state) {
  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, false);

  AlignedTestBuffer buffer(csv);
  if (!buffer.data) {
    state.SkipWithError("Failed to allocate aligned buffer");
    return;
  }

  libvroom::Parser parser(1); // Single thread

  for (auto _ : state) {
    auto result = parser.parse(buffer.data, buffer.size);
    benchmark::DoNotOptimize(result.num_rows());
  }

  state.SetBytesProcessed(state.iterations() * buffer.size);
  state.SetLabel(std::to_string(buffer.size / (1024 * 1024)) + " MB, 1 thread");
}

// Fair comparison: CPU TwoPass index building only (matches what GPU does)
// Skips dialect detection, uses pre-set dialect, minimal error handling
static void BM_CpuTwoPassIndexOnly(benchmark::State& state) {
  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, false);

  AlignedTestBuffer buffer(csv);
  if (!buffer.data) {
    state.SkipWithError("Failed to allocate aligned buffer");
    return;
  }

  libvroom::TwoPass parser;
  const char delimiter = ',';
  const char quote_char = '"';

  for (auto _ : state) {
    // First pass: count separators (matches GPU counting pass)
    auto stats =
        libvroom::TwoPass::first_pass_simd(buffer.data, 0, buffer.size, quote_char, delimiter);

    // Allocate index based on count
    auto idx = parser.init_counted(stats.n_separators, 1);

    // Second pass: build index (matches GPU index building)
    libvroom::TwoPass::second_pass_simd(buffer.data, 0, buffer.size, &idx, 0, delimiter,
                                        quote_char);

    benchmark::DoNotOptimize(idx.indexes);
    benchmark::DoNotOptimize(stats.n_separators);
  }

  state.SetBytesProcessed(state.iterations() * buffer.size);
  state.SetLabel(std::to_string(buffer.size / (1024 * 1024)) + " MB, TwoPass only");
}

// =============================================================================
// Detailed Timing Benchmarks
// =============================================================================

// Measure transfer overhead vs computation
static void BM_GpuTimingBreakdown(benchmark::State& state) {
  auto info = vroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = state.range(0);
  std::string csv = generate_csv_by_size(data_size, false);

  vroom::gpu::GpuParseConfig config;
  config.handle_quotes = false;

  double total_h2d = 0, total_kernel = 0, total_d2h = 0;
  int64_t count = 0;

  for (auto _ : state) {
    vroom::gpu::GpuTimings timings;
    auto result = vroom::gpu::parse_csv_gpu(csv.data(), csv.size(), config, &timings);

    if (result.success) {
      total_h2d += timings.h2d_transfer_ms;
      total_kernel += timings.kernel_exec_ms;
      total_d2h += timings.d2h_transfer_ms;
      count++;
    }

    vroom::gpu::free_gpu_result(result);
  }

  if (count > 0) {
    state.counters["H2D_ms"] = total_h2d / count;
    state.counters["Kernel_ms"] = total_kernel / count;
    state.counters["D2H_ms"] = total_d2h / count;
    state.counters["Transfer%"] =
        100.0 * (total_h2d + total_d2h) / (total_h2d + total_kernel + total_d2h);
  }

  state.SetBytesProcessed(state.iterations() * csv.size());
}

// =============================================================================
// Benchmark Registration
// =============================================================================

// Test different data sizes to find crossover point
// 1MB, 10MB, 50MB, 100MB, 250MB, 500MB
BENCHMARK(BM_GpuParseSimple)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(50 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Arg(250 * 1024 * 1024)
    ->Arg(500 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_GpuParseWithQuotes)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(50 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_CpuParse)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(50 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Arg(250 * 1024 * 1024)
    ->Arg(500 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_CpuParseSingleThread)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

// Fair comparison: just TwoPass index building (no dialect detection, no error handling)
BENCHMARK(BM_CpuTwoPassIndexOnly)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(50 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Arg(250 * 1024 * 1024)
    ->Arg(500 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_CpuParseWithQuotes)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(50 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_GpuTimingBreakdown)
    ->Arg(10 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Arg(500 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

} // namespace

BENCHMARK_MAIN();
