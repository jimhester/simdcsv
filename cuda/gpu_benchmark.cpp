// GPU Acceleration Benchmark Harness
//
// Three approaches compared:
// 1. CPU Baseline:  libvroom SIMD field boundary detection (SplitFields iterator)
// 2. GPU Full:      CUDA kernels for field boundary detection (CUB prefix scan)
// 3. Hybrid:        GPU for boundary detection, CPU for row splitting
//
// All approaches count field boundaries (delimiters + newlines outside quotes)
// on the same generated CSV data at various sizes.

#include "libvroom/gpu_parser.h"
#include "libvroom/split_fields.h"
#include "libvroom/vroom.h"

#include "csv_gpu.cuh"

#include <algorithm>
#include <benchmark/benchmark.h>
#include <string>
#include <vector>

namespace {

// Generate CSV data with the specified target size.
// Uses 10 numeric columns with optional quoted fields for quote handling tests.
std::string generate_csv(size_t target_size, bool with_quotes = false) {
  const size_t cols = 10;
  std::string csv;
  csv.reserve(target_size + 1024);

  // Header
  for (size_t c = 0; c < cols; c++) {
    if (c > 0)
      csv += ',';
    csv += "col" + std::to_string(c);
  }
  csv += '\n';

  size_t row = 0;
  while (csv.size() < target_size) {
    for (size_t c = 0; c < cols; c++) {
      if (c > 0)
        csv += ',';
      if (with_quotes && c == 0) {
        // Every row has one quoted field with a comma inside
        csv += "\"val," + std::to_string(row) + "\"";
      } else {
        csv += std::to_string(row * cols + c);
      }
    }
    csv += '\n';
    row++;
  }
  return csv;
}

// =============================================================================
// CPU Baseline: SIMD field boundary counting using SplitFields iterator
// =============================================================================

// Count all field boundaries in a CSV buffer using the CPU SplitFields iterator.
// This iterates line-by-line, counting fields per line (which equals boundaries + 1).
static size_t cpu_count_boundaries(const char* data, size_t size, char sep, char quote,
                                   bool handle_quotes) {
  size_t boundary_count = 0;
  const char* pos = data;
  const char* end = data + size;

  while (pos < end) {
    // Find the end of this line
    const char* line_end = pos;
    while (line_end < end && *line_end != '\n') {
      ++line_end;
    }
    size_t line_len = static_cast<size_t>(line_end - pos);

    if (line_len > 0) {
      // Count fields in this line using SplitFields iterator
      libvroom::SplitFields splitter(pos, line_len, sep, handle_quotes ? quote : '\0', '\n');
      const char* field_data;
      size_t field_len;
      bool needs_escaping;
      size_t fields_in_line = 0;
      while (splitter.next(field_data, field_len, needs_escaping)) {
        fields_in_line++;
      }
      // fields_in_line fields = (fields_in_line - 1) separators + 1 newline
      // but we just count the total boundaries
      boundary_count += fields_in_line; // each separator + the newline
    }

    pos = line_end + 1; // skip past the newline
  }

  return boundary_count;
}

static void BM_CpuFindBoundaries(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  bool with_quotes = state.range(1) != 0;
  std::string csv = generate_csv(data_size, with_quotes);

  for (auto _ : state) {
    size_t count = cpu_count_boundaries(csv.data(), csv.size(), ',', '"', with_quotes);
    benchmark::DoNotOptimize(count);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * csv.size()));
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

// CPU row counting using ChunkFinder (SIMD-accelerated)
static void BM_CpuCountRows(benchmark::State& state) {
  size_t data_size = static_cast<size_t>(state.range(0));
  bool with_quotes = state.range(1) != 0;
  std::string csv = generate_csv(data_size, with_quotes);

  libvroom::ChunkFinder finder(',', '"');

  for (auto _ : state) {
    auto [row_count, last_end] = finder.count_rows(csv.data(), csv.size());
    benchmark::DoNotOptimize(row_count);
    benchmark::DoNotOptimize(last_end);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * csv.size()));
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

// =============================================================================
// GPU Full: CUDA kernel for field boundary detection
// =============================================================================

static void BM_GpuFindBoundaries(benchmark::State& state) {
  auto info = libvroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = static_cast<size_t>(state.range(0));
  bool with_quotes = state.range(1) != 0;
  std::string csv = generate_csv(data_size, with_quotes);

  libvroom::gpu::GpuParseConfig config;
  config.delimiter = ',';
  config.quote_char = '"';
  config.handle_quotes = with_quotes;

  // Warmup: one iteration to initialize GPU context
  {
    auto res = libvroom::gpu::gpu_find_field_boundaries(csv.data(), csv.size(), config);
    libvroom::gpu::gpu_cleanup(res);
  }

  for (auto _ : state) {
    libvroom::gpu::GpuTimings timings;
    auto res = libvroom::gpu::gpu_find_field_boundaries(csv.data(), csv.size(), config, &timings);
    if (!res.success) {
      state.SkipWithError(res.error_message);
      return;
    }
    benchmark::DoNotOptimize(res.count);
    libvroom::gpu::gpu_cleanup(res);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * csv.size()));
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

// GPU with detailed timing breakdown (reports H2D, kernel, D2H phases)
static void BM_GpuFindBoundariesDetailed(benchmark::State& state) {
  auto info = libvroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = static_cast<size_t>(state.range(0));
  bool with_quotes = state.range(1) != 0;
  std::string csv = generate_csv(data_size, with_quotes);

  libvroom::gpu::GpuParseConfig config;
  config.delimiter = ',';
  config.quote_char = '"';
  config.handle_quotes = with_quotes;

  // Warmup
  {
    auto res = libvroom::gpu::gpu_find_field_boundaries(csv.data(), csv.size(), config);
    libvroom::gpu::gpu_cleanup(res);
  }

  double total_h2d = 0, total_kernel = 0, total_d2h = 0;
  int64_t iters = 0;

  for (auto _ : state) {
    libvroom::gpu::GpuTimings timings;
    auto res = libvroom::gpu::gpu_find_field_boundaries(csv.data(), csv.size(), config, &timings);
    if (!res.success) {
      state.SkipWithError(res.error_message);
      return;
    }
    benchmark::DoNotOptimize(res.count);

    total_h2d += timings.h2d_transfer_ms;
    total_kernel += timings.kernel_exec_ms;
    total_d2h += timings.d2h_transfer_ms;
    iters++;

    libvroom::gpu::gpu_cleanup(res);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * csv.size()));
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");

  if (iters > 0) {
    state.counters["h2d_ms"] =
        benchmark::Counter(total_h2d / iters, benchmark::Counter::kAvgThreads);
    state.counters["kernel_ms"] =
        benchmark::Counter(total_kernel / iters, benchmark::Counter::kAvgThreads);
    state.counters["d2h_ms"] =
        benchmark::Counter(total_d2h / iters, benchmark::Counter::kAvgThreads);
  }
}

// =============================================================================
// Hybrid: GPU boundary detection + CPU row splitting
// =============================================================================

static void BM_HybridGpuCpu(benchmark::State& state) {
  auto info = libvroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = static_cast<size_t>(state.range(0));
  bool with_quotes = state.range(1) != 0;
  std::string csv = generate_csv(data_size, with_quotes);

  libvroom::gpu::GpuParseConfig config;
  config.delimiter = ',';
  config.quote_char = '"';
  config.handle_quotes = with_quotes;

  // Warmup
  {
    auto res = libvroom::gpu::gpu_find_field_boundaries(csv.data(), csv.size(), config);
    libvroom::gpu::gpu_cleanup(res);
  }

  for (auto _ : state) {
    // Phase 1: GPU finds all boundary positions
    libvroom::gpu::GpuTimings timings;
    auto res = libvroom::gpu::gpu_find_field_boundaries(csv.data(), csv.size(), config, &timings);
    if (!res.success) {
      state.SkipWithError(res.error_message);
      return;
    }

    // Phase 2: CPU uses boundary positions to split into rows
    // This simulates the hybrid approach where GPU provides the index
    // and CPU uses it for field extraction
    size_t row_count = 0;
    if (res.count > 0 && res.positions != nullptr) {
      // Sort positions (GPU atomics produce unordered output)
      std::vector<uint32_t> sorted_positions(res.positions, res.positions + res.count);
      std::sort(sorted_positions.begin(), sorted_positions.end());

      // Walk through positions to count rows (newline boundaries)
      for (uint32_t i = 0; i < res.count; i++) {
        uint32_t pos = sorted_positions[i];
        if (pos < csv.size() && csv[pos] == '\n') {
          row_count++;
        }
      }
    }
    benchmark::DoNotOptimize(row_count);
    benchmark::DoNotOptimize(res.count);
    libvroom::gpu::gpu_cleanup(res);
  }

  state.SetBytesProcessed(static_cast<int64_t>(state.iterations() * csv.size()));
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

// =============================================================================
// Benchmark registrations
// =============================================================================

// Args: (data_size_bytes, with_quotes)
// Sizes: 1MB, 10MB, 50MB, 100MB, 250MB

// --- CPU Baseline ---
BENCHMARK(BM_CpuFindBoundaries)
    ->Args({1 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 0})
    ->Args({50 * 1024 * 1024, 0})
    ->Args({100 * 1024 * 1024, 0})
    ->Args({250 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 1})
    ->Args({100 * 1024 * 1024, 1})
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_CpuCountRows)
    ->Args({1 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 0})
    ->Args({50 * 1024 * 1024, 0})
    ->Args({100 * 1024 * 1024, 0})
    ->Args({250 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 1})
    ->Args({100 * 1024 * 1024, 1})
    ->Unit(benchmark::kMillisecond);

// --- GPU Full ---
BENCHMARK(BM_GpuFindBoundaries)
    ->Args({1 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 0})
    ->Args({50 * 1024 * 1024, 0})
    ->Args({100 * 1024 * 1024, 0})
    ->Args({250 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 1})
    ->Args({100 * 1024 * 1024, 1})
    ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_GpuFindBoundariesDetailed)
    ->Args({10 * 1024 * 1024, 0})
    ->Args({100 * 1024 * 1024, 0})
    ->Args({250 * 1024 * 1024, 0})
    ->Unit(benchmark::kMillisecond);

// --- Hybrid GPU+CPU ---
BENCHMARK(BM_HybridGpuCpu)
    ->Args({1 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 0})
    ->Args({50 * 1024 * 1024, 0})
    ->Args({100 * 1024 * 1024, 0})
    ->Args({250 * 1024 * 1024, 0})
    ->Args({10 * 1024 * 1024, 1})
    ->Args({100 * 1024 * 1024, 1})
    ->Unit(benchmark::kMillisecond);

} // namespace
