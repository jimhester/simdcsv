#include "libvroom/gpu_parser.h"

#include "csv_gpu.cuh"

#include <benchmark/benchmark.h>
#include <string>

namespace {

std::string generate_csv(size_t target_size) {
  const size_t cols = 10;
  std::string csv;
  csv.reserve(target_size + 1024);

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
      csv += std::to_string(row * cols + c);
    }
    csv += '\n';
    row++;
  }
  return csv;
}

static void BM_GpuFindBoundaries(benchmark::State& state) {
  auto info = libvroom::gpu::query_gpu_info();
  if (!info.cuda_available) {
    state.SkipWithError("CUDA not available");
    return;
  }

  size_t data_size = state.range(0);
  std::string csv = generate_csv(data_size);

  libvroom::gpu::GpuParseConfig config;
  config.handle_quotes = false;

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

  state.SetBytesProcessed(state.iterations() * csv.size());
  state.SetLabel(std::to_string(csv.size() / (1024 * 1024)) + " MB");
}

BENCHMARK(BM_GpuFindBoundaries)
    ->Arg(1 * 1024 * 1024)
    ->Arg(10 * 1024 * 1024)
    ->Arg(100 * 1024 * 1024)
    ->Unit(benchmark::kMillisecond);

} // namespace
