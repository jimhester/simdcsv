#include <benchmark/benchmark.h>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"
#include "error.h"
#include <sstream>
#include <string>

extern std::map<std::string, std::basic_string_view<uint8_t>> test_data;
extern simdcsv::two_pass* global_parser;

// Generate CSV data of specified size
static std::string generate_csv_data(size_t rows, size_t cols) {
  std::ostringstream oss;
  // Header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0) oss << ',';
    oss << "col" << c;
  }
  oss << '\n';
  // Data rows
  for (size_t r = 0; r < rows; ++r) {
    for (size_t c = 0; c < cols; ++c) {
      if (c > 0) oss << ',';
      oss << "value" << r << "_" << c;
    }
    oss << '\n';
  }
  return oss.str();
}

// Generate CSV with some malformed rows (missing fields)
static std::string generate_csv_with_errors(size_t rows, size_t cols, size_t error_rate) {
  std::ostringstream oss;
  // Header
  for (size_t c = 0; c < cols; ++c) {
    if (c > 0) oss << ',';
    oss << "col" << c;
  }
  oss << '\n';
  // Data rows
  for (size_t r = 0; r < rows; ++r) {
    // Every error_rate rows, create a malformed row
    size_t actual_cols = ((error_rate > 0) && (r % error_rate == 0)) ? (cols - 1) : cols;
    for (size_t c = 0; c < actual_cols; ++c) {
      if (c > 0) oss << ',';
      oss << "value" << r << "_" << c;
    }
    oss << '\n';
  }
  return oss.str();
}

// ============================================================================
// BENCHMARK: Compare error handling approaches
// ============================================================================

// Approach 0: No error handling (baseline)
static void BM_Parse_NoErrors(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = 10;
  int n_threads = static_cast<int>(state.range(1));

  std::string csv_data = generate_csv_data(rows, cols);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), n_threads);

  for (auto _ : state) {
    global_parser->parse(data, result, csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// Approach 1: Thread-local error collectors
static void BM_Parse_ThreadLocalErrors(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = 10;
  int n_threads = static_cast<int>(state.range(1));

  std::string csv_data = generate_csv_data(rows, cols);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), n_threads);

  for (auto _ : state) {
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    global_parser->parse_with_errors(data, result, csv_data.size(), errors);
    benchmark::DoNotOptimize(result);
    benchmark::DoNotOptimize(errors);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// Approach 3: Combined SIMD + multi-threaded error detection
static void BM_Parse_CombinedSIMD(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = 10;
  int n_threads = static_cast<int>(state.range(1));

  std::string csv_data = generate_csv_data(rows, cols);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), n_threads);

  for (auto _ : state) {
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    global_parser->parse_combined_with_errors(data, result, csv_data.size(), errors);
    benchmark::DoNotOptimize(result);
    benchmark::DoNotOptimize(errors);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// ============================================================================
// BENCHMARK: Error detection overhead with different error rates
// ============================================================================

static void BM_ErrorDetection_NoErrors(benchmark::State& state) {
  size_t rows = 10000;
  size_t cols = 10;
  int n_threads = static_cast<int>(state.range(0));
  int approach = static_cast<int>(state.range(1));

  std::string csv_data = generate_csv_data(rows, cols);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), n_threads);

  for (auto _ : state) {
    if (approach == 0) {
      // Baseline: no error handling
      global_parser->parse(data, result, csv_data.size());
    } else if (approach == 1) {
      // Thread-local errors
      simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
      global_parser->parse_with_errors(data, result, csv_data.size(), errors);
    } else {
      // Combined SIMD
      simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
      global_parser->parse_combined_with_errors(data, result, csv_data.size(), errors);
    }
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Approach"] = static_cast<double>(approach);
}

static void BM_ErrorDetection_WithErrors(benchmark::State& state) {
  size_t rows = 10000;
  size_t cols = 10;
  size_t error_rate = 100;  // 1% error rate
  int n_threads = static_cast<int>(state.range(0));
  int approach = static_cast<int>(state.range(1));

  std::string csv_data = generate_csv_with_errors(rows, cols, error_rate);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), n_threads);

  for (auto _ : state) {
    if (approach == 0) {
      // Baseline: no error handling
      global_parser->parse(data, result, csv_data.size());
    } else if (approach == 1) {
      // Thread-local errors
      simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
      global_parser->parse_with_errors(data, result, csv_data.size(), errors);
    } else {
      // Combined SIMD
      simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
      global_parser->parse_combined_with_errors(data, result, csv_data.size(), errors);
    }
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Approach"] = static_cast<double>(approach);
}

// ============================================================================
// BENCHMARK: Scalability with increasing data size
// ============================================================================

static void BM_Scalability_NoErrors(benchmark::State& state) {
  size_t size = static_cast<size_t>(state.range(0));
  size_t rows = size / 100;  // Approximate rows from size
  if (rows < 10) rows = 10;

  std::string csv_data = generate_csv_data(rows, 10);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), 4);

  for (auto _ : state) {
    global_parser->parse(data, result, csv_data.size());
    benchmark::DoNotOptimize(result);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["ActualSize"] = static_cast<double>(csv_data.size());
}

static void BM_Scalability_ThreadLocalErrors(benchmark::State& state) {
  size_t size = static_cast<size_t>(state.range(0));
  size_t rows = size / 100;
  if (rows < 10) rows = 10;

  std::string csv_data = generate_csv_data(rows, 10);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), 4);

  for (auto _ : state) {
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    global_parser->parse_with_errors(data, result, csv_data.size(), errors);
    benchmark::DoNotOptimize(result);
    benchmark::DoNotOptimize(errors);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["ActualSize"] = static_cast<double>(csv_data.size());
}

static void BM_Scalability_CombinedSIMD(benchmark::State& state) {
  size_t size = static_cast<size_t>(state.range(0));
  size_t rows = size / 100;
  if (rows < 10) rows = 10;

  std::string csv_data = generate_csv_data(rows, 10);
  auto data = reinterpret_cast<const uint8_t*>(csv_data.data());

  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }

  simdcsv::index result = global_parser->init(csv_data.size(), 4);

  for (auto _ : state) {
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    global_parser->parse_combined_with_errors(data, result, csv_data.size(), errors);
    benchmark::DoNotOptimize(result);
    benchmark::DoNotOptimize(errors);
  }

  state.SetBytesProcessed(static_cast<int64_t>(csv_data.size() * state.iterations()));
  state.counters["ActualSize"] = static_cast<double>(csv_data.size());
}

// ============================================================================
// BENCHMARK REGISTRATIONS
// ============================================================================

// Compare all approaches at different thread counts
// Format: (rows, threads)
BENCHMARK(BM_Parse_NoErrors)
  ->Args({1000, 1})
  ->Args({1000, 4})
  ->Args({10000, 1})
  ->Args({10000, 4})
  ->Args({100000, 1})
  ->Args({100000, 4})
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Parse_ThreadLocalErrors)
  ->Args({1000, 1})
  ->Args({1000, 4})
  ->Args({10000, 1})
  ->Args({10000, 4})
  ->Args({100000, 1})
  ->Args({100000, 4})
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Parse_CombinedSIMD)
  ->Args({1000, 1})
  ->Args({1000, 4})
  ->Args({10000, 1})
  ->Args({10000, 4})
  ->Args({100000, 1})
  ->Args({100000, 4})
  ->Unit(benchmark::kMillisecond);

// Compare overhead for error detection
// Format: (threads, approach) where approach: 0=baseline, 1=thread-local, 2=combined
BENCHMARK(BM_ErrorDetection_NoErrors)
  ->Args({1, 0})->Args({1, 1})->Args({1, 2})
  ->Args({4, 0})->Args({4, 1})->Args({4, 2})
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_ErrorDetection_WithErrors)
  ->Args({1, 0})->Args({1, 1})->Args({1, 2})
  ->Args({4, 0})->Args({4, 1})->Args({4, 2})
  ->Unit(benchmark::kMillisecond);

// Scalability tests
BENCHMARK(BM_Scalability_NoErrors)
  ->Range(1000, 10000000)  // 1KB to 10MB approximated
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Scalability_ThreadLocalErrors)
  ->Range(1000, 10000000)
  ->Unit(benchmark::kMillisecond);

BENCHMARK(BM_Scalability_CombinedSIMD)
  ->Range(1000, 10000000)
  ->Unit(benchmark::kMillisecond);
