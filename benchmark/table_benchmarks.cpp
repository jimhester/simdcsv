/**
 * @file table_benchmarks.cpp
 * @brief Benchmarks comparing CsvReader::read_all() vs Table creation vs Arrow export.
 *
 * Measures the incremental cost of:
 * 1. BM_CsvReaderOnly - CsvReader open + read_all (baseline parse to column builders)
 * 2. BM_CsvReaderToTable - Same + Table::from_parsed_chunks (Table creation overhead)
 * 3. BM_CsvReaderToArrowStream - Same + Table + full Arrow stream export/consume
 *
 * Uses generated CSV files written to disk once at setup time.
 */

#include "libvroom.h"
#include "libvroom/table.h"

#include <benchmark/benchmark.h>
#include <cstdio>
#include <fstream>
#include <map>
#include <random>
#include <sstream>
#include <string>
#include <vector>

namespace {

/// Generate CSV content with mixed types (int, double, string columns)
std::string generate_typed_csv(size_t target_rows, size_t num_int_cols, size_t num_dbl_cols,
                               size_t num_str_cols) {
  std::mt19937 rng(42);
  std::uniform_int_distribution<int> int_dist(0, 99999);
  std::uniform_real_distribution<double> dbl_dist(-1000.0, 1000.0);

  const std::vector<std::string> words = {"hello", "world", "foo", "bar",
                                          "test",  "data",  "csv", "benchmark"};
  std::uniform_int_distribution<size_t> word_dist(0, words.size() - 1);

  size_t total_cols = num_int_cols + num_dbl_cols + num_str_cols;
  std::ostringstream oss;

  // Header
  for (size_t c = 0; c < total_cols; ++c) {
    if (c > 0)
      oss << ',';
    if (c < num_int_cols)
      oss << "int" << c;
    else if (c < num_int_cols + num_dbl_cols)
      oss << "dbl" << (c - num_int_cols);
    else
      oss << "str" << (c - num_int_cols - num_dbl_cols);
  }
  oss << '\n';

  // Data rows
  for (size_t r = 0; r < target_rows; ++r) {
    for (size_t c = 0; c < total_cols; ++c) {
      if (c > 0)
        oss << ',';
      if (c < num_int_cols) {
        oss << int_dist(rng);
      } else if (c < num_int_cols + num_dbl_cols) {
        oss << dbl_dist(rng);
      } else {
        oss << words[word_dist(rng)];
      }
    }
    oss << '\n';
  }

  return oss.str();
}

/// Key for cached CSV files: (rows, int_cols, dbl_cols, str_cols)
using CsvKey = std::tuple<size_t, size_t, size_t, size_t>;

/// Cache of temp file paths
std::map<CsvKey, std::string> csv_file_cache;

/// Get or create a temp CSV file with the given dimensions
const std::string& get_or_create_csv_file(size_t rows, size_t int_cols, size_t dbl_cols,
                                          size_t str_cols) {
  auto key = CsvKey{rows, int_cols, dbl_cols, str_cols};
  auto it = csv_file_cache.find(key);
  if (it != csv_file_cache.end()) {
    return it->second;
  }

  std::string csv = generate_typed_csv(rows, int_cols, dbl_cols, str_cols);

  // Write to a temp file
  char tmpname[] = "/tmp/libvroom_bench_XXXXXX";
  int fd = mkstemp(tmpname);
  if (fd < 0) {
    static std::string empty;
    return empty;
  }

  FILE* f = fdopen(fd, "w");
  fwrite(csv.data(), 1, csv.size(), f);
  fclose(f);

  auto result = csv_file_cache.emplace(key, std::string(tmpname));
  return result.first->second;
}

/// Cleanup temp files at program exit
struct TempFileCleanup {
  ~TempFileCleanup() {
    for (auto& [key, path] : csv_file_cache) {
      std::remove(path.c_str());
    }
  }
} cleanup_guard;

} // anonymous namespace

// ============================================================================
// BM_CsvReaderOnly - Parse CSV to column builders (baseline)
// ============================================================================
static void BM_CsvReaderOnly(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  // Split cols evenly: 1/3 int, 1/3 double, 1/3 string
  size_t int_cols = cols / 3;
  size_t dbl_cols = cols / 3;
  size_t str_cols = cols - int_cols - dbl_cols;

  const auto& path = get_or_create_csv_file(rows, int_cols, dbl_cols, str_cols);
  if (path.empty()) {
    state.SkipWithError("Failed to create temp file");
    return;
  }

  libvroom::CsvOptions opts;

  for (auto _ : state) {
    libvroom::CsvReader reader(opts);
    auto open_result = reader.open(path);
    if (!open_result.ok) {
      state.SkipWithError(open_result.error.c_str());
      return;
    }
    auto read_result = reader.read_all();
    if (!read_result.ok) {
      state.SkipWithError(read_result.error.c_str());
      return;
    }
    benchmark::DoNotOptimize(read_result.value);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

// ============================================================================
// BM_CsvReaderToTable - Parse CSV + create Table (measures Table overhead)
// ============================================================================
static void BM_CsvReaderToTable(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  size_t int_cols = cols / 3;
  size_t dbl_cols = cols / 3;
  size_t str_cols = cols - int_cols - dbl_cols;

  const auto& path = get_or_create_csv_file(rows, int_cols, dbl_cols, str_cols);
  if (path.empty()) {
    state.SkipWithError("Failed to create temp file");
    return;
  }

  libvroom::CsvOptions opts;

  for (auto _ : state) {
    auto table = libvroom::read_csv_to_table(path, opts);
    benchmark::DoNotOptimize(table);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

// ============================================================================
// BM_CsvReaderToArrowStream - Parse CSV + Table + full Arrow stream consume
// ============================================================================
static void BM_CsvReaderToArrowStream(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));

  size_t int_cols = cols / 3;
  size_t dbl_cols = cols / 3;
  size_t str_cols = cols - int_cols - dbl_cols;

  const auto& path = get_or_create_csv_file(rows, int_cols, dbl_cols, str_cols);
  if (path.empty()) {
    state.SkipWithError("Failed to create temp file");
    return;
  }

  libvroom::CsvOptions opts;

  for (auto _ : state) {
    auto table = libvroom::read_csv_to_table(path, opts);

    // Export and consume the Arrow stream (simulating what R/Python would do)
    libvroom::ArrowArrayStream stream;
    table->export_to_stream(&stream);

    // Get schema
    libvroom::ArrowSchema schema;
    int rc = stream.get_schema(&stream, &schema);
    benchmark::DoNotOptimize(rc);

    // Get the single batch
    libvroom::ArrowArray batch;
    rc = stream.get_next(&stream, &batch);
    benchmark::DoNotOptimize(rc);

    // Verify end of stream
    libvroom::ArrowArray end;
    rc = stream.get_next(&stream, &end);
    benchmark::DoNotOptimize(rc);

    // Release everything
    if (schema.release)
      schema.release(&schema);
    if (batch.release)
      batch.release(&batch);
    stream.release(&stream);
  }

  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
}

// ============================================================================
// Benchmark Registration
// ============================================================================

// Test matrix: Rows x Cols
// Rows: 10K, 100K, 1M
// Cols: 9 (3+3+3), 30 (10+10+10)
static void TableBenchmarkArgs(benchmark::internal::Benchmark* b) {
  for (int64_t rows : {10000, 100000, 1000000}) {
    for (int64_t cols : {9, 30}) {
      b->Args({rows, cols});
    }
  }
}

BENCHMARK(BM_CsvReaderOnly)
    ->Apply(TableBenchmarkArgs)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_CsvReaderToTable)
    ->Apply(TableBenchmarkArgs)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
BENCHMARK(BM_CsvReaderToArrowStream)
    ->Apply(TableBenchmarkArgs)
    ->Unit(benchmark::kMillisecond)
    ->UseRealTime();
