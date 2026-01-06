// Benchmarks intentionally test deprecated two_pass methods for performance comparison
#include "two_pass.h"
LIBVROOM_SUPPRESS_DEPRECATION_START

#include <benchmark/benchmark.h>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"

// Global variables for shared test data
std::map<std::string, std::basic_string_view<uint8_t>> test_data;
libvroom::two_pass* global_parser = nullptr;

// Initialize test data and parser
static void InitializeBenchmarkData() {
  if (global_parser == nullptr) {
    global_parser = new libvroom::two_pass();
  }
  
  // Load common test files if they exist
  std::vector<std::string> test_files = {
    "benchmark/data/basic/simple.csv",
    "benchmark/data/basic/many_rows.csv",
    "benchmark/data/basic/wide_columns.csv",
    "test/data/basic/simple.csv",
    "test/data/basic/many_rows.csv", 
    "test/data/basic/wide_columns.csv"
  };
  
  for (const auto& file : test_files) {
    try {
      auto data = get_corpus(file.c_str(), LIBVROOM_PADDING);
      test_data[file] = data;
    } catch (...) {
      // File doesn't exist, skip it
    }
  }
}

// Cleanup function
static void CleanupBenchmarkData() {
  for (auto& pair : test_data) {
    aligned_free((void*)pair.second.data());
  }
  test_data.clear();
  delete global_parser;
  global_parser = nullptr;
}

LIBVROOM_SUPPRESS_DEPRECATION_END

BENCHMARK_MAIN();