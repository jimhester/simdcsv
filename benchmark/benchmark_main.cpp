#include <benchmark/benchmark.h>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"

// Global variables for shared test data
std::map<std::string, std::basic_string_view<uint8_t>> test_data;
simdcsv::two_pass* global_parser = nullptr;

// Initialize test data and parser
static void InitializeBenchmarkData() {
  if (global_parser == nullptr) {
    global_parser = new simdcsv::two_pass();
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
      auto data = get_corpus(file.c_str(), SIMDCSV_PADDING);
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

BENCHMARK_MAIN();