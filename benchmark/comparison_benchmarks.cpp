#include <benchmark/benchmark.h>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"
#include <fstream>
#include <sstream>
#include <string>
#include <vector>

extern std::map<std::string, std::basic_string_view<uint8_t>> test_data;
extern simdcsv::two_pass* global_parser;

// Simple CSV parser for comparison (naive implementation)
class NaiveCSVParser {
public:
  static std::vector<std::vector<std::string>> parse(const std::string& data) {
    std::vector<std::vector<std::string>> result;
    std::istringstream stream(data);
    std::string line;
    
    while (std::getline(stream, line)) {
      std::vector<std::string> row;
      std::istringstream line_stream(line);
      std::string field;
      
      while (std::getline(line_stream, field, ',')) {
        row.push_back(field);
      }
      result.push_back(row);
    }
    
    return result;
  }
};

// Stream-based CSV parser for comparison
class StreamCSVParser {
public:
  static size_t count_records(const std::string& data) {
    size_t count = 0;
    for (char c : data) {
      if (c == '\n') count++;
    }
    return count;
  }
  
  static size_t count_fields(const std::string& data) {
    size_t count = 0;
    for (char c : data) {
      if (c == ',' || c == '\n') count++;
    }
    return count;
  }
};

// Benchmark simdcsv vs naive parser
static void BM_simdcsv_vs_naive(benchmark::State& state, const std::string& filename) {
  std::basic_string_view<uint8_t> data;
  
  if (test_data.find(filename) == test_data.end()) {
    try {
      data = get_corpus(filename.c_str(), SIMDCSV_PADDING);
      test_data[filename] = data;
    } catch (const std::exception& e) {
      state.SkipWithError(("Failed to load " + filename + ": " + e.what()).c_str());
      return;
    }
  } else {
    data = test_data[filename];
  }
  
  if (!global_parser) {
    global_parser = new simdcsv::two_pass();
  }
  
  bool use_simdcsv = state.range(0) == 1;
  
  if (use_simdcsv) {
    simdcsv::index result = global_parser->init(data.size(), 1);
    
    for (auto _ : state) {
      global_parser->parse(data.data(), result, data.size());
      benchmark::DoNotOptimize(result);
    }
  } else {
    // Naive parser
    std::string str_data(reinterpret_cast<const char*>(data.data()), data.size());
    
    for (auto _ : state) {
      auto result = NaiveCSVParser::parse(str_data);
      benchmark::DoNotOptimize(result);
    }
  }
  
  state.SetBytesProcessed(static_cast<int64_t>(data.size() * state.iterations()));
  state.counters["Parser"] = use_simdcsv ? 1.0 : 0.0; // 1 = simdcsv, 0 = naive
}

static void BM_simdcsv_vs_naive_simple(benchmark::State& state) {
  BM_simdcsv_vs_naive(state, "test/data/basic/simple.csv");
}
BENCHMARK(BM_simdcsv_vs_naive_simple)
  ->Arg(0) // Naive parser
  ->Arg(1) // simdcsv
  ->Unit(benchmark::kMillisecond);

static void BM_simdcsv_vs_naive_many_rows(benchmark::State& state) {
  BM_simdcsv_vs_naive(state, "test/data/basic/many_rows.csv");
}
BENCHMARK(BM_simdcsv_vs_naive_many_rows)
  ->Arg(0) // Naive parser
  ->Arg(1) // simdcsv
  ->Unit(benchmark::kMillisecond);

// Benchmark different parsing approaches
static void BM_parsing_approaches(benchmark::State& state, const std::string& filename) {
  std::basic_string_view<uint8_t> data;
  
  if (test_data.find(filename) == test_data.end()) {
    try {
      data = get_corpus(filename.c_str(), SIMDCSV_PADDING);
      test_data[filename] = data;
    } catch (const std::exception& e) {
      state.SkipWithError(("Failed to load " + filename + ": " + e.what()).c_str());
      return;
    }
  } else {
    data = test_data[filename];
  }
  
  int approach = static_cast<int>(state.range(0));
  std::string str_data(reinterpret_cast<const char*>(data.data()), data.size());
  
  switch (approach) {
    case 0: { // Character-by-character counting
      for (auto _ : state) {
        size_t count = StreamCSVParser::count_records(str_data);
        benchmark::DoNotOptimize(count);
      }
      break;
    }
    case 1: { // Field counting
      for (auto _ : state) {
        size_t count = StreamCSVParser::count_fields(str_data);
        benchmark::DoNotOptimize(count);
      }
      break;
    }
    case 2: { // Full naive parsing
      for (auto _ : state) {
        auto result = NaiveCSVParser::parse(str_data);
        benchmark::DoNotOptimize(result);
      }
      break;
    }
    case 3: { // simdcsv indexing
      if (!global_parser) {
        global_parser = new simdcsv::two_pass();
      }
      simdcsv::index result = global_parser->init(data.size(), 1);
      
      for (auto _ : state) {
        global_parser->parse(data.data(), result, data.size());
        benchmark::DoNotOptimize(result);
      }
      break;
    }
  }
  
  state.SetBytesProcessed(static_cast<int64_t>(data.size() * state.iterations()));
  state.counters["Approach"] = static_cast<double>(approach);
}

static void BM_parsing_approaches_simple(benchmark::State& state) {
  BM_parsing_approaches(state, "test/data/basic/simple.csv");
}
BENCHMARK(BM_parsing_approaches_simple)
  ->DenseRange(0, 3, 1) // Test all 4 approaches
  ->Unit(benchmark::kMillisecond);

static void BM_parsing_approaches_quoted(benchmark::State& state) {
  BM_parsing_approaches(state, "test/data/quoted/quoted_fields.csv");
}
BENCHMARK(BM_parsing_approaches_quoted)
  ->DenseRange(0, 3, 1) // Test all 4 approaches
  ->Unit(benchmark::kMillisecond);

// Memory bandwidth benchmark
static void BM_memory_bandwidth(benchmark::State& state) {
  size_t size = static_cast<size_t>(state.range(0));
  auto data = static_cast<char*>(aligned_malloc(64, size));
  
  // Initialize data
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<char>(i % 256);
  }
  
  for (auto _ : state) {
    volatile char sum = 0;
    for (size_t i = 0; i < size; ++i) {
      sum += data[i];
    }
    benchmark::DoNotOptimize(sum);
  }
  
  state.SetBytesProcessed(static_cast<int64_t>(size * state.iterations()));
  
  // Google Benchmark calculates throughput automatically
  
  aligned_free(data);
}
BENCHMARK(BM_memory_bandwidth)
  ->Range(1024, 1024*1024*100) // 1KB to 100MB
  ->Unit(benchmark::kMillisecond);