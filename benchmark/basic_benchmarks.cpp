// Benchmarks intentionally test deprecated two_pass methods for performance comparison
#include "two_pass.h"
LIBVROOM_SUPPRESS_DEPRECATION_START

#include <benchmark/benchmark.h>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include <memory>

extern std::map<std::string, std::basic_string_view<uint8_t>> test_data;
extern libvroom::two_pass* global_parser;

// Basic parsing benchmark for different file sizes
static void BM_ParseFile(benchmark::State& state, const std::string& filename) {
  std::basic_string_view<uint8_t> data;
  
  // Try to load the file if not already loaded
  if (test_data.find(filename) == test_data.end()) {
    try {
      data = get_corpus(filename.c_str(), LIBVROOM_PADDING);
      test_data[filename] = data;
    } catch (const std::exception& e) {
      state.SkipWithError(("Failed to load " + filename + ": " + e.what()).c_str());
      return;
    }
  } else {
    data = test_data[filename];
  }
  
  if (!global_parser) {
    global_parser = new libvroom::two_pass();
  }
  
  int n_threads = static_cast<int>(state.range(0));
  libvroom::index result = global_parser->init(data.size(), n_threads);
  
  for (auto _ : state) {
    global_parser->parse(data.data(), result, data.size());
    benchmark::DoNotOptimize(result);
  }
  
  // Performance metrics are calculated automatically by Google Benchmark
  state.SetBytesProcessed(static_cast<int64_t>(data.size() * state.iterations()));
  state.counters["FileSize"] = static_cast<double>(data.size());
  state.counters["Threads"] = static_cast<double>(n_threads);
}

// Benchmark different thread counts
static void BM_ParseSimple_Threads(benchmark::State& state) {
  BM_ParseFile(state, "test/data/basic/simple.csv");
}
BENCHMARK(BM_ParseSimple_Threads)->RangeMultiplier(2)->Range(1, 16)->Unit(benchmark::kMillisecond);

static void BM_ParseManyRows_Threads(benchmark::State& state) {
  BM_ParseFile(state, "test/data/basic/many_rows.csv");
}
BENCHMARK(BM_ParseManyRows_Threads)->RangeMultiplier(2)->Range(1, 16)->Unit(benchmark::kMillisecond);

static void BM_ParseWideColumns_Threads(benchmark::State& state) {
  BM_ParseFile(state, "test/data/basic/wide_columns.csv");
}
BENCHMARK(BM_ParseWideColumns_Threads)->RangeMultiplier(2)->Range(1, 16)->Unit(benchmark::kMillisecond);

// Benchmark different file types
static void BM_ParseQuoted(benchmark::State& state) {
  BM_ParseFile(state, "test/data/quoted/quoted_fields.csv");
}
BENCHMARK(BM_ParseQuoted)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParseWithEmbeddedSeparators(benchmark::State& state) {
  BM_ParseFile(state, "test/data/quoted/embedded_separators.csv");
}
BENCHMARK(BM_ParseWithEmbeddedSeparators)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParseWithNewlines(benchmark::State& state) {
  BM_ParseFile(state, "test/data/quoted/newlines_in_quotes.csv");
}
BENCHMARK(BM_ParseWithNewlines)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

// Benchmark different separators
static void BM_ParseTabSeparated(benchmark::State& state) {
  BM_ParseFile(state, "test/data/separators/tab.csv");
}
BENCHMARK(BM_ParseTabSeparated)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParseSemicolonSeparated(benchmark::State& state) {
  BM_ParseFile(state, "test/data/separators/semicolon.csv");
}
BENCHMARK(BM_ParseSemicolonSeparated)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

static void BM_ParsePipeSeparated(benchmark::State& state) {
  BM_ParseFile(state, "test/data/separators/pipe.csv");
}
BENCHMARK(BM_ParsePipeSeparated)->Arg(1)->Arg(4)->Arg(8)->Unit(benchmark::kMillisecond);

// Memory allocation benchmark
static void BM_MemoryAllocation(benchmark::State& state) {
  size_t file_size = static_cast<size_t>(state.range(0));
  
  for (auto _ : state) {
    auto data = aligned_malloc(64, file_size + LIBVROOM_PADDING);
    benchmark::DoNotOptimize(data);
    aligned_free(data);
  }
  
  state.SetBytesProcessed(static_cast<int64_t>(file_size * state.iterations()));
}
BENCHMARK(BM_MemoryAllocation)
  ->Range(1024, 1024*1024*100) // 1KB to 100MB
  ->Unit(benchmark::kMicrosecond);

// Index creation benchmark
static void BM_IndexCreation(benchmark::State& state) {
  if (!global_parser) {
    global_parser = new libvroom::two_pass();
  }
  
  size_t file_size = static_cast<size_t>(state.range(0));
  int n_threads = static_cast<int>(state.range(1));
  
  for (auto _ : state) {
    auto result = global_parser->init(file_size, n_threads);
    benchmark::DoNotOptimize(result);
  }
  
  state.counters["FileSize"] = static_cast<double>(file_size);
  state.counters["Threads"] = static_cast<double>(n_threads);
}
BENCHMARK(BM_IndexCreation)
  ->Ranges({{1024, 1024*1024*100}, {1, 16}}) // File sizes 1KB-100MB, threads 1-16
  ->Unit(benchmark::kMicrosecond);