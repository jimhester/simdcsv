/**
 * @file transpose_benchmarks.cpp
 * @brief Benchmarks for transposing row-major indices to column-major.
 *
 * Part of #599 - evaluating index layout strategies.
 * Measures the cost of transposing flat_indexes[row * cols + col]
 * to col_indexes[col * rows + row].
 */

#include "common_defs.h"
#include "mem_util.h"

#include <benchmark/benchmark.h>
#include <cstdint>
#include <cstring>
#include <thread>
#include <vector>

/**
 * @brief Single-threaded transpose from row-major to column-major.
 *
 * Input:  row_major[row * cols + col] = value for (row, col)
 * Output: col_major[col * rows + row] = value for (row, col)
 */
static void transpose_single_threaded(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                      size_t cols) {
  for (size_t row = 0; row < rows; ++row) {
    for (size_t col = 0; col < cols; ++col) {
      col_major[col * rows + row] = row_major[row * cols + col];
    }
  }
}

/**
 * @brief Multi-threaded transpose from row-major to column-major.
 *
 * Parallelizes by columns - each thread handles a contiguous range of columns.
 * This provides good cache locality for the output (each thread writes to
 * contiguous memory in its column range).
 */
static void transpose_multi_threaded(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                                     size_t cols, size_t n_threads) {
  if (n_threads <= 1) {
    transpose_single_threaded(row_major, col_major, rows, cols);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  // Divide columns among threads
  size_t cols_per_thread = (cols + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t col_start = t * cols_per_thread;
    size_t col_end = std::min(col_start + cols_per_thread, cols);

    if (col_start >= cols)
      break;

    threads.emplace_back([=]() {
      for (size_t col = col_start; col < col_end; ++col) {
        for (size_t row = 0; row < rows; ++row) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

/**
 * @brief Blocked transpose for better cache utilization.
 *
 * Processes the matrix in blocks that fit in L1/L2 cache.
 * Block size of 64 is chosen to fit within typical 32KB L1 cache:
 * 64 * 64 * 8 bytes = 32KB
 */
static void transpose_blocked(const uint64_t* row_major, uint64_t* col_major, size_t rows,
                              size_t cols, size_t block_size = 64) {
  for (size_t row_block = 0; row_block < rows; row_block += block_size) {
    for (size_t col_block = 0; col_block < cols; col_block += block_size) {
      // Process one block
      size_t row_end = std::min(row_block + block_size, rows);
      size_t col_end = std::min(col_block + block_size, cols);

      for (size_t row = row_block; row < row_end; ++row) {
        for (size_t col = col_block; col < col_end; ++col) {
          col_major[col * rows + row] = row_major[row * cols + col];
        }
      }
    }
  }
}

/**
 * @brief Multi-threaded blocked transpose.
 */
static void transpose_blocked_multi_threaded(const uint64_t* row_major, uint64_t* col_major,
                                             size_t rows, size_t cols, size_t n_threads,
                                             size_t block_size = 64) {
  if (n_threads <= 1) {
    transpose_blocked(row_major, col_major, rows, cols, block_size);
    return;
  }

  std::vector<std::thread> threads;
  threads.reserve(n_threads);

  // Divide column blocks among threads
  size_t n_col_blocks = (cols + block_size - 1) / block_size;
  size_t blocks_per_thread = (n_col_blocks + n_threads - 1) / n_threads;

  for (size_t t = 0; t < n_threads; ++t) {
    size_t block_start = t * blocks_per_thread;
    size_t block_end = std::min(block_start + blocks_per_thread, n_col_blocks);

    if (block_start >= n_col_blocks)
      break;

    threads.emplace_back([=]() {
      for (size_t cb = block_start; cb < block_end; ++cb) {
        size_t col_block = cb * block_size;
        size_t col_end = std::min(col_block + block_size, cols);

        for (size_t row_block = 0; row_block < rows; row_block += block_size) {
          size_t row_end = std::min(row_block + block_size, rows);

          for (size_t row = row_block; row < row_end; ++row) {
            for (size_t col = col_block; col < col_end; ++col) {
              col_major[col * rows + row] = row_major[row * cols + col];
            }
          }
        }
      }
    });
  }

  for (auto& t : threads) {
    t.join();
  }
}

// =============================================================================
// Benchmarks
// =============================================================================

/**
 * @brief BM_TransposeSingleThreaded - Measure single-threaded transpose throughput.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols
 */
static void BM_TransposeSingleThreaded(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t total_elements = rows * cols;

  // Allocate row-major and column-major arrays
  auto row_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));
  auto col_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    aligned_free(row_major);
    aligned_free(col_major);
    return;
  }

  // Initialize row-major with sequential values (simulating byte positions)
  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10; // Simulate byte positions
  }

  for (auto _ : state) {
    transpose_single_threaded(row_major, col_major, rows, cols);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  // Report metrics
  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2; // read + write
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);
  state.counters["MemoryMB"] =
      static_cast<double>(total_elements * sizeof(uint64_t) * 2) / (1024.0 * 1024.0);

  aligned_free(row_major);
  aligned_free(col_major);
}

/**
 * @brief BM_TransposeMultiThreaded - Measure multi-threaded transpose throughput.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = threads
 */
static void BM_TransposeMultiThreaded(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t n_threads = static_cast<size_t>(state.range(2));
  size_t total_elements = rows * cols;

  auto row_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));
  auto col_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    aligned_free(row_major);
    aligned_free(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  for (auto _ : state) {
    transpose_multi_threaded(row_major, col_major, rows, cols, n_threads);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  aligned_free(row_major);
  aligned_free(col_major);
}

/**
 * @brief BM_TransposeBlocked - Measure blocked transpose throughput.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols
 */
static void BM_TransposeBlocked(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t total_elements = rows * cols;

  auto row_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));
  auto col_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    aligned_free(row_major);
    aligned_free(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  for (auto _ : state) {
    transpose_blocked(row_major, col_major, rows, cols);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  aligned_free(row_major);
  aligned_free(col_major);
}

/**
 * @brief BM_TransposeBlockedMultiThreaded - Measure blocked multi-threaded transpose.
 *
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = threads
 */
static void BM_TransposeBlockedMultiThreaded(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  size_t n_threads = static_cast<size_t>(state.range(2));
  size_t total_elements = rows * cols;

  auto row_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));
  auto col_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    aligned_free(row_major);
    aligned_free(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  for (auto _ : state) {
    transpose_blocked_multi_threaded(row_major, col_major, rows, cols, n_threads);
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Threads"] = static_cast<double>(n_threads);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  aligned_free(row_major);
  aligned_free(col_major);
}

/**
 * @brief BM_TransposeScaling - Compare transpose methods at different scales.
 *
 * Measures single-threaded, multi-threaded (4 threads), and blocked transpose.
 * Parameters: state.range(0) = rows, state.range(1) = cols, state.range(2) = method
 * Method: 0 = single-threaded, 1 = multi-threaded (4), 2 = blocked, 3 = blocked multi-threaded (4)
 */
static void BM_TransposeScaling(benchmark::State& state) {
  size_t rows = static_cast<size_t>(state.range(0));
  size_t cols = static_cast<size_t>(state.range(1));
  int method = static_cast<int>(state.range(2));
  size_t total_elements = rows * cols;
  size_t n_threads = 4;

  auto row_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));
  auto col_major = static_cast<uint64_t*>(aligned_malloc(64, total_elements * sizeof(uint64_t)));

  if (!row_major || !col_major) {
    state.SkipWithError("Failed to allocate memory");
    aligned_free(row_major);
    aligned_free(col_major);
    return;
  }

  for (size_t i = 0; i < total_elements; ++i) {
    row_major[i] = i * 10;
  }

  const char* method_name = "unknown";
  for (auto _ : state) {
    switch (method) {
    case 0:
      transpose_single_threaded(row_major, col_major, rows, cols);
      method_name = "single";
      break;
    case 1:
      transpose_multi_threaded(row_major, col_major, rows, cols, n_threads);
      method_name = "multi4";
      break;
    case 2:
      transpose_blocked(row_major, col_major, rows, cols);
      method_name = "blocked";
      break;
    case 3:
      transpose_blocked_multi_threaded(row_major, col_major, rows, cols, n_threads);
      method_name = "blocked_multi4";
      break;
    }
    benchmark::DoNotOptimize(col_major);
    benchmark::ClobberMemory();
  }

  // Suppress unused variable warning
  (void)method_name;

  size_t bytes_processed = total_elements * sizeof(uint64_t) * 2;
  state.SetBytesProcessed(static_cast<int64_t>(bytes_processed * state.iterations()));
  state.counters["Rows"] = static_cast<double>(rows);
  state.counters["Cols"] = static_cast<double>(cols);
  state.counters["Method"] = static_cast<double>(method);
  state.counters["Elements"] = static_cast<double>(total_elements);
  state.counters["Elements/s"] = benchmark::Counter(static_cast<double>(total_elements),
                                                    benchmark::Counter::kIsIterationInvariantRate);

  aligned_free(row_major);
  aligned_free(col_major);
}

// =============================================================================
// Benchmark Registration
// =============================================================================

// Test matrix from issue #600:
// Rows: 10K, 100K, 1M, 10M
// Cols: 10, 100, 500

// Single-threaded transpose - full test matrix
BENCHMARK(BM_TransposeSingleThreaded)
    ->Args({10000, 10})     // 10K rows, 10 cols
    ->Args({10000, 100})    // 10K rows, 100 cols
    ->Args({10000, 500})    // 10K rows, 500 cols
    ->Args({100000, 10})    // 100K rows, 10 cols
    ->Args({100000, 100})   // 100K rows, 100 cols
    ->Args({100000, 500})   // 100K rows, 500 cols
    ->Args({1000000, 10})   // 1M rows, 10 cols
    ->Args({1000000, 100})  // 1M rows, 100 cols
    ->Args({1000000, 500})  // 1M rows, 500 cols
    ->Args({10000000, 10})  // 10M rows, 10 cols
    ->Args({10000000, 100}) // 10M rows, 100 cols
    ->Unit(benchmark::kMillisecond);

// Multi-threaded transpose - compare thread counts
// Args: rows, cols, threads
BENCHMARK(BM_TransposeMultiThreaded)
    // 100K rows, 100 cols - varying threads
    ->Args({100000, 100, 1})
    ->Args({100000, 100, 2})
    ->Args({100000, 100, 4})
    ->Args({100000, 100, 8})
    // 1M rows, 100 cols - varying threads
    ->Args({1000000, 100, 1})
    ->Args({1000000, 100, 2})
    ->Args({1000000, 100, 4})
    ->Args({1000000, 100, 8})
    // 1M rows, 500 cols - varying threads
    ->Args({1000000, 500, 1})
    ->Args({1000000, 500, 2})
    ->Args({1000000, 500, 4})
    ->Args({1000000, 500, 8})
    ->Unit(benchmark::kMillisecond);

// Blocked transpose - full test matrix
BENCHMARK(BM_TransposeBlocked)
    ->Args({10000, 10})
    ->Args({10000, 100})
    ->Args({10000, 500})
    ->Args({100000, 10})
    ->Args({100000, 100})
    ->Args({100000, 500})
    ->Args({1000000, 10})
    ->Args({1000000, 100})
    ->Args({1000000, 500})
    ->Args({10000000, 10})
    ->Args({10000000, 100})
    ->Unit(benchmark::kMillisecond);

// Blocked multi-threaded transpose
BENCHMARK(BM_TransposeBlockedMultiThreaded)
    ->Args({100000, 100, 4})
    ->Args({1000000, 100, 4})
    ->Args({1000000, 500, 4})
    ->Args({10000000, 10, 4})
    ->Args({10000000, 100, 4})
    ->Unit(benchmark::kMillisecond);

// Scaling comparison - all methods at key sizes
// Args: rows, cols, method (0=single, 1=multi4, 2=blocked, 3=blocked_multi4)
BENCHMARK(BM_TransposeScaling)
    // 100K x 100 - all methods
    ->Args({100000, 100, 0})
    ->Args({100000, 100, 1})
    ->Args({100000, 100, 2})
    ->Args({100000, 100, 3})
    // 1M x 100 - all methods
    ->Args({1000000, 100, 0})
    ->Args({1000000, 100, 1})
    ->Args({1000000, 100, 2})
    ->Args({1000000, 100, 3})
    // 1M x 500 - all methods
    ->Args({1000000, 500, 0})
    ->Args({1000000, 500, 1})
    ->Args({1000000, 500, 2})
    ->Args({1000000, 500, 3})
    ->Unit(benchmark::kMillisecond);
