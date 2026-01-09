#include "csv_gpu.cuh"

#include <gpu_parser.h>
#include <gtest/gtest.h>
#include <string>
#include <vector>

namespace vroom {
namespace gpu {
namespace {

// =============================================================================
// GPU Availability Tests
// =============================================================================

TEST(GpuParserTest, QueryGpuInfo) {
  GpuInfo info = query_gpu_info();

  // We can't assume CUDA is available, but the function should not crash
  if (info.cuda_available) {
    EXPECT_GT(info.device_count, 0);
    EXPECT_GT(info.total_memory, 0u);
    EXPECT_GT(info.sm_count, 0);
    std::cout << "GPU detected: " << info.device_name << std::endl;
    std::cout << "  Compute capability: " << info.compute_capability_major << "."
              << info.compute_capability_minor << std::endl;
    std::cout << "  SMs: " << info.sm_count << std::endl;
    std::cout << "  Memory: " << (info.total_memory / (1024 * 1024)) << " MB" << std::endl;
  } else {
    std::cout << "No CUDA GPU detected - skipping GPU-specific tests" << std::endl;
  }
}

TEST(GpuParserTest, GpuInfoString) {
  std::string info = gpu_info_string();
  EXPECT_FALSE(info.empty());
  std::cout << info << std::endl;
}

TEST(GpuParserTest, CudaAvailable) {
  // Just ensure the function doesn't crash
  bool available = cuda_available();
  std::cout << "CUDA available: " << (available ? "yes" : "no") << std::endl;
}

TEST(GpuParserTest, MinGpuFileSize) {
  size_t min_size = min_gpu_file_size();
  // Should be reasonable (at least 1MB, at most 1GB)
  EXPECT_GE(min_size, 1024 * 1024u);
  EXPECT_LE(min_size, 1024 * 1024 * 1024u);
}

// =============================================================================
// GPU Parsing Tests (only run if CUDA is available)
// =============================================================================

class GpuParserFixture : public ::testing::Test {
protected:
  void SetUp() override {
    info_ = query_gpu_info();
    if (!info_.cuda_available) {
      GTEST_SKIP() << "CUDA not available, skipping GPU tests";
    }
  }

  GpuInfo info_;
};

TEST_F(GpuParserFixture, SimpleCSVParsing) {
  // Simple CSV without quotes
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n";

  GpuParseConfig config;
  config.delimiter = ',';
  config.handle_quotes = false;

  GpuTimings timings;
  GpuParseResult result = parse_csv_gpu(csv.data(), csv.size(), config, &timings);

  ASSERT_TRUE(result.success) << "GPU parsing failed: " << result.error_message;
  EXPECT_EQ(result.num_lines, 3u); // 3 newlines

  std::cout << "Simple CSV parsing timings:" << std::endl;
  std::cout << "  H2D transfer: " << timings.h2d_transfer_ms << " ms" << std::endl;
  std::cout << "  Kernel exec:  " << timings.kernel_exec_ms << " ms" << std::endl;
  std::cout << "  D2H transfer: " << timings.d2h_transfer_ms << " ms" << std::endl;
  std::cout << "  Total:        " << timings.total_ms << " ms" << std::endl;

  // Verify newline positions
  std::vector<uint32_t> positions(result.num_lines);
  EXPECT_TRUE(copy_newline_positions_to_host(result, positions.data()));

  // Newlines should be at positions 5, 11, 17 (0-indexed)
  EXPECT_EQ(positions[0], 5u);  // After "a,b,c"
  EXPECT_EQ(positions[1], 11u); // After "1,2,3"
  EXPECT_EQ(positions[2], 17u); // After "4,5,6"

  free_gpu_result(result);
}

TEST_F(GpuParserFixture, QuotedCSVParsing) {
  // CSV with quoted fields containing delimiters
  std::string csv = "name,value\n\"hello, world\",123\n\"foo\",456\n";

  GpuParseConfig config;
  config.delimiter = ',';
  config.quote_char = '"';
  config.handle_quotes = true;

  GpuTimings timings;
  GpuParseResult result = parse_csv_gpu(csv.data(), csv.size(), config, &timings);

  ASSERT_TRUE(result.success) << "GPU parsing failed: " << result.error_message;
  EXPECT_EQ(result.num_lines, 3u);

  std::cout << "Quoted CSV parsing timings:" << std::endl;
  std::cout << "  H2D transfer: " << timings.h2d_transfer_ms << " ms" << std::endl;
  std::cout << "  Kernel exec:  " << timings.kernel_exec_ms << " ms" << std::endl;
  std::cout << "  D2H transfer: " << timings.d2h_transfer_ms << " ms" << std::endl;
  std::cout << "  Total:        " << timings.total_ms << " ms" << std::endl;

  // The comma inside "hello, world" should NOT be counted as a field separator
  // We should have: name|value, "hello, world"|123, "foo"|456
  // So 2 delimiters per line = 6 total (plus 3 newlines = 9 separators)
  // But the comma inside quotes is filtered, so we should have fewer
  EXPECT_GT(result.num_fields, 0u);

  free_gpu_result(result);
}

TEST_F(GpuParserFixture, GpuCsvIndexWrapper) {
  std::string csv = "col1,col2,col3\nval1,val2,val3\nval4,val5,val6\n";

  GpuCsvIndex index;
  EXPECT_FALSE(index.is_valid());

  bool success = index.parse(csv.data(), csv.size(), ',', '"', false);
  ASSERT_TRUE(success) << index.error_message();

  EXPECT_TRUE(index.is_valid());
  EXPECT_EQ(index.num_lines(), 3u);

  // Access positions (triggers D2H copy)
  const auto& positions = index.line_positions();
  ASSERT_EQ(positions.size(), 3u);

  std::cout << "GpuCsvIndex timing:" << std::endl;
  std::cout << "  H2D:    " << index.h2d_transfer_ms() << " ms" << std::endl;
  std::cout << "  Kernel: " << index.kernel_exec_ms() << " ms" << std::endl;
  std::cout << "  D2H:    " << index.d2h_transfer_ms() << " ms" << std::endl;
  std::cout << "  Total:  " << index.total_ms() << " ms" << std::endl;
}

TEST_F(GpuParserFixture, MoveSemantics) {
  std::string csv = "a,b\n1,2\n";

  GpuCsvIndex index1;
  ASSERT_TRUE(index1.parse(csv.data(), csv.size()));
  EXPECT_TRUE(index1.is_valid());

  // Move constructor
  GpuCsvIndex index2(std::move(index1));
  EXPECT_FALSE(index1.is_valid()); // NOLINT: testing moved-from state
  EXPECT_TRUE(index2.is_valid());

  // Move assignment
  GpuCsvIndex index3;
  index3 = std::move(index2);
  EXPECT_FALSE(index2.is_valid()); // NOLINT: testing moved-from state
  EXPECT_TRUE(index3.is_valid());
}

TEST_F(GpuParserFixture, ShouldUseGpu) {
  // Small data should not use GPU
  EXPECT_FALSE(should_use_gpu(1024, info_));        // 1 KB
  EXPECT_FALSE(should_use_gpu(1024 * 1024, info_)); // 1 MB

  // Large data should use GPU (if there's enough memory)
  if (info_.free_memory > 100 * 1024 * 1024) {
    EXPECT_TRUE(should_use_gpu(50 * 1024 * 1024, info_)); // 50 MB
  }
}

TEST_F(GpuParserFixture, EmptyInput) {
  std::string csv = "";

  GpuParseConfig config;
  GpuParseResult result = parse_csv_gpu(csv.data(), csv.size(), config, nullptr);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.num_lines, 0u);

  free_gpu_result(result);
}

TEST_F(GpuParserFixture, SingleLineNoNewline) {
  std::string csv = "a,b,c"; // No trailing newline

  GpuParseConfig config;
  config.handle_quotes = false;

  GpuParseResult result = parse_csv_gpu(csv.data(), csv.size(), config, nullptr);

  ASSERT_TRUE(result.success);
  EXPECT_EQ(result.num_lines, 0u); // No newlines found

  free_gpu_result(result);
}

TEST_F(GpuParserFixture, LargeRandomData) {
  // Generate larger test data
  const size_t num_rows = 10000;
  const size_t cols = 10;

  std::string csv;
  csv.reserve(num_rows * cols * 10); // Rough estimate

  // Header
  for (size_t c = 0; c < cols; c++) {
    if (c > 0)
      csv += ',';
    csv += "col" + std::to_string(c);
  }
  csv += '\n';

  // Data rows
  for (size_t r = 0; r < num_rows; r++) {
    for (size_t c = 0; c < cols; c++) {
      if (c > 0)
        csv += ',';
      csv += std::to_string(r * cols + c);
    }
    csv += '\n';
  }

  std::cout << "Large data test: " << csv.size() << " bytes, " << num_rows + 1 << " rows"
            << std::endl;

  GpuParseConfig config;
  config.handle_quotes = false;

  GpuTimings timings;
  GpuParseResult result = parse_csv_gpu(csv.data(), csv.size(), config, &timings);

  ASSERT_TRUE(result.success) << result.error_message;
  EXPECT_EQ(result.num_lines, num_rows + 1); // +1 for header

  double throughput_gbps = (csv.size() / (1024.0 * 1024.0 * 1024.0)) / (timings.total_ms / 1000.0);
  std::cout << "Large data performance:" << std::endl;
  std::cout << "  Total time: " << timings.total_ms << " ms" << std::endl;
  std::cout << "  Throughput: " << throughput_gbps << " GB/s" << std::endl;

  free_gpu_result(result);
}

} // namespace
} // namespace gpu
} // namespace vroom
