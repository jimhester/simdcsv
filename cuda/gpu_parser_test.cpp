#include "libvroom/gpu_parser.h"

#include "csv_gpu.cuh"

#include <gtest/gtest.h>
#include <iostream>
#include <string>
#include <vector>

namespace libvroom {
namespace gpu {
namespace {

TEST(GpuParserTest, QueryGpuInfo) {
  GpuInfo info = query_gpu_info();
  if (info.cuda_available) {
    EXPECT_GT(info.device_count, 0);
    EXPECT_GT(info.total_memory, 0u);
    EXPECT_GT(info.sm_count, 0);
    std::cout << "GPU detected: " << info.device_name << std::endl;
    std::cout << "  Compute: " << info.compute_capability_major << "."
              << info.compute_capability_minor << std::endl;
  } else {
    std::cout << "No CUDA GPU detected" << std::endl;
  }
}

TEST(GpuParserTest, GpuInfoString) {
  std::string info = gpu_info_string();
  EXPECT_FALSE(info.empty());
}

TEST(GpuParserTest, CudaAvailable) {
  bool available = cuda_available();
  std::cout << "CUDA available: " << (available ? "yes" : "no") << std::endl;
}

TEST(GpuParserTest, MinGpuFileSize) {
  size_t min_size = min_gpu_file_size();
  EXPECT_GE(min_size, 1024 * 1024u);
  EXPECT_LE(min_size, 1024 * 1024 * 1024u);
}

class GpuParserFixture : public ::testing::Test {
protected:
  void SetUp() override {
    info_ = query_gpu_info();
    if (!info_.cuda_available) {
      GTEST_SKIP() << "CUDA not available";
    }
  }
  GpuInfo info_;
};

TEST_F(GpuParserFixture, EmptyInput) {
  GpuParseConfig config;
  GpuIndexResult res = gpu_find_field_boundaries("", 0, config);
  ASSERT_TRUE(res.success);
  EXPECT_EQ(res.count, 0u);
  EXPECT_EQ(res.num_lines, 0u);
  gpu_cleanup(res);
}

TEST_F(GpuParserFixture, SimpleCSV) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n";

  GpuParseConfig config;
  config.delimiter = ',';
  config.handle_quotes = false;

  GpuTimings timings;
  GpuIndexResult res = gpu_find_field_boundaries(csv.data(), csv.size(), config, &timings);

  ASSERT_TRUE(res.success) << res.error_message;
  EXPECT_EQ(res.num_lines, 3u);
  EXPECT_EQ(res.count, 9u); // 6 commas + 3 newlines

  for (uint32_t i = 1; i < res.count; i++) {
    EXPECT_LE(res.positions[i - 1], res.positions[i]);
  }

  gpu_cleanup(res);
}

TEST_F(GpuParserFixture, QuotedCSV) {
  std::string csv = "name,value\n\"hello, world\",123\n\"foo\",456\n";

  GpuParseConfig config;
  config.delimiter = ',';
  config.quote_char = '"';
  config.handle_quotes = true;

  GpuIndexResult res = gpu_find_field_boundaries(csv.data(), csv.size(), config);

  ASSERT_TRUE(res.success) << res.error_message;
  EXPECT_EQ(res.num_lines, 3u);
  // comma inside "hello, world" should be excluded
  // Row 0: name,value\n => 1 comma + 1 newline = 2
  // Row 1: "hello, world",123\n => 1 comma (outside) + 1 newline = 2
  // Row 2: "foo",456\n => 1 comma + 1 newline = 2
  // Total = 6
  EXPECT_EQ(res.count, 6u);

  gpu_cleanup(res);
}

TEST_F(GpuParserFixture, GpuCsvIndexBuild) {
  std::string csv = "col1,col2,col3\nval1,val2,val3\nval4,val5,val6\n";

  GpuCsvIndex idx;
  EXPECT_FALSE(idx.is_valid());

  bool ok = idx.build(csv.data(), csv.size(), ',', '"', false);
  ASSERT_TRUE(ok) << idx.error();
  EXPECT_TRUE(idx.is_valid());
  EXPECT_EQ(idx.num_lines(), 3u);
  EXPECT_GT(idx.num_fields(), 0u);
  EXPECT_FALSE(idx.positions().empty());
}

TEST_F(GpuParserFixture, MoveSemantics) {
  std::string csv = "a,b\n1,2\n";

  GpuCsvIndex idx1;
  ASSERT_TRUE(idx1.build(csv.data(), csv.size()));
  EXPECT_TRUE(idx1.is_valid());

  GpuCsvIndex idx2(std::move(idx1));
  EXPECT_FALSE(idx1.is_valid()); // NOLINT
  EXPECT_TRUE(idx2.is_valid());

  GpuCsvIndex idx3;
  idx3 = std::move(idx2);
  EXPECT_FALSE(idx2.is_valid()); // NOLINT
  EXPECT_TRUE(idx3.is_valid());
}

TEST_F(GpuParserFixture, ShouldUseGpu) {
  EXPECT_FALSE(should_use_gpu(1024, info_));
  EXPECT_FALSE(should_use_gpu(1024 * 1024, info_));

  if (info_.free_memory > 200 * 1024 * 1024) {
    EXPECT_TRUE(should_use_gpu(50 * 1024 * 1024, info_));
  }
}

} // namespace
} // namespace gpu
} // namespace libvroom
