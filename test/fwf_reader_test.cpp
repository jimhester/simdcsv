/**
 * @file fwf_reader_test.cpp
 * @brief Fixed-width file reader tests.
 */

#include "libvroom.h"

#include "test_util.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>

class FwfReaderTest : public ::testing::Test {
protected:
  struct ParsedFwf {
    std::vector<std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>>> chunks;
    std::vector<libvroom::ColumnSchema> schema;
    size_t total_rows = 0;
  };

  ParsedFwf parseContent(const std::string& content, libvroom::FwfOptions opts) {
    test_util::TempCsvFile f(content, ".fwf");
    return parseFile(f.path(), opts);
  }

  ParsedFwf parseFile(const std::string& path, libvroom::FwfOptions opts) {
    libvroom::FwfReader reader(opts);
    auto open_result = reader.open(path);
    EXPECT_TRUE(open_result.ok) << "Failed to open: " << open_result.error;
    auto stream_result = reader.start_streaming();
    EXPECT_TRUE(stream_result.ok) << "Failed to start streaming: " << stream_result.error;

    ParsedFwf result;
    result.schema.assign(reader.schema().begin(), reader.schema().end());

    while (auto chunk = reader.next_chunk()) {
      result.total_rows += chunk->empty() ? 0 : (*chunk)[0]->size();
      result.chunks.push_back(std::move(*chunk));
    }
    return result;
  }

  std::string getValue(const ParsedFwf& parsed, size_t col, size_t row) {
    size_t row_offset = 0;
    for (const auto& chunk : parsed.chunks) {
      size_t chunk_rows = chunk[col]->size();
      if (row < row_offset + chunk_rows) {
        return test_util::getValue(chunk[col].get(), row - row_offset);
      }
      row_offset += chunk_rows;
    }
    ADD_FAILURE() << "Row " << row << " not found";
    return "";
  }

  bool isNull(const ParsedFwf& parsed, size_t col, size_t row) {
    size_t row_offset = 0;
    for (const auto& chunk : parsed.chunks) {
      size_t chunk_rows = chunk[col]->size();
      if (row < row_offset + chunk_rows) {
        return chunk[col]->null_bitmap().is_null(row - row_offset);
      }
      row_offset += chunk_rows;
    }
    ADD_FAILURE() << "Row " << row << " not found";
    return false;
  }
};

// ============================================================================
// BASIC FWF PARSING
// ============================================================================

TEST_F(FwfReaderTest, SimpleFixedWidth) {
  std::string content = "John 25  NYC \n"
                        "Jane 30  LA  \n"
                        "Bob  22  CHI \n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 5, 9};
  opts.col_ends = {5, 9, -1};
  opts.col_names = {"name", "age", "city"};

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 3u);
  ASSERT_EQ(result.schema.size(), 3u);
  EXPECT_EQ(result.schema[0].name, "name");
  EXPECT_EQ(result.schema[1].name, "age");
  EXPECT_EQ(result.schema[2].name, "city");

  EXPECT_EQ(getValue(result, 0, 0), "John");
  EXPECT_EQ(getValue(result, 1, 0), "25");
  EXPECT_EQ(getValue(result, 2, 0), "NYC");

  EXPECT_EQ(getValue(result, 0, 2), "Bob");
  EXPECT_EQ(getValue(result, 1, 2), "22");
  EXPECT_EQ(getValue(result, 2, 2), "CHI");
}

TEST_F(FwfReaderTest, TypeInference) {
  std::string content = "  1 2.5  true 2024-01-15\n"
                        "  2 3.7  false2024-06-30\n"
                        "  3 1.0  true 2024-12-25\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4, 9, 14};
  opts.col_ends = {4, 9, 14, -1};
  opts.col_names = {"int_col", "dbl_col", "bool_col", "date_col"};
  opts.guess_integer = true;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 3u);
  EXPECT_EQ(result.schema[0].type, libvroom::DataType::INT32);
  EXPECT_EQ(result.schema[1].type, libvroom::DataType::FLOAT64);
  EXPECT_EQ(result.schema[2].type, libvroom::DataType::BOOL);
  EXPECT_EQ(result.schema[3].type, libvroom::DataType::DATE);
}

TEST_F(FwfReaderTest, WhitespaceTrimming) {
  std::string content = "  hello   world \n"
                        "  foo     bar   \n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 10};
  opts.col_ends = {10, -1};
  opts.col_names = {"a", "b"};
  opts.trim_ws = true;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 0, 0), "hello");
  EXPECT_EQ(getValue(result, 1, 0), "world");
  EXPECT_EQ(getValue(result, 0, 1), "foo");
  EXPECT_EQ(getValue(result, 1, 1), "bar");
}

TEST_F(FwfReaderTest, NoWhitespaceTrimming) {
  std::string content = "  hello   world \n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 10};
  opts.col_ends = {10, -1};
  opts.col_names = {"a", "b"};
  opts.trim_ws = false;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 1u);
  EXPECT_EQ(getValue(result, 0, 0), "  hello   ");
  EXPECT_EQ(getValue(result, 1, 0), "world ");
}

TEST_F(FwfReaderTest, NullValueHandling) {
  std::string content = "  1 hello\n"
                        " NA      \n"
                        "  3 world\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 3u);
  EXPECT_FALSE(isNull(result, 0, 0));
  EXPECT_TRUE(isNull(result, 0, 1)); // "NA" is null
  EXPECT_TRUE(isNull(result, 1, 1)); // empty after trim is null
}

TEST_F(FwfReaderTest, CommentLines) {
  std::string content = "# header comment\n"
                        "  1 hello\n"
                        "# mid comment\n"
                        "  2 world\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};
  opts.comment = '#';

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 1, 0), "hello");
  EXPECT_EQ(getValue(result, 1, 1), "world");
}

TEST_F(FwfReaderTest, SkipEmptyRows) {
  std::string content = "  1 hello\n"
                        "\n"
                        "  2 world\n"
                        "\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};
  opts.skip_empty_rows = true;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
}

TEST_F(FwfReaderTest, SkipLines) {
  std::string content = "header line 1\n"
                        "header line 2\n"
                        "  1 hello\n"
                        "  2 world\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};
  opts.skip = 2;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 1, 0), "hello");
}

TEST_F(FwfReaderTest, MaxRows) {
  std::string content = "  1 aaa\n"
                        "  2 bbb\n"
                        "  3 ccc\n"
                        "  4 ddd\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};
  opts.max_rows = 2;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 1, 0), "aaa");
  EXPECT_EQ(getValue(result, 1, 1), "bbb");
}

TEST_F(FwfReaderTest, CRLFLineEndings) {
  std::string content = "  1 hello\r\n"
                        "  2 world\r\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 1, 0), "hello");
  EXPECT_EQ(getValue(result, 1, 1), "world");
}

TEST_F(FwfReaderTest, RaggedLastColumn) {
  std::string content = "  1 short\n"
                        "  2 a much longer value here\n"
                        "  3 x\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 3u);
  EXPECT_EQ(getValue(result, 1, 0), "short");
  EXPECT_EQ(getValue(result, 1, 1), "a much longer value here");
  EXPECT_EQ(getValue(result, 1, 2), "x");
}

TEST_F(FwfReaderTest, ShortLine) {
  std::string content = "AB\n"
                        "ABCDEF\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 2, 4};
  opts.col_ends = {2, 4, 6};
  opts.col_names = {"a", "b", "c"};

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 0, 0), "AB");
  EXPECT_TRUE(isNull(result, 1, 0)); // beyond line length
  EXPECT_TRUE(isNull(result, 2, 0));
  EXPECT_EQ(getValue(result, 0, 1), "AB");
  EXPECT_EQ(getValue(result, 1, 1), "CD");
  EXPECT_EQ(getValue(result, 2, 1), "EF");
}

TEST_F(FwfReaderTest, OpenFromBuffer) {
  std::string content = "  1 hello\n"
                        "  2 world\n";

  auto buffer = libvroom::AlignedBuffer::allocate(content.size());
  std::memcpy(buffer.data(), content.data(), content.size());

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};

  libvroom::FwfReader reader(opts);
  auto open_result = reader.open_from_buffer(std::move(buffer));
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto stream_result = reader.start_streaming();
  ASSERT_TRUE(stream_result.ok) << stream_result.error;

  size_t total = 0;
  while (auto chunk = reader.next_chunk()) {
    total += chunk->empty() ? 0 : (*chunk)[0]->size();
  }
  EXPECT_EQ(total, 2u);
}

TEST_F(FwfReaderTest, SkipPlusMaxRows) {
  std::string content = "header\n"
                        "  1 aaa\n"
                        "  2 bbb\n"
                        "  3 ccc\n"
                        "  4 ddd\n";

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};
  opts.skip = 1;
  opts.max_rows = 2;

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 1, 0), "aaa");
  EXPECT_EQ(getValue(result, 1, 1), "bbb");
}

TEST_F(FwfReaderTest, NoTrailingNewline) {
  std::string content = "  1 hello\n"
                        "  2 world"; // no trailing newline

  libvroom::FwfOptions opts;
  opts.col_starts = {0, 4};
  opts.col_ends = {4, -1};
  opts.col_names = {"num", "str"};

  auto result = parseContent(content, opts);
  ASSERT_EQ(result.total_rows, 2u);
  EXPECT_EQ(getValue(result, 1, 1), "world");
}
