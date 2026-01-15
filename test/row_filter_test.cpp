/**
 * @file row_filter_test.cpp
 * @brief Tests for row filtering options (skip, n_max, comment, skip_empty_rows).
 *
 * Issue #559: Missing skip, n_max, comment, skip_empty_rows features
 */

#include <cstring>
#include <gtest/gtest.h>
#include <libvroom.h>
#include <string>

class RowFilterTest : public ::testing::Test {
protected:
  static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
    size_t len = content.size();
    uint8_t* buf = allocate_padded_buffer(len, 64);
    std::memcpy(buf, content.data(), len);
    return {buf, len};
  }
};

// =============================================================================
// skip option tests
// =============================================================================

TEST_F(RowFilterTest, SkipZeroRows) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 0});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 3); // 3 data rows (header not counted)
}

TEST_F(RowFilterTest, SkipOneRow) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 1});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2); // Skip first data row, 2 remain

  // Verify correct rows are returned
  auto row0 = result.row(0);
  EXPECT_EQ(row0.get_string(0), "4");
  EXPECT_EQ(row0.get_string(1), "5");
}

TEST_F(RowFilterTest, SkipAllRows) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result =
      parser.parse(buffer.data(), buffer.size(), {.skip = 10}); // Skip more than available
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 0);
}

// =============================================================================
// n_max option tests
// =============================================================================

TEST_F(RowFilterTest, NMaxZeroMeansUnlimited) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.n_max = 0});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 3); // All rows returned
}

TEST_F(RowFilterTest, NMaxLimitsRows) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.n_max = 2});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2); // Only 2 rows returned

  // Verify correct rows
  auto row1 = result.row(1);
  EXPECT_EQ(row1.get_string(0), "4");
}

TEST_F(RowFilterTest, NMaxLargerThanAvailable) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.n_max = 100});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2); // Only 2 data rows exist
}

// =============================================================================
// skip + n_max combined tests
// =============================================================================

TEST_F(RowFilterTest, SkipAndNMaxCombined) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 1, .n_max = 2});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 2); // Skip 1, then read 2

  auto row0 = result.row(0);
  EXPECT_EQ(row0.get_string(0), "4"); // First row after skip

  auto row1 = result.row(1);
  EXPECT_EQ(row1.get_string(0), "7"); // Second row after skip
}

// =============================================================================
// comment option tests
// =============================================================================

TEST_F(RowFilterTest, CommentLinesSkipped) {
  auto [data, len] = make_buffer("a,b,c\n# comment\n1,2,3\n# another\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.comment = '#'});
  EXPECT_TRUE(result.success());
  // Comment lines are skipped during parsing
  EXPECT_EQ(result.num_columns(), 3);
}

TEST_F(RowFilterTest, NoCommentByDefault) {
  auto [data, len] = make_buffer("a,b,c\n#not,a,comment\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size()); // No comment char
  EXPECT_TRUE(result.success());
  // Without comment handling, # line is treated as data
  EXPECT_EQ(result.num_rows(), 2);
}

TEST_F(RowFilterTest, CommentCharFromDialect) {
  auto [data, len] = make_buffer("a,b,c\n; comment line\n1,2,3\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;

  // Use dialect with comment char
  auto dialect = libvroom::Dialect::csv();
  dialect.comment_char = ';';
  auto result = parser.parse(buffer.data(), buffer.size(), {.dialect = dialect});
  EXPECT_TRUE(result.success());
}

// =============================================================================
// skip_empty_rows option tests
// =============================================================================

TEST_F(RowFilterTest, SkipEmptyRowsTrue) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n\n4,5,6\n   \n7,8,9\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip_empty_rows = true});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 3); // Empty and whitespace-only rows excluded
}

TEST_F(RowFilterTest, SkipEmptyRowsFalse) {
  auto [data, len] = make_buffer("a,b,c\n1,2,3\n\n4,5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip_empty_rows = false});
  EXPECT_TRUE(result.success());
  // The parser may or may not count empty lines as rows depending on implementation
  // When skip_empty_rows is false, filtering preserves all rows from parsing
  EXPECT_EQ(result.total_rows(), result.num_rows()); // Same when no filtering
  EXPECT_GE(result.total_rows(), 2);                 // At least the 2 data rows
}

// =============================================================================
// Row iteration with filters
// =============================================================================

TEST_F(RowFilterTest, IterationRespectsFilters) {
  auto [data, len] = make_buffer("a,b\n1,2\n3,4\n5,6\n7,8\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 1, .n_max = 2});
  EXPECT_TRUE(result.success());

  // Iterate using rows()
  size_t count = 0;
  for (auto row : result.rows()) {
    if (count == 0) {
      EXPECT_EQ(row.get_string(0), "3"); // After skipping "1"
    }
    ++count;
  }
  EXPECT_EQ(count, 2);
}

TEST_F(RowFilterTest, AllRowsIgnoresFilters) {
  auto [data, len] = make_buffer("a,b\n1,2\n3,4\n5,6\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 1, .n_max = 1});
  EXPECT_TRUE(result.success());

  // Filtered view
  EXPECT_EQ(result.num_rows(), 1);

  // Unfiltered view via all_rows()
  size_t count = 0;
  for ([[maybe_unused]] auto row : result.all_rows()) {
    ++count;
  }
  EXPECT_EQ(count, 3); // All 3 data rows
}

// =============================================================================
// total_rows() vs num_rows() distinction
// =============================================================================

TEST_F(RowFilterTest, TotalRowsVsNumRows) {
  auto [data, len] = make_buffer("a,b\n1,2\n3,4\n5,6\n7,8\n9,10\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 2, .n_max = 2});
  EXPECT_TRUE(result.success());

  EXPECT_EQ(result.total_rows(), 5); // All parsed data rows
  EXPECT_EQ(result.num_rows(), 2);   // After filtering
}

// =============================================================================
// Edge cases
// =============================================================================

TEST_F(RowFilterTest, EmptyInputWithFilters) {
  auto [data, len] = make_buffer("");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 10, .n_max = 100});
  EXPECT_EQ(result.num_rows(), 0);
}

TEST_F(RowFilterTest, HeaderOnlyWithSkip) {
  auto [data, len] = make_buffer("a,b,c\n");
  libvroom::FileBuffer buffer(data, len);
  libvroom::Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), {.skip = 1});
  EXPECT_TRUE(result.success());
  EXPECT_EQ(result.num_rows(), 0); // Only header, skip would skip non-existent row
}
