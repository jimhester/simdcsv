/**
 * @file column_escape_test.cpp
 * @brief Tests for per-column escape sequence tracking.
 *
 * Tests the ColumnEscapeInfo functionality that enables zero-copy string
 * extraction for columns without escape sequences (96%+ of typical CSV fields).
 */

#include "libvroom.h"

#include "value_extraction.h"

#include <cstring>
#include <gtest/gtest.h>

using namespace libvroom;

class TestBuffer {
public:
  explicit TestBuffer(const std::string& content) : content_(content) {
    buffer_ = new uint8_t[content.size() + 64];
    std::memcpy(buffer_, content.data(), content.size());
    std::memset(buffer_ + content.size(), 0, 64);
  }
  ~TestBuffer() { delete[] buffer_; }
  const uint8_t* data() const { return buffer_; }
  size_t size() const { return content_.size(); }

private:
  std::string content_;
  uint8_t* buffer_;
};

class ColumnEscapeInfoTest : public ::testing::Test {
protected:
  Parser parser_;
};

// Test ColumnEscapeInfo struct behavior
TEST(ColumnEscapeInfoStructTest, DefaultConstruction) {
  ColumnEscapeInfo info;
  EXPECT_FALSE(info.has_quotes);
  EXPECT_FALSE(info.has_escapes);
  EXPECT_FALSE(info.needs_unescape());
  EXPECT_TRUE(info.allows_zero_copy());
}

TEST(ColumnEscapeInfoStructTest, QuotedNoEscapes) {
  ColumnEscapeInfo info{true, false};
  EXPECT_TRUE(info.has_quotes);
  EXPECT_FALSE(info.has_escapes);
  EXPECT_FALSE(info.needs_unescape());
  EXPECT_TRUE(info.allows_zero_copy());
}

TEST(ColumnEscapeInfoStructTest, QuotedWithEscapes) {
  ColumnEscapeInfo info{true, true};
  EXPECT_TRUE(info.has_quotes);
  EXPECT_TRUE(info.has_escapes);
  EXPECT_TRUE(info.needs_unescape());
  EXPECT_FALSE(info.allows_zero_copy());
}

// Test ParseIndex escape info methods
TEST_F(ColumnEscapeInfoTest, NoEscapeInfoBeforeCompute) {
  TestBuffer buf("a,b,c\n1,2,3\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  EXPECT_FALSE(result.idx.has_escape_info());
  EXPECT_EQ(result.idx.get_escape_info(0), nullptr);
  // column_allows_zero_copy returns false if no info available (conservative)
  EXPECT_FALSE(result.idx.column_allows_zero_copy(0));
}

TEST_F(ColumnEscapeInfoTest, SimpleCSVNoQuotes) {
  TestBuffer buf("name,value\nAlice,100\nBob,200\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());
  ASSERT_EQ(result.idx.columns, 2);

  // Both columns should allow zero-copy (no quotes at all)
  const ColumnEscapeInfo* col0 = result.idx.get_escape_info(0);
  const ColumnEscapeInfo* col1 = result.idx.get_escape_info(1);
  ASSERT_NE(col0, nullptr);
  ASSERT_NE(col1, nullptr);

  EXPECT_FALSE(col0->has_quotes);
  EXPECT_FALSE(col0->has_escapes);
  EXPECT_TRUE(result.idx.column_allows_zero_copy(0));

  EXPECT_FALSE(col1->has_quotes);
  EXPECT_FALSE(col1->has_escapes);
  EXPECT_TRUE(result.idx.column_allows_zero_copy(1));
}

TEST_F(ColumnEscapeInfoTest, QuotedFieldsNoEscapes) {
  // First column has quoted fields, second doesn't
  TestBuffer buf("name,value\n\"Alice\",100\n\"Bob\",200\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());

  // Column 0: has quotes but no escapes
  const ColumnEscapeInfo* col0 = result.idx.get_escape_info(0);
  ASSERT_NE(col0, nullptr);
  EXPECT_TRUE(col0->has_quotes);
  EXPECT_FALSE(col0->has_escapes);
  EXPECT_TRUE(result.idx.column_allows_zero_copy(0));

  // Column 1: no quotes
  const ColumnEscapeInfo* col1 = result.idx.get_escape_info(1);
  ASSERT_NE(col1, nullptr);
  EXPECT_FALSE(col1->has_quotes);
  EXPECT_FALSE(col1->has_escapes);
  EXPECT_TRUE(result.idx.column_allows_zero_copy(1));
}

TEST_F(ColumnEscapeInfoTest, DoubledQuotes) {
  // Column 0 has escaped quotes (doubled), column 1 doesn't
  TestBuffer buf("name,value\n\"Alice \"\"The Great\"\"\",100\n\"Bob\",200\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());

  // Column 0: has escapes (doubled quotes)
  const ColumnEscapeInfo* col0 = result.idx.get_escape_info(0);
  ASSERT_NE(col0, nullptr);
  EXPECT_TRUE(col0->has_quotes);
  EXPECT_TRUE(col0->has_escapes);
  EXPECT_FALSE(result.idx.column_allows_zero_copy(0));

  // Column 1: no quotes
  const ColumnEscapeInfo* col1 = result.idx.get_escape_info(1);
  ASSERT_NE(col1, nullptr);
  EXPECT_FALSE(col1->has_quotes);
  EXPECT_FALSE(col1->has_escapes);
  EXPECT_TRUE(result.idx.column_allows_zero_copy(1));
}

TEST_F(ColumnEscapeInfoTest, MixedColumns) {
  // Col 0: no quotes, Col 1: quoted no escape, Col 2: quoted with escape
  TestBuffer buf("a,b,c\n1,\"hello\",\"say \"\"hi\"\"\"\n2,\"world\",\"bye\"\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());
  ASSERT_EQ(result.idx.columns, 3);

  // Column 0: unquoted
  EXPECT_TRUE(result.idx.column_allows_zero_copy(0));

  // Column 1: quoted, no escapes
  const ColumnEscapeInfo* col1 = result.idx.get_escape_info(1);
  ASSERT_NE(col1, nullptr);
  EXPECT_TRUE(col1->has_quotes);
  EXPECT_FALSE(col1->has_escapes);
  EXPECT_TRUE(result.idx.column_allows_zero_copy(1));

  // Column 2: quoted with escapes in first row
  const ColumnEscapeInfo* col2 = result.idx.get_escape_info(2);
  ASSERT_NE(col2, nullptr);
  EXPECT_TRUE(col2->has_quotes);
  EXPECT_TRUE(col2->has_escapes);
  EXPECT_FALSE(result.idx.column_allows_zero_copy(2));
}

TEST_F(ColumnEscapeInfoTest, IdempotentCompute) {
  TestBuffer buf("a,b\n1,2\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  // First call
  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');
  ASSERT_TRUE(result.idx.has_escape_info());
  const ColumnEscapeInfo* info1 = result.idx.get_escape_info(0);

  // Second call should be idempotent
  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');
  const ColumnEscapeInfo* info2 = result.idx.get_escape_info(0);

  // Should be the same pointer (not re-allocated)
  EXPECT_EQ(info1, info2);
}

TEST_F(ColumnEscapeInfoTest, OutOfBoundsColumn) {
  TestBuffer buf("a,b\n1,2\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  // Out of bounds should return nullptr
  EXPECT_EQ(result.idx.get_escape_info(99), nullptr);
  // column_allows_zero_copy returns false for invalid columns
  EXPECT_FALSE(result.idx.column_allows_zero_copy(99));
}

// Test ValueExtractor integration
TEST_F(ColumnEscapeInfoTest, ValueExtractorFastPath) {
  // Column 0: quoted with escapes, Column 1: unquoted
  TestBuffer buf("name,value\n\"He said \"\"Hi\"\"\",100\n\"Alice\",200\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  ValueExtractor extractor(buf.data(), buf.size(), result.idx);

  // Before compute_column_escape_info, fast path not available
  EXPECT_FALSE(extractor.column_allows_zero_copy(0));
  EXPECT_FALSE(extractor.column_allows_zero_copy(1));

  // Compute escape info
  extractor.compute_column_escape_info();

  // Now we can check
  EXPECT_FALSE(extractor.column_allows_zero_copy(0)); // Has escapes
  EXPECT_TRUE(extractor.column_allows_zero_copy(1));  // Unquoted

  // Verify string extraction still works correctly
  EXPECT_EQ(extractor.get_string(0, 0), "He said \"Hi\"");
  EXPECT_EQ(extractor.get_string(0, 1), "100");
  EXPECT_EQ(extractor.get_string(1, 0), "Alice");
  EXPECT_EQ(extractor.get_string(1, 1), "200");
}

TEST_F(ColumnEscapeInfoTest, ValueExtractorFastPathSimpleQuoted) {
  // All fields quoted but no escapes
  TestBuffer buf("a,b\n\"hello\",\"world\"\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  ValueExtractor extractor(buf.data(), buf.size(), result.idx);
  extractor.compute_column_escape_info();

  // Both columns allow zero-copy (quotes but no escapes)
  EXPECT_TRUE(extractor.column_allows_zero_copy(0));
  EXPECT_TRUE(extractor.column_allows_zero_copy(1));

  // Verify extraction strips quotes correctly
  EXPECT_EQ(extractor.get_string(0, 0), "hello");
  EXPECT_EQ(extractor.get_string(0, 1), "world");
}

TEST_F(ColumnEscapeInfoTest, EmptyCSV) {
  TestBuffer buf("");
  auto result = parser_.parse(buf.data(), buf.size());
  // Empty CSV should parse successfully
  EXPECT_TRUE(result.success());

  // compute_column_escape_info should handle empty gracefully
  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');
  // No columns, so no escape info
  EXPECT_FALSE(result.idx.has_escape_info());
}

TEST_F(ColumnEscapeInfoTest, HeaderOnlyCSV) {
  TestBuffer buf("a,b,c\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  // Header row is present but no data rows
  // Escape info may or may not be populated depending on implementation
  // The important thing is it doesn't crash
  EXPECT_EQ(result.idx.columns, 3);
}

// Test with column-major layout
TEST_F(ColumnEscapeInfoTest, ColumnMajorLayout) {
  TestBuffer buf("a,b\n\"x\",1\n\"y\",2\n\"z\",3\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  // Convert to column-major first
  result.idx.compact_column_major();
  ASSERT_TRUE(result.idx.is_column_major());

  // Now compute escape info
  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());

  // Column 0: quoted, no escapes
  EXPECT_TRUE(result.idx.column_allows_zero_copy(0));

  // Column 1: unquoted
  EXPECT_TRUE(result.idx.column_allows_zero_copy(1));
}

// Test column-major: quote detection on later rows of column 0
// This tests the bug fix where column 0, row > 0 was incorrectly using
// the end position of column 0, row-1 instead of column (columns-1), row-1
TEST_F(ColumnEscapeInfoTest, ColumnMajorColumn0LaterRows) {
  // Row 0 col 0 is unquoted, but row 1 col 0 has quotes
  // This ensures we correctly scan row 1's column 0 field
  TestBuffer buf("a,b\nfirst,1\n\"second\",2\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compact_column_major();
  ASSERT_TRUE(result.idx.is_column_major());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());

  // Column 0 should detect has_quotes=true from row 1
  const ColumnEscapeInfo* col0 = result.idx.get_escape_info(0);
  ASSERT_NE(col0, nullptr);
  EXPECT_TRUE(col0->has_quotes) << "Should detect quotes in column 0 from row 1";
  EXPECT_FALSE(col0->has_escapes);
}

// Test column-major: escape detection on later rows of column 0
TEST_F(ColumnEscapeInfoTest, ColumnMajorColumn0LaterRowsWithEscapes) {
  // Row 0 col 0 is simple quoted, row 1 col 0 has escaped quotes
  TestBuffer buf("a,b\n\"simple\",1\n\"has \"\"escape\"\"\",2\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compact_column_major();
  ASSERT_TRUE(result.idx.is_column_major());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');

  ASSERT_TRUE(result.idx.has_escape_info());

  // Column 0 should detect has_escapes=true from row 1
  const ColumnEscapeInfo* col0 = result.idx.get_escape_info(0);
  ASSERT_NE(col0, nullptr);
  EXPECT_TRUE(col0->has_quotes);
  EXPECT_TRUE(col0->has_escapes) << "Should detect escapes in column 0 from row 1";
}

// Test move semantics preserve escape info
TEST_F(ColumnEscapeInfoTest, MoveSemantics) {
  TestBuffer buf("a,b\n\"x\",1\n");
  auto result = parser_.parse(buf.data(), buf.size());
  ASSERT_TRUE(result.success());

  result.idx.compute_column_escape_info(buf.data(), buf.size(), '"');
  ASSERT_TRUE(result.idx.has_escape_info());

  // Move the ParseIndex
  ParseIndex moved_idx = std::move(result.idx);

  // Moved-to should have escape info
  EXPECT_TRUE(moved_idx.has_escape_info());
  EXPECT_TRUE(moved_idx.column_allows_zero_copy(0));

  // Moved-from should be empty
  EXPECT_FALSE(result.idx.has_escape_info());
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
