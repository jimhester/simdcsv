#include "libvroom.h"

#include "column_index.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>
#include <vector>

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

class ColumnIndexTest : public ::testing::Test {
protected:
  std::unique_ptr<TestBuffer> buffer_;
  Parser parser_;
  Parser::Result result_;

  void ParseCSV(const std::string& csv) {
    buffer_ = std::make_unique<TestBuffer>(csv);
    result_ = parser_.parse(buffer_->data(), buffer_->size());
  }

  ParseIndex& idx() { return result_.idx; }
};

// ============================================================================
// SortedIndexIterator Tests
// ============================================================================

TEST_F(ColumnIndexTest, SortedIndexIteratorBasic) {
  ParseCSV("a,b,c\n1,2,3\n");

  SortedIndexIterator iter(idx());
  EXPECT_TRUE(iter.has_next());
  EXPECT_EQ(iter.total_count(), 6); // 3 columns * 2 rows
}

TEST_F(ColumnIndexTest, SortedIndexIteratorSorted) {
  ParseCSV("a,b,c\n1,2,3\n");

  SortedIndexIterator iter(idx());
  std::vector<uint64_t> positions;
  while (iter.has_next()) {
    positions.push_back(iter.next());
  }

  // Should be in sorted order
  for (size_t i = 1; i < positions.size(); ++i) {
    EXPECT_LE(positions[i - 1], positions[i]);
  }
}

TEST_F(ColumnIndexTest, SortedIndexIteratorEmpty) {
  // Empty buffer results in no indexes
  buffer_ = std::make_unique<TestBuffer>("");
  // Can't parse empty buffer meaningfully, but we can test with a minimal CSV
  ParseCSV("a\n");

  SortedIndexIterator iter(idx());
  EXPECT_TRUE(iter.has_next());
  EXPECT_EQ(iter.total_count(), 1);
}

TEST_F(ColumnIndexTest, SortedIndexIteratorPeek) {
  ParseCSV("a,b\n1,2\n");

  SortedIndexIterator iter(idx());
  uint64_t peeked = iter.peek();
  uint64_t actual = iter.next();
  EXPECT_EQ(peeked, actual);
}

// ============================================================================
// LazySortedIndex Tests
// ============================================================================

TEST_F(ColumnIndexTest, LazySortedIndexBasic) {
  ParseCSV("a,b,c\n1,2,3\n");

  LazySortedIndex sorted(idx());
  EXPECT_EQ(sorted.size(), 6);
  EXPECT_FALSE(sorted.empty());
}

TEST_F(ColumnIndexTest, LazySortedIndexAccess) {
  ParseCSV("a,b,c\n1,2,3\n");

  LazySortedIndex sorted(idx());

  // Access elements
  uint64_t first = sorted[0];
  uint64_t second = sorted[1];

  // Should be in sorted order
  EXPECT_LE(first, second);
}

TEST_F(ColumnIndexTest, LazySortedIndexLazyMaterialization) {
  ParseCSV("a,b,c\n1,2,3\n4,5,6\n7,8,9\n");

  LazySortedIndex sorted(idx());

  // Initially not fully materialized
  EXPECT_FALSE(sorted.is_fully_materialized());

  // Access first element - should only materialize one element
  uint64_t first = sorted[0];
  (void)first;
  EXPECT_FALSE(sorted.is_fully_materialized());

  // Materialize all
  sorted.materialize_all();
  EXPECT_TRUE(sorted.is_fully_materialized());
}

TEST_F(ColumnIndexTest, LazySortedIndexLowerBound) {
  // CSV: "a,b\n1,2\n"
  //       0123 456
  ParseCSV("a,b\n1,2\n");

  LazySortedIndex sorted(idx());

  // Test lower_bound
  size_t pos = sorted.lower_bound(0);
  EXPECT_LT(pos, sorted.size());

  // Find position >= 4 (should find the ',' at position 5)
  pos = sorted.lower_bound(4);
  EXPECT_LT(pos, sorted.size());
}

TEST_F(ColumnIndexTest, LazySortedIndexOutOfRange) {
  ParseCSV("a\n1\n");

  LazySortedIndex sorted(idx());
  EXPECT_THROW(sorted[100], std::out_of_range);
}

// ============================================================================
// ColumnIndex Tests
// ============================================================================

TEST_F(ColumnIndexTest, ColumnIndexBasic) {
  ParseCSV("a,b,c\n1,2,3\n");

  ColumnIndex col_idx(idx(), buffer_->data(), buffer_->size());

  EXPECT_TRUE(col_idx.is_valid());
  EXPECT_EQ(col_idx.num_columns(), 3);
  EXPECT_EQ(col_idx.num_rows(), 1); // Excludes header
}

TEST_F(ColumnIndexTest, ColumnIndexFieldSpan) {
  // CSV: "a,b\n1,2\n"
  //       0123 456
  ParseCSV("a,b\n1,2\n");

  ColumnIndex col_idx(idx(), buffer_->data(), buffer_->size());

  // Field at row 0, col 0 should be "1"
  FieldSpan span = col_idx.get_field_span(0, 0);
  EXPECT_TRUE(span.is_valid());
  EXPECT_EQ(span.start, 4);
  EXPECT_EQ(span.length(), 1);
}

TEST_F(ColumnIndexTest, ColumnIndexHeaderSpan) {
  // CSV: "name,age\n1,2\n"
  ParseCSV("name,age\n1,2\n");

  ColumnIndex col_idx(idx(), buffer_->data(), buffer_->size());

  FieldSpan span0 = col_idx.get_header_span(0);
  EXPECT_TRUE(span0.is_valid());

  FieldSpan span1 = col_idx.get_header_span(1);
  EXPECT_TRUE(span1.is_valid());
}

TEST_F(ColumnIndexTest, ColumnIndexColumnSeparators) {
  ParseCSV("a,b,c\n1,2,3\n4,5,6\n");

  ColumnIndex col_idx(idx(), buffer_->data(), buffer_->size());

  // Check column separators
  const uint64_t* seps = col_idx.column_separators(0);
  EXPECT_NE(seps, nullptr);

  size_t count = col_idx.column_separator_count(0);
  EXPECT_GT(count, 0);
}

TEST_F(ColumnIndexTest, ColumnIndexOutOfBounds) {
  ParseCSV("a,b\n1,2\n");

  ColumnIndex col_idx(idx(), buffer_->data(), buffer_->size());

  // Out of bounds column
  FieldSpan span = col_idx.get_field_span(0, 100);
  EXPECT_FALSE(span.is_valid());

  // Out of bounds row
  span = col_idx.get_field_span(100, 0);
  EXPECT_FALSE(span.is_valid());
}

// ============================================================================
// Integration Tests: ValueExtractor with Deferred Sorting
// ============================================================================

TEST_F(ColumnIndexTest, ValueExtractorNonBlockingConstruction) {
  // Test that ValueExtractor construction doesn't block on sorting
  std::string csv = "col1,col2,col3\n";
  for (int i = 0; i < 100; ++i) {
    csv += std::to_string(i) + "," + std::to_string(i * 2) + "," + std::to_string(i * 3) + "\n";
  }
  ParseCSV(csv);

  // Construction should be fast (no O(n log n) sort)
  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  EXPECT_EQ(extractor.num_columns(), 3);
  EXPECT_EQ(extractor.num_rows(), 100);
}

TEST_F(ColumnIndexTest, ValueExtractorFieldAccessWithoutSort) {
  ParseCSV("name,age\nAlice,30\nBob,25\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  // Field access should work without triggering full sort
  EXPECT_EQ(extractor.get_string_view(0, 0), "Alice");
  EXPECT_EQ(extractor.get_string_view(0, 1), "30");
  EXPECT_EQ(extractor.get_string_view(1, 0), "Bob");
  EXPECT_EQ(extractor.get_string_view(1, 1), "25");
}

TEST_F(ColumnIndexTest, ValueExtractorLazyColumnWithoutSort) {
  ParseCSV("name,age\nAlice,30\nBob,25\nCharlie,35\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  // Get lazy column - should not trigger sorting
  LazyColumn col = extractor.get_lazy_column(0);

  EXPECT_EQ(col.size(), 3);
  EXPECT_EQ(col[0], "Alice");
  EXPECT_EQ(col[1], "Bob");
  EXPECT_EQ(col[2], "Charlie");
}

TEST_F(ColumnIndexTest, ValueExtractorByteOffsetToLocation) {
  // CSV: "a,b\n1,2\n"
  //       0123 456
  ParseCSV("a,b\n1,2\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  // byte_offset_to_location uses lazy sorted index
  auto loc = extractor.byte_offset_to_location(0);
  EXPECT_TRUE(loc.found);

  loc = extractor.byte_offset_to_location(4);
  EXPECT_TRUE(loc.found);
}

TEST_F(ColumnIndexTest, ValueExtractorGetHeader) {
  ParseCSV("name,age,city\n1,2,3\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  auto headers = extractor.get_header();
  EXPECT_EQ(headers.size(), 3);
  EXPECT_EQ(headers[0], "name");
  EXPECT_EQ(headers[1], "age");
  EXPECT_EQ(headers[2], "city");
}

TEST_F(ColumnIndexTest, ValueExtractorColumnExtraction) {
  ParseCSV("val\n1\n2\n3\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  auto col = extractor.extract_column<int64_t>(0);
  EXPECT_EQ(col.size(), 3);
  EXPECT_EQ(col[0].value(), 1);
  EXPECT_EQ(col[1].value(), 2);
  EXPECT_EQ(col[2].value(), 3);
}

// ============================================================================
// Performance-Oriented Tests
// ============================================================================

TEST_F(ColumnIndexTest, PerformanceLargeFileSingleColumnAccess) {
  // Create a large CSV
  std::string csv = "a,b,c,d,e\n";
  for (int i = 0; i < 1000; ++i) {
    csv += std::to_string(i) + ",";
    csv += std::to_string(i * 2) + ",";
    csv += std::to_string(i * 3) + ",";
    csv += std::to_string(i * 4) + ",";
    csv += std::to_string(i * 5) + "\n";
  }
  ParseCSV(csv);

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  // Accessing a single column should not require sorting the entire index
  LazyColumn col = extractor.get_lazy_column(2);
  EXPECT_EQ(col.size(), 1000);

  // Verify random access works
  EXPECT_EQ(col.get<int64_t>(500).get(), 500 * 3);
}

TEST_F(ColumnIndexTest, PerformanceRandomAccessPattern) {
  std::string csv = "col\n";
  for (int i = 0; i < 500; ++i) {
    csv += std::to_string(i) + "\n";
  }
  ParseCSV(csv);

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  // Random access pattern - should work without full sort
  EXPECT_EQ(extractor.get<int64_t>(250, 0).get(), 250);
  EXPECT_EQ(extractor.get<int64_t>(0, 0).get(), 0);
  EXPECT_EQ(extractor.get<int64_t>(499, 0).get(), 499);
  EXPECT_EQ(extractor.get<int64_t>(100, 0).get(), 100);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
