#include "libvroom.h"

#include "mem_util.h"
#include "value_extraction.h"

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

class LazyColumnTest : public ::testing::Test {
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
// Basic Construction and Size Tests
// ============================================================================

TEST_F(LazyColumnTest, ConstructionAndSize) {
  ParseCSV("name,age,city\nAlice,30,NYC\nBob,25,LA\n");

  LazyColumn col0 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);
  LazyColumn col1 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 1, true);
  LazyColumn col2 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 2, true);

  EXPECT_EQ(col0.size(), 2);
  EXPECT_EQ(col1.size(), 2);
  EXPECT_EQ(col2.size(), 2);
  EXPECT_FALSE(col0.empty());
}

TEST_F(LazyColumnTest, EmptyColumn) {
  ParseCSV("header\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.size(), 0);
  EXPECT_TRUE(col.empty());
}

TEST_F(LazyColumnTest, NoHeader) {
  ParseCSV("Alice,30\nBob,25\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, false);

  EXPECT_EQ(col.size(), 2);
  EXPECT_EQ(col[0], "Alice");
  EXPECT_EQ(col[1], "Bob");
}

TEST_F(LazyColumnTest, ColumnIndex) {
  ParseCSV("a,b,c\n1,2,3\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 1, true);

  EXPECT_EQ(col.column_index(), 1);
}

// ============================================================================
// Random Access Tests
// ============================================================================

TEST_F(LazyColumnTest, RandomAccessStringView) {
  ParseCSV("name,age\nAlice,30\nBob,25\nCharlie,35\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col[0], "Alice");
  EXPECT_EQ(col[1], "Bob");
  EXPECT_EQ(col[2], "Charlie");

  // Access out of order
  EXPECT_EQ(col[2], "Charlie");
  EXPECT_EQ(col[0], "Alice");
  EXPECT_EQ(col[1], "Bob");
}

TEST_F(LazyColumnTest, RandomAccessOutOfRange) {
  ParseCSV("name\nAlice\nBob\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_THROW(col[100], std::out_of_range);
}

TEST_F(LazyColumnTest, RandomAccessIntegerColumn) {
  ParseCSV("val\n1\n2\n3\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col[0], "1");
  EXPECT_EQ(col[1], "2");
  EXPECT_EQ(col[2], "3");
}

// ============================================================================
// get_bounds() Tests
// ============================================================================

TEST_F(LazyColumnTest, GetBoundsBasic) {
  // CSV: "a,b\n1,2\n"
  //       0123 456
  ParseCSV("a,b\n1,2\n");

  LazyColumn col0 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);
  LazyColumn col1 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 1, true);

  // Row 0 (first data row after header)
  FieldSpan span0 = col0.get_bounds(0);
  EXPECT_TRUE(span0.is_valid());
  EXPECT_EQ(span0.start, 4); // "1" starts at offset 4
  EXPECT_EQ(span0.end, 5);   // ends at comma at offset 5

  FieldSpan span1 = col1.get_bounds(0);
  EXPECT_TRUE(span1.is_valid());
  EXPECT_EQ(span1.start, 6); // "2" starts at offset 6
  EXPECT_EQ(span1.end, 7);   // ends at newline at offset 7
}

TEST_F(LazyColumnTest, GetBoundsMultipleRows) {
  ParseCSV("name\nAlice\nBob\nCharlie\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  FieldSpan span0 = col.get_bounds(0);
  FieldSpan span1 = col.get_bounds(1);
  FieldSpan span2 = col.get_bounds(2);

  EXPECT_TRUE(span0.is_valid());
  EXPECT_TRUE(span1.is_valid());
  EXPECT_TRUE(span2.is_valid());

  // Verify lengths match expected values
  EXPECT_EQ(span0.length(), 5); // "Alice"
  EXPECT_EQ(span1.length(), 3); // "Bob"
  EXPECT_EQ(span2.length(), 7); // "Charlie"
}

TEST_F(LazyColumnTest, GetBoundsUsableForDeferredParsing) {
  ParseCSV("val\n42\n-123\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  // Get bounds for deferred parsing
  FieldSpan span = col.get_bounds(0);
  EXPECT_TRUE(span.is_valid());

  // Use bounds to parse the value manually (simulating deferred parsing)
  const char* data = reinterpret_cast<const char*>(buffer_->data() + span.start);
  auto result = parse_integer<int64_t>(data, span.length());
  EXPECT_TRUE(result.ok());
  EXPECT_EQ(result.get(), 42);

  // Second row
  FieldSpan span2 = col.get_bounds(1);
  const char* data2 = reinterpret_cast<const char*>(buffer_->data() + span2.start);
  auto result2 = parse_integer<int64_t>(data2, span2.length());
  EXPECT_TRUE(result2.ok());
  EXPECT_EQ(result2.get(), -123);
}

// ============================================================================
// get<T>() Typed Access Tests
// ============================================================================

TEST_F(LazyColumnTest, GetInteger) {
  ParseCSV("val\n42\n-123\n999\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.get<int64_t>(0).get(), 42);
  EXPECT_EQ(col.get<int64_t>(1).get(), -123);
  EXPECT_EQ(col.get<int64_t>(2).get(), 999);
}

TEST_F(LazyColumnTest, GetInt32) {
  ParseCSV("val\n42\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.get<int32_t>(0).get(), 42);
}

TEST_F(LazyColumnTest, GetDouble) {
  ParseCSV("val\n3.14\n-2.71\n1e10\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_NEAR(col.get<double>(0).get(), 3.14, 0.01);
  EXPECT_NEAR(col.get<double>(1).get(), -2.71, 0.01);
  EXPECT_NEAR(col.get<double>(2).get(), 1e10, 1e5);
}

TEST_F(LazyColumnTest, GetBool) {
  ParseCSV("val\ntrue\nfalse\n1\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_TRUE(col.get<bool>(0).get());
  EXPECT_FALSE(col.get<bool>(1).get());
  EXPECT_TRUE(col.get<bool>(2).get());
}

TEST_F(LazyColumnTest, GetNA) {
  ParseCSV("val\nNA\n\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_TRUE(col.get<int64_t>(0).is_na());
  EXPECT_TRUE(col.get<int64_t>(1).is_na());
}

TEST_F(LazyColumnTest, GetOutOfRange) {
  ParseCSV("val\n1\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_THROW(col.get<int64_t>(100), std::out_of_range);
}

// ============================================================================
// get_string() Tests
// ============================================================================

TEST_F(LazyColumnTest, GetStringUnquoted) {
  ParseCSV("name\nAlice\nBob\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.get_string(0), "Alice");
  EXPECT_EQ(col.get_string(1), "Bob");
}

TEST_F(LazyColumnTest, GetStringQuoted) {
  ParseCSV("name\n\"Hello\"\n\"World\"\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.get_string(0), "Hello");
  EXPECT_EQ(col.get_string(1), "World");
}

TEST_F(LazyColumnTest, GetStringWithEscapedQuotes) {
  ParseCSV("name\n\"He said \"\"Hi\"\"\"\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.get_string(0), "He said \"Hi\"");
}

TEST_F(LazyColumnTest, GetStringOutOfRange) {
  ParseCSV("val\n1\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_THROW(col.get_string(100), std::out_of_range);
}

// ============================================================================
// Iterator Tests
// ============================================================================

TEST_F(LazyColumnTest, IteratorBasic) {
  ParseCSV("name\nAlice\nBob\nCharlie\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  std::vector<std::string_view> values;
  for (auto sv : col) {
    values.push_back(sv);
  }

  EXPECT_EQ(values.size(), 3);
  EXPECT_EQ(values[0], "Alice");
  EXPECT_EQ(values[1], "Bob");
  EXPECT_EQ(values[2], "Charlie");
}

TEST_F(LazyColumnTest, IteratorEmpty) {
  ParseCSV("header\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  int count = 0;
  for (auto sv : col) {
    (void)sv;
    ++count;
  }
  EXPECT_EQ(count, 0);
}

TEST_F(LazyColumnTest, IteratorManual) {
  ParseCSV("name\nAlice\nBob\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  auto it = col.begin();
  EXPECT_EQ(*it, "Alice");
  EXPECT_EQ(it.row(), 0);

  ++it;
  EXPECT_EQ(*it, "Bob");
  EXPECT_EQ(it.row(), 1);

  ++it;
  EXPECT_EQ(it, col.end());
}

TEST_F(LazyColumnTest, IteratorPostIncrement) {
  ParseCSV("name\nAlice\nBob\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  auto it = col.begin();
  auto old_it = it++;

  EXPECT_EQ(*old_it, "Alice");
  EXPECT_EQ(*it, "Bob");
}

// ============================================================================
// Factory Method Tests
// ============================================================================

TEST_F(LazyColumnTest, MakeLazyColumnWithDialect) {
  ParseCSV("a,b\n1,2\n");

  Dialect dialect = Dialect::csv();
  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true, dialect);

  EXPECT_EQ(col[0], "1");
}

TEST_F(LazyColumnTest, MakeLazyColumnWithConfig) {
  ParseCSV("val\nMISSING\n");

  ExtractionConfig config;
  config.na_values = {"MISSING"};

  LazyColumn col =
      make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true, Dialect::csv(), config);

  EXPECT_TRUE(col.get<int64_t>(0).is_na());
}

TEST_F(LazyColumnTest, ValueExtractorFactory) {
  ParseCSV("name,age\nAlice,30\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  LazyColumn col0 = extractor.get_lazy_column(0);
  LazyColumn col1 = extractor.get_lazy_column(1);

  EXPECT_EQ(col0[0], "Alice");
  EXPECT_EQ(col1[0], "30");
}

TEST_F(LazyColumnTest, ValueExtractorFactoryOutOfRange) {
  ParseCSV("a,b\n1,2\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  EXPECT_THROW(extractor.get_lazy_column(100), std::out_of_range);
}

TEST_F(LazyColumnTest, FreeFunctionFactory) {
  ParseCSV("name\nAlice\n");

  ValueExtractor extractor(buffer_->data(), buffer_->size(), idx());

  LazyColumn col = get_lazy_column(extractor, 0);

  EXPECT_EQ(col[0], "Alice");
}

// ============================================================================
// Accessor Tests
// ============================================================================

TEST_F(LazyColumnTest, ConfigAccessor) {
  ParseCSV("val\n1\n");

  ExtractionConfig config;
  config.trim_whitespace = false;

  LazyColumn col =
      make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true, Dialect::csv(), config);

  EXPECT_FALSE(col.config().trim_whitespace);
}

TEST_F(LazyColumnTest, DialectAccessor) {
  ParseCSV("val\n1\n");

  Dialect dialect = Dialect::tsv();
  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true, dialect);

  EXPECT_EQ(col.dialect().delimiter, '\t');
}

TEST_F(LazyColumnTest, HasHeaderAccessor) {
  ParseCSV("val\n1\n");

  LazyColumn col_with_header = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);
  LazyColumn col_without_header =
      make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, false);

  EXPECT_TRUE(col_with_header.has_header());
  EXPECT_FALSE(col_without_header.has_header());
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(LazyColumnTest, QuotedFieldWithCRLF) {
  ParseCSV("name\r\n\"Hello\"\r\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col[0], "Hello");
}

TEST_F(LazyColumnTest, EmptyField) {
  ParseCSV("a,b\n,\n");

  LazyColumn col0 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);
  LazyColumn col1 = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 1, true);

  EXPECT_EQ(col0[0], "");
  EXPECT_EQ(col1[0], "");
}

TEST_F(LazyColumnTest, SingleColumn) {
  ParseCSV("header\nvalue1\nvalue2\n");

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.size(), 2);
  EXPECT_EQ(col[0], "value1");
  EXPECT_EQ(col[1], "value2");
}

TEST_F(LazyColumnTest, ManyRows) {
  std::string csv = "val\n";
  for (int i = 0; i < 1000; ++i) {
    csv += std::to_string(i) + "\n";
  }

  ParseCSV(csv);

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  EXPECT_EQ(col.size(), 1000);

  // Test random access at various positions
  EXPECT_EQ(col[0], "0");
  EXPECT_EQ(col[499], "499");
  EXPECT_EQ(col[999], "999");

  // Typed access
  EXPECT_EQ(col.get<int64_t>(500).get(), 500);
}

TEST_F(LazyColumnTest, MultipleColumns) {
  ParseCSV("a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n");

  std::vector<LazyColumn> columns;
  for (size_t i = 0; i < 5; ++i) {
    columns.push_back(make_lazy_column(buffer_->data(), buffer_->size(), idx(), i, true));
  }

  // Verify each column independently
  EXPECT_EQ(columns[0][0], "1");
  EXPECT_EQ(columns[1][0], "2");
  EXPECT_EQ(columns[2][0], "3");
  EXPECT_EQ(columns[3][0], "4");
  EXPECT_EQ(columns[4][0], "5");

  EXPECT_EQ(columns[0][1], "6");
  EXPECT_EQ(columns[1][1], "7");
  EXPECT_EQ(columns[2][1], "8");
  EXPECT_EQ(columns[3][1], "9");
  EXPECT_EQ(columns[4][1], "10");
}

// ============================================================================
// Performance-Oriented Tests (verify lazy behavior)
// ============================================================================

TEST_F(LazyColumnTest, LazyAccessDoesNotParseAll) {
  // This test verifies the design intent - LazyColumn should not
  // parse all rows upfront. While we can't directly measure this,
  // we verify that random access works correctly.

  std::string csv = "val\n";
  for (int i = 0; i < 100; ++i) {
    csv += std::to_string(i) + "\n";
  }
  ParseCSV(csv);

  LazyColumn col = make_lazy_column(buffer_->data(), buffer_->size(), idx(), 0, true);

  // Access only row 50 - should work without needing to parse all rows
  EXPECT_EQ(col.get<int64_t>(50).get(), 50);

  // Access first and last row
  EXPECT_EQ(col.get<int64_t>(0).get(), 0);
  EXPECT_EQ(col.get<int64_t>(99).get(), 99);
}

int main(int argc, char** argv) {
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
