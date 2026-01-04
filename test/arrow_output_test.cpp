#ifdef SIMDCSV_ENABLE_ARROW
#include <gtest/gtest.h>
#include <arrow/api.h>
#include "arrow_output.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"
#include <cstring>

namespace simdcsv {

struct TestBuffer {
    uint8_t* data;
    size_t len;
    explicit TestBuffer(const std::string& content) {
        len = content.size();
        data = allocate_padded_buffer(len, 64);
        std::memcpy(data, content.data(), len);
    }
    ~TestBuffer() { if (data) aligned_free(data); }
    TestBuffer(const TestBuffer&) = delete;
    TestBuffer& operator=(const TestBuffer&) = delete;
};

class ArrowOutputTest : public ::testing::Test {
protected:
    ArrowConvertResult parseAndConvert(const std::string& csv, const ArrowConvertOptions& opts = ArrowConvertOptions()) {
        TestBuffer buf(csv);
        two_pass parser;
        index idx = parser.init(buf.len, 1);
        parser.parse(buf.data, idx, buf.len);
        ArrowConverter converter(opts);
        return converter.convert(buf.data, buf.len, idx);
    }
};

TEST_F(ArrowOutputTest, BasicConversion) {
    auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 2);
    EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, TypeInferenceInteger) {
    ArrowConvertOptions opts; opts.infer_types = true;
    auto result = parseAndConvert("id,count\n1,100\n2,200\n", opts);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowOutputTest, TypeInferenceDouble) {
    ArrowConvertOptions opts; opts.infer_types = true;
    auto result = parseAndConvert("value\n1.5\n2.7\n", opts);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

TEST_F(ArrowOutputTest, TypeInferenceBoolean) {
    ArrowConvertOptions opts; opts.infer_types = true;
    auto result = parseAndConvert("flag\ntrue\nfalse\n", opts);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

TEST_F(ArrowOutputTest, ColumnTypeHelpers) {
    EXPECT_EQ(column_type_to_arrow(ColumnType::STRING)->id(), arrow::Type::STRING);
    EXPECT_EQ(column_type_to_arrow(ColumnType::INT64)->id(), arrow::Type::INT64);
    EXPECT_STREQ(column_type_to_string(ColumnType::STRING), "STRING");
}

// Null value tests
TEST_F(ArrowOutputTest, NullValues) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("id,value\n1,NA\n2,\n3,NULL\n4,100\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_rows, 4);
    // The value column should have nulls
    auto col = result.table->column(1);
    EXPECT_EQ(col->null_count(), 3);  // NA, empty, and NULL are all null values
}

TEST_F(ArrowOutputTest, NullValueCustom) {
    ArrowConvertOptions opts;
    opts.null_values = {"MISSING", "-999"};
    opts.infer_types = true;
    auto result = parseAndConvert("id,value\n1,MISSING\n2,-999\n3,100\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    auto col = result.table->column(1);
    EXPECT_EQ(col->null_count(), 2);
}

// Boolean tests
TEST_F(ArrowOutputTest, BooleanCaseInsensitive) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("flag\nTRUE\ntrue\nTrue\nFALSE\nfalse\nFalse\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
    EXPECT_EQ(result.num_rows, 6);
}

TEST_F(ArrowOutputTest, BooleanNumeric) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("flag\n1\n0\n1\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

TEST_F(ArrowOutputTest, BooleanYesNo) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("flag\nyes\nno\nYES\nNO\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

// Edge case tests
TEST_F(ArrowOutputTest, SingleColumn) {
    auto result = parseAndConvert("name\nAlice\nBob\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 1);
    EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, SingleRow) {
    auto result = parseAndConvert("a,b,c\n1,2,3\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 1);
}

TEST_F(ArrowOutputTest, EmptyFields) {
    ArrowConvertOptions opts;
    opts.infer_types = false;  // Treat all as strings
    auto result = parseAndConvert("a,b,c\n,,\n1,,3\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 2);
}

// Type inference edge cases
TEST_F(ArrowOutputTest, MixedIntDouble) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("value\n1\n2.5\n3\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    // Mixed int/double should promote to DOUBLE
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

TEST_F(ArrowOutputTest, MixedTypesToString) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    // Mix of numbers and text should become STRING
    auto result = parseAndConvert("value\n1\nhello\n3\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::STRING);
}

// Quoted field tests
TEST_F(ArrowOutputTest, QuotedFields) {
    auto result = parseAndConvert("name,address\n\"John Doe\",\"123 Main St\"\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 2);
    EXPECT_EQ(result.num_rows, 1);
}

TEST_F(ArrowOutputTest, QuotedWithCommas) {
    auto result = parseAndConvert("a,b,c\n1,\"A,B,C\",2\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 1);
}

// Special double values
TEST_F(ArrowOutputTest, SpecialDoubleValues) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("value\ninf\n-inf\nnan\n1.5\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
    EXPECT_EQ(result.num_rows, 4);
}

// Large integer test
TEST_F(ArrowOutputTest, LargeIntegers) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("id\n9223372036854775807\n-9223372036854775808\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

// Column name inference
TEST_F(ArrowOutputTest, AutoGeneratedColumnNames) {
    // When no header is properly parsed or columns exceed header count
    auto result = parseAndConvert("a,b\n1,2,3\n");  // Extra column in data
    ASSERT_TRUE(result.ok()) << result.error_message;
    // Should still work with auto-generated names for extra columns
    EXPECT_GE(result.num_columns, 2);
}

// Disable type inference
TEST_F(ArrowOutputTest, NoTypeInference) {
    ArrowConvertOptions opts;
    opts.infer_types = false;
    auto result = parseAndConvert("id,value\n1,100\n2,200\n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    // All columns should be STRING when type inference is disabled
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::STRING);
    EXPECT_EQ(result.schema->field(1)->type()->id(), arrow::Type::STRING);
}

// Whitespace handling
TEST_F(ArrowOutputTest, WhitespaceInNumbers) {
    ArrowConvertOptions opts;
    opts.infer_types = true;
    auto result = parseAndConvert("value\n  42  \n  3.14  \n", opts);
    ASSERT_TRUE(result.ok()) << result.error_message;
    // Should still parse numbers with leading/trailing whitespace
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

// Bounds validation tests (Issue #85)
// These tests verify that extract_field handles edge cases safely
TEST_F(ArrowOutputTest, FieldRangeStartEqualsEnd) {
    // When start == end, should return empty string_view without crashing
    auto result = parseAndConvert("a,b,c\n,,\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    // Empty fields are handled gracefully
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 1);
}

TEST_F(ArrowOutputTest, ConsecutiveDelimiters) {
    // Tests multiple consecutive delimiters creating zero-length fields
    auto result = parseAndConvert("a,b,c\n1,,3\n,2,\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 3);
    EXPECT_EQ(result.num_rows, 2);
}

// Error handling - empty data
TEST_F(ArrowOutputTest, EmptyData) {
    auto result = parseAndConvert("");
    EXPECT_FALSE(result.ok());
}

TEST_F(ArrowOutputTest, HeaderOnly) {
    auto result = parseAndConvert("a,b,c\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_rows, 0);
}

// Memory conversion function test
TEST_F(ArrowOutputTest, FromMemoryConversion) {
    std::string csv = "name,age\nAlice,30\nBob,25\n";
    auto result = csv_to_arrow_from_memory(
        reinterpret_cast<const uint8_t*>(csv.data()), csv.size());
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 2);
    EXPECT_EQ(result.num_rows, 2);
}

}  // namespace simdcsv

#else
#include <gtest/gtest.h>
TEST(ArrowOutputTest, ArrowNotEnabled) { GTEST_SKIP() << "Arrow not enabled"; }
#endif
