#include <gtest/gtest.h>
#include <string>
#include <vector>
#include <cstring>

#include "type_detector.h"

using namespace simdcsv;

class TypeDetectorTest : public ::testing::Test {
protected:
    TypeDetectionOptions options;
    void SetUp() override { options = TypeDetectionOptions::defaults(); }
};

TEST_F(TypeDetectorTest, EmptyString) {
    EXPECT_EQ(TypeDetector::detect_field("", options), FieldType::EMPTY);
}

TEST_F(TypeDetectorTest, WhitespaceOnly) {
    EXPECT_EQ(TypeDetector::detect_field("   ", options), FieldType::EMPTY);
}

TEST_F(TypeDetectorTest, BooleanTrue) {
    EXPECT_EQ(TypeDetector::detect_field("true", options), FieldType::BOOLEAN);
    EXPECT_EQ(TypeDetector::detect_field("TRUE", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanFalse) {
    EXPECT_EQ(TypeDetector::detect_field("false", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanYesNo) {
    EXPECT_EQ(TypeDetector::detect_field("yes", options), FieldType::BOOLEAN);
    EXPECT_EQ(TypeDetector::detect_field("no", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanNumeric) {
    EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::BOOLEAN);
    EXPECT_EQ(TypeDetector::detect_field("1", options), FieldType::BOOLEAN);
}

TEST_F(TypeDetectorTest, BooleanNumericDisabled) {
    options.bool_as_int = false;
    EXPECT_EQ(TypeDetector::detect_field("0", options), FieldType::INTEGER);
    EXPECT_EQ(TypeDetector::detect_field("1", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerPositive) {
    EXPECT_EQ(TypeDetector::detect_field("42", options), FieldType::INTEGER);
    EXPECT_EQ(TypeDetector::detect_field("123456789", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerNegative) {
    EXPECT_EQ(TypeDetector::detect_field("-42", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, IntegerWithThousandsSeparator) {
    options.allow_thousands_sep = true;
    EXPECT_EQ(TypeDetector::detect_field("1,000", options), FieldType::INTEGER);
    EXPECT_EQ(TypeDetector::detect_field("1,000,000", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, FloatSimple) {
    EXPECT_EQ(TypeDetector::detect_field("3.14", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatNegative) {
    EXPECT_EQ(TypeDetector::detect_field("-3.14", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatExponential) {
    EXPECT_EQ(TypeDetector::detect_field("1e10", options), FieldType::FLOAT);
    EXPECT_EQ(TypeDetector::detect_field("1.5e-10", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, FloatSpecialValues) {
    EXPECT_EQ(TypeDetector::detect_field("inf", options), FieldType::FLOAT);
    EXPECT_EQ(TypeDetector::detect_field("nan", options), FieldType::FLOAT);
    EXPECT_EQ(TypeDetector::detect_field("-inf", options), FieldType::FLOAT);
}

TEST_F(TypeDetectorTest, DateISO) {
    EXPECT_EQ(TypeDetector::detect_field("2024-01-15", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024/01/15", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateUS) {
    EXPECT_EQ(TypeDetector::detect_field("01/15/2024", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateEU) {
    EXPECT_EQ(TypeDetector::detect_field("15/01/2024", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateCompact) {
    EXPECT_EQ(TypeDetector::detect_field("20240115", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalid) {
    EXPECT_NE(TypeDetector::detect_field("2024-13-15", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, StringSimple) {
    EXPECT_EQ(TypeDetector::detect_field("hello", options), FieldType::STRING);
}

TEST_F(TypeDetectorTest, FieldTypeToString) {
    EXPECT_STREQ(field_type_to_string(FieldType::BOOLEAN), "boolean");
    EXPECT_STREQ(field_type_to_string(FieldType::INTEGER), "integer");
    EXPECT_STREQ(field_type_to_string(FieldType::FLOAT), "float");
    EXPECT_STREQ(field_type_to_string(FieldType::DATE), "date");
    EXPECT_STREQ(field_type_to_string(FieldType::STRING), "string");
    EXPECT_STREQ(field_type_to_string(FieldType::EMPTY), "empty");
}

class ColumnTypeStatsTest : public ::testing::Test {
protected:
    ColumnTypeStats stats;
};

TEST_F(ColumnTypeStatsTest, AddTypes) {
    stats.add(FieldType::INTEGER);
    stats.add(FieldType::INTEGER);
    EXPECT_EQ(stats.total_count, 2);
    EXPECT_EQ(stats.integer_count, 2);
}

TEST_F(ColumnTypeStatsTest, DominantType) {
    for (int i = 0; i < 100; ++i) stats.add(FieldType::INTEGER);
    EXPECT_EQ(stats.dominant_type(), FieldType::INTEGER);
}

class ColumnTypeInferenceTest : public ::testing::Test {
protected:
    ColumnTypeInference inference;
};

TEST_F(ColumnTypeInferenceTest, SingleRow) {
    inference.add_row({"123", "3.14", "true", "2024-01-15", "hello"});
    auto types = inference.infer_types();
    EXPECT_EQ(types[0], FieldType::INTEGER);
    EXPECT_EQ(types[1], FieldType::FLOAT);
    EXPECT_EQ(types[2], FieldType::BOOLEAN);
    EXPECT_EQ(types[3], FieldType::DATE);
    EXPECT_EQ(types[4], FieldType::STRING);
}

TEST_F(ColumnTypeInferenceTest, MultipleRows) {
    inference.add_row({"123", "true"});
    inference.add_row({"456", "false"});
    auto types = inference.infer_types();
    EXPECT_EQ(types[0], FieldType::INTEGER);
    EXPECT_EQ(types[1], FieldType::BOOLEAN);
}

class TypeHintsTest : public ::testing::Test {
protected:
    TypeHints hints;
};

TEST_F(TypeHintsTest, AddAndGet) {
    hints.add("age", FieldType::INTEGER);
    EXPECT_EQ(hints.get("age"), FieldType::INTEGER);
    EXPECT_EQ(hints.get("unknown"), FieldType::STRING);
}

TEST_F(TypeHintsTest, HasHint) {
    hints.add("age", FieldType::INTEGER);
    EXPECT_TRUE(hints.has_hint("age"));
    EXPECT_FALSE(hints.has_hint("unknown"));
}

class SIMDTypeDetectorTest : public ::testing::Test {
protected:
    std::vector<uint8_t> buffer;
    void SetUp() override { buffer.resize(128, 0); }
};

TEST_F(SIMDTypeDetectorTest, AllDigits) {
    std::string digits = "12345678";
    std::memcpy(buffer.data(), digits.data(), digits.size());
    EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), digits.size()));
}

TEST_F(SIMDTypeDetectorTest, NotAllDigits) {
    std::string mixed = "1234a5678";
    std::memcpy(buffer.data(), mixed.data(), mixed.size());
    EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), mixed.size()));
}

TEST_F(SIMDTypeDetectorTest, DetectBatch) {
    const char* fields[] = {"123", "3.14", "true", "hello"};
    size_t lengths[] = {3, 4, 4, 5};
    FieldType results[4];
    const uint8_t* field_ptrs[4];
    for (int i = 0; i < 4; ++i) {
        field_ptrs[i] = reinterpret_cast<const uint8_t*>(fields[i]);
    }
    SIMDTypeDetector::detect_batch(field_ptrs, lengths, 4, results);
    EXPECT_EQ(results[0], FieldType::INTEGER);
    EXPECT_EQ(results[1], FieldType::FLOAT);
    EXPECT_EQ(results[2], FieldType::BOOLEAN);
    EXPECT_EQ(results[3], FieldType::STRING);
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
