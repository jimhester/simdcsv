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

// Bug fix tests for thousands separator validation
TEST_F(TypeDetectorTest, ThousandsSeparatorValidGrouping) {
    options.allow_thousands_sep = true;
    // Valid: first group 1-3 digits, subsequent groups exactly 3 digits
    EXPECT_EQ(TypeDetector::detect_field("1,000", options), FieldType::INTEGER);
    EXPECT_EQ(TypeDetector::detect_field("12,000", options), FieldType::INTEGER);
    EXPECT_EQ(TypeDetector::detect_field("123,000", options), FieldType::INTEGER);
    EXPECT_EQ(TypeDetector::detect_field("1,234,567", options), FieldType::INTEGER);
}

TEST_F(TypeDetectorTest, ThousandsSeparatorInvalidGrouping) {
    options.allow_thousands_sep = true;
    // Invalid: first group > 3 digits with separator
    EXPECT_NE(TypeDetector::detect_field("1234,567", options), FieldType::INTEGER);
    // Invalid: group after separator not exactly 3 digits
    EXPECT_NE(TypeDetector::detect_field("1,00", options), FieldType::INTEGER);
    EXPECT_NE(TypeDetector::detect_field("1,0000", options), FieldType::INTEGER);
    EXPECT_NE(TypeDetector::detect_field("1,23,456", options), FieldType::INTEGER);
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

TEST_F(TypeDetectorTest, DateInvalidMonth) {
    EXPECT_NE(TypeDetector::detect_field("2024-13-15", options), FieldType::DATE);
    EXPECT_NE(TypeDetector::detect_field("2024-00-15", options), FieldType::DATE);
}

// Bug fix tests for date validation
TEST_F(TypeDetectorTest, DateInvalidFebruary30) {
    // February 30 should never be valid
    EXPECT_NE(TypeDetector::detect_field("2024-02-30", options), FieldType::DATE);
    EXPECT_NE(TypeDetector::detect_field("2023-02-30", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidFebruary29NonLeapYear) {
    // February 29 invalid in non-leap years
    EXPECT_NE(TypeDetector::detect_field("2023-02-29", options), FieldType::DATE);
    EXPECT_NE(TypeDetector::detect_field("2100-02-29", options), FieldType::DATE); // Century not divisible by 400
}

TEST_F(TypeDetectorTest, DateValidFebruary29LeapYear) {
    // February 29 valid in leap years
    EXPECT_EQ(TypeDetector::detect_field("2024-02-29", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2000-02-29", options), FieldType::DATE); // Century divisible by 400
}

TEST_F(TypeDetectorTest, DateInvalidApril31) {
    // April has only 30 days
    EXPECT_NE(TypeDetector::detect_field("2024-04-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidJune31) {
    // June has only 30 days
    EXPECT_NE(TypeDetector::detect_field("2024-06-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidSeptember31) {
    // September has only 30 days
    EXPECT_NE(TypeDetector::detect_field("2024-09-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateInvalidNovember31) {
    // November has only 30 days
    EXPECT_NE(TypeDetector::detect_field("2024-11-31", options), FieldType::DATE);
}

TEST_F(TypeDetectorTest, DateValidMonthsWith31Days) {
    // Months with 31 days should accept day 31
    EXPECT_EQ(TypeDetector::detect_field("2024-01-31", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024-03-31", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024-05-31", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024-07-31", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024-08-31", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024-10-31", options), FieldType::DATE);
    EXPECT_EQ(TypeDetector::detect_field("2024-12-31", options), FieldType::DATE);
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

// Bug fix tests for type priority/hierarchy
TEST_F(ColumnTypeStatsTest, DominantTypePriorityBooleanOverInteger) {
    // 95% booleans should return BOOLEAN, not INTEGER
    for (int i = 0; i < 95; ++i) stats.add(FieldType::BOOLEAN);
    for (int i = 0; i < 5; ++i) stats.add(FieldType::STRING);
    EXPECT_EQ(stats.dominant_type(), FieldType::BOOLEAN);
}

TEST_F(ColumnTypeStatsTest, DominantTypePriorityIntegerOverFloat) {
    // 95% integers should return INTEGER, not FLOAT
    for (int i = 0; i < 95; ++i) stats.add(FieldType::INTEGER);
    for (int i = 0; i < 5; ++i) stats.add(FieldType::STRING);
    EXPECT_EQ(stats.dominant_type(), FieldType::INTEGER);
}

TEST_F(ColumnTypeStatsTest, DominantTypeMixedNumericFloatWins) {
    // Mix of floats and integers should return FLOAT
    for (int i = 0; i < 50; ++i) stats.add(FieldType::FLOAT);
    for (int i = 0; i < 45; ++i) stats.add(FieldType::INTEGER);
    for (int i = 0; i < 5; ++i) stats.add(FieldType::STRING);
    EXPECT_EQ(stats.dominant_type(), FieldType::FLOAT);
}

TEST_F(ColumnTypeStatsTest, DominantTypeDateNotNumeric) {
    // Dates should not be confused with numerics
    for (int i = 0; i < 95; ++i) stats.add(FieldType::DATE);
    for (int i = 0; i < 5; ++i) stats.add(FieldType::STRING);
    EXPECT_EQ(stats.dominant_type(), FieldType::DATE);
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

TEST_F(SIMDTypeDetectorTest, AllDigitsEmpty) {
    EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), 0));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsSingleDigit) {
    buffer[0] = '5';
    EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), 1));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsSingleNonDigit) {
    buffer[0] = 'x';
    EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), 1));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsLongString) {
    // Test with a string longer than one SIMD vector (typically 16 or 32 bytes)
    std::string long_digits(100, '7');
    std::memcpy(buffer.data(), long_digits.data(), long_digits.size());
    EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), long_digits.size()));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsLongStringWithNonDigitAtEnd) {
    std::string long_digits(99, '7');
    long_digits += 'x';
    std::memcpy(buffer.data(), long_digits.data(), long_digits.size());
    EXPECT_FALSE(SIMDTypeDetector::all_digits(buffer.data(), long_digits.size()));
}

TEST_F(SIMDTypeDetectorTest, AllDigitsExactVectorSize) {
    // Test with exactly 16, 32, and 64 bytes (common SIMD vector sizes)
    for (size_t size : {16, 32, 64}) {
        if (size > buffer.size()) continue;
        std::string digits(size, '9');
        std::memcpy(buffer.data(), digits.data(), digits.size());
        EXPECT_TRUE(SIMDTypeDetector::all_digits(buffer.data(), digits.size()))
            << "Failed for size " << size;
    }
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsEmpty) {
    EXPECT_EQ(SIMDTypeDetector::classify_digits(buffer.data(), 0), 0ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsAllDigits) {
    std::string digits = "12345678";
    std::memcpy(buffer.data(), digits.data(), digits.size());
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), digits.size());
    // All 8 bits should be set
    EXPECT_EQ(result, 0xFFULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsNoDigits) {
    std::string text = "abcdefgh";
    std::memcpy(buffer.data(), text.data(), text.size());
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), text.size());
    EXPECT_EQ(result, 0ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsMixed) {
    std::string mixed = "1a2b3c4d";  // digits at positions 0, 2, 4, 6
    std::memcpy(buffer.data(), mixed.data(), mixed.size());
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), mixed.size());
    // Bits 0, 2, 4, 6 should be set: 0b01010101 = 0x55
    EXPECT_EQ(result, 0x55ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsSingleDigit) {
    buffer[0] = '7';
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 1);
    EXPECT_EQ(result, 1ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsSingleNonDigit) {
    buffer[0] = 'x';
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 1);
    EXPECT_EQ(result, 0ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsBoundaryChars) {
    // Test characters just outside the '0'-'9' range
    buffer[0] = '/' ;  // '0' - 1
    buffer[1] = '0';   // boundary
    buffer[2] = '9';   // boundary
    buffer[3] = ':';   // '9' + 1
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 4);
    // Only positions 1 and 2 should be digits: 0b0110 = 0x6
    EXPECT_EQ(result, 0x6ULL);
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsLongerThan64) {
    // For classify_digits, only first 64 bytes matter
    std::string digits(100, '5');
    std::memcpy(buffer.data(), digits.data(), digits.size());
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 64);
    EXPECT_EQ(result, ~0ULL);  // All 64 bits set
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsExact64Bytes) {
    std::string digits(64, '3');
    std::memcpy(buffer.data(), digits.data(), digits.size());
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), 64);
    EXPECT_EQ(result, ~0ULL);  // All 64 bits set
}

TEST_F(SIMDTypeDetectorTest, ClassifyDigitsPatternAtVectorBoundary) {
    // Create a pattern that spans SIMD vector boundaries
    // Fill with digits, then put a non-digit at position 16 (common vector boundary)
    std::string pattern(32, '8');
    pattern[15] = 'x';  // just before common 16-byte boundary
    pattern[16] = 'y';  // at common 16-byte boundary
    std::memcpy(buffer.data(), pattern.data(), pattern.size());
    uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), pattern.size());

    // Expected: all bits set except 15 and 16
    uint64_t expected = 0xFFFFFFFFULL & ~(1ULL << 15) & ~(1ULL << 16);
    EXPECT_EQ(result, expected);
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
