/**
 * @file simd_number_parsing_test.cpp
 * @brief Unit tests for SIMD-accelerated number parsing.
 */

#include <gtest/gtest.h>
#include <cstring>
#include <cmath>
#include <limits>
#include <vector>

#include "simd_number_parsing.h"

using namespace simdcsv;

// =============================================================================
// SIMD Integer Parser Tests
// =============================================================================

class SIMDIntegerParserTest : public ::testing::Test {
protected:
    const char* make_str(const std::string& s) {
        str_storage_ = s;
        return str_storage_.c_str();
    }
private:
    std::string str_storage_;
};

// Basic parsing tests
TEST_F(SIMDIntegerParserTest, ParseZero) {
    auto result = SIMDIntegerParser::parse_int64("0", 1);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 0);
}

TEST_F(SIMDIntegerParserTest, ParsePositiveSmall) {
    auto result = SIMDIntegerParser::parse_int64("12345", 5);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDIntegerParserTest, ParsePositiveLarge) {
    auto result = SIMDIntegerParser::parse_int64("123456789012345678", 18);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 123456789012345678LL);
}

TEST_F(SIMDIntegerParserTest, ParseNegativeSmall) {
    auto result = SIMDIntegerParser::parse_int64("-12345", 6);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), -12345);
}

TEST_F(SIMDIntegerParserTest, ParseNegativeLarge) {
    auto result = SIMDIntegerParser::parse_int64("-123456789012345678", 19);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), -123456789012345678LL);
}

TEST_F(SIMDIntegerParserTest, ParseWithPlusSign) {
    auto result = SIMDIntegerParser::parse_int64("+42", 3);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 42);
}

// Boundary tests
TEST_F(SIMDIntegerParserTest, Int64Max) {
    auto result = SIMDIntegerParser::parse_int64("9223372036854775807", 19);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), INT64_MAX);
}

TEST_F(SIMDIntegerParserTest, Int64Min) {
    auto result = SIMDIntegerParser::parse_int64("-9223372036854775808", 20);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), INT64_MIN);
}

TEST_F(SIMDIntegerParserTest, Int64Overflow) {
    auto result = SIMDIntegerParser::parse_int64("9223372036854775808", 19);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, Int64Underflow) {
    auto result = SIMDIntegerParser::parse_int64("-9223372036854775809", 20);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

// Uint64 boundary tests
TEST_F(SIMDIntegerParserTest, Uint64MaxBoundary) {
    // 18446744073709551615 is UINT64_MAX
    auto result = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), UINT64_MAX);
}

TEST_F(SIMDIntegerParserTest, Uint64Overflow) {
    // 18446744073709551616 is UINT64_MAX + 1
    auto result = SIMDIntegerParser::parse_uint64("18446744073709551616", 20);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, Uint64OverflowByLastDigit) {
    // Test that the boundary condition at max_before_mul works correctly
    // 1844674407370955161 * 10 + 6 = 18446744073709551616 (overflow)
    auto result = SIMDIntegerParser::parse_uint64("18446744073709551616", 20);
    EXPECT_FALSE(result.ok());

    // 1844674407370955161 * 10 + 5 = 18446744073709551615 (UINT64_MAX, ok)
    auto result2 = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
    EXPECT_TRUE(result2.ok());
}

// Whitespace handling
TEST_F(SIMDIntegerParserTest, WhitespaceTrimming) {
    auto result = SIMDIntegerParser::parse_int64("  42  ", 6);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 42);
}

TEST_F(SIMDIntegerParserTest, LeadingWhitespace) {
    auto result = SIMDIntegerParser::parse_int64("   123", 6);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 123);
}

TEST_F(SIMDIntegerParserTest, TrailingWhitespace) {
    auto result = SIMDIntegerParser::parse_int64("456   ", 6);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 456);
}

TEST_F(SIMDIntegerParserTest, TabWhitespace) {
    auto result = SIMDIntegerParser::parse_int64("\t789\t", 5);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 789);
}

TEST_F(SIMDIntegerParserTest, MixedTabsAndSpaces) {
    auto result = SIMDIntegerParser::parse_int64(" \t 42 \t ", 8);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 42);
}

TEST_F(SIMDIntegerParserTest, NoTrimWhitespace) {
    auto result = SIMDIntegerParser::parse_int64("  42  ", 6, false);
    EXPECT_FALSE(result.ok());  // Fails because leading space is not a digit
}

// NA and empty handling
TEST_F(SIMDIntegerParserTest, EmptyIsNA) {
    auto result = SIMDIntegerParser::parse_int64("", 0);
    EXPECT_TRUE(result.is_na());
    EXPECT_FALSE(result.ok());
    EXPECT_EQ(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, WhitespaceOnlyIsNA) {
    auto result = SIMDIntegerParser::parse_int64("   ", 3);
    EXPECT_TRUE(result.is_na());
}

// Error cases
TEST_F(SIMDIntegerParserTest, InvalidCharacter) {
    auto result = SIMDIntegerParser::parse_int64("12a34", 5);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, DecimalPoint) {
    auto result = SIMDIntegerParser::parse_int64("12.34", 5);
    EXPECT_FALSE(result.ok());
}

TEST_F(SIMDIntegerParserTest, JustSign) {
    auto result = SIMDIntegerParser::parse_int64("-", 1);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDIntegerParserTest, TooManyDigits) {
    auto result = SIMDIntegerParser::parse_int64("12345678901234567890", 20);
    EXPECT_FALSE(result.ok());  // 20 digits is too many for int64
}

// Unsigned integer tests
TEST_F(SIMDIntegerParserTest, ParseUint64) {
    auto result = SIMDIntegerParser::parse_uint64("12345", 5);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 12345ULL);
}

TEST_F(SIMDIntegerParserTest, Uint64Max) {
    auto result = SIMDIntegerParser::parse_uint64("18446744073709551615", 20);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), UINT64_MAX);
}

TEST_F(SIMDIntegerParserTest, Uint64NegativeError) {
    auto result = SIMDIntegerParser::parse_uint64("-1", 2);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

// Digit validation
TEST_F(SIMDIntegerParserTest, ValidateDigitsAllValid) {
    const uint8_t data[] = "1234567890";
    EXPECT_TRUE(SIMDIntegerParser::validate_digits_simd(data, 10));
}

TEST_F(SIMDIntegerParserTest, ValidateDigitsWithInvalid) {
    const uint8_t data[] = "12345a6789";
    EXPECT_FALSE(SIMDIntegerParser::validate_digits_simd(data, 10));
}

TEST_F(SIMDIntegerParserTest, ValidateDigitsLongString) {
    // Test SIMD path with 64+ characters
    std::string digits(100, '5');
    EXPECT_TRUE(SIMDIntegerParser::validate_digits_simd(
        reinterpret_cast<const uint8_t*>(digits.c_str()), digits.size()));
}

// Column parsing
TEST_F(SIMDIntegerParserTest, ParseInt64Column) {
    const char* fields[] = {"123", "-456", "789", "", "42"};
    size_t lengths[] = {3, 4, 3, 0, 2};
    int64_t results[5];
    bool valid[5];

    SIMDIntegerParser::parse_int64_column(fields, lengths, 5, results, valid);

    EXPECT_TRUE(valid[0]); EXPECT_EQ(results[0], 123);
    EXPECT_TRUE(valid[1]); EXPECT_EQ(results[1], -456);
    EXPECT_TRUE(valid[2]); EXPECT_EQ(results[2], 789);
    EXPECT_FALSE(valid[3]);  // Empty
    EXPECT_TRUE(valid[4]); EXPECT_EQ(results[4], 42);
}

TEST_F(SIMDIntegerParserTest, ParseInt64ColumnVector) {
    const char* fields[] = {"100", "200", "invalid", "300"};
    size_t lengths[] = {3, 3, 7, 3};

    auto results = SIMDIntegerParser::parse_int64_column(fields, lengths, 4);

    EXPECT_EQ(results.size(), 4);
    EXPECT_TRUE(results[0].has_value()); EXPECT_EQ(*results[0], 100);
    EXPECT_TRUE(results[1].has_value()); EXPECT_EQ(*results[1], 200);
    EXPECT_FALSE(results[2].has_value());  // Invalid
    EXPECT_TRUE(results[3].has_value()); EXPECT_EQ(*results[3], 300);
}

// =============================================================================
// SIMD Double Parser Tests
// =============================================================================

class SIMDDoubleParserTest : public ::testing::Test {};

// Basic parsing tests
TEST_F(SIMDDoubleParserTest, ParseInteger) {
    auto result = SIMDDoubleParser::parse_double("42", 2);
    EXPECT_TRUE(result.ok());
    EXPECT_DOUBLE_EQ(result.get(), 42.0);
}

TEST_F(SIMDDoubleParserTest, ParseDecimal) {
    auto result = SIMDDoubleParser::parse_double("3.14", 4);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 3.14, 0.001);
}

TEST_F(SIMDDoubleParserTest, ParseDecimalNoIntPart) {
    auto result = SIMDDoubleParser::parse_double(".5", 2);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 0.5, 0.001);
}

TEST_F(SIMDDoubleParserTest, ParseDecimalNoFracPart) {
    auto result = SIMDDoubleParser::parse_double("5.", 2);
    EXPECT_TRUE(result.ok());
    EXPECT_DOUBLE_EQ(result.get(), 5.0);
}

TEST_F(SIMDDoubleParserTest, ParseNegative) {
    auto result = SIMDDoubleParser::parse_double("-3.14", 5);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), -3.14, 0.001);
}

// Scientific notation
TEST_F(SIMDDoubleParserTest, ParseScientificPositive) {
    auto result = SIMDDoubleParser::parse_double("1e10", 4);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 1e10, 1e5);
}

TEST_F(SIMDDoubleParserTest, ParseScientificNegativeExp) {
    auto result = SIMDDoubleParser::parse_double("1e-10", 5);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 1e-10, 1e-15);
}

TEST_F(SIMDDoubleParserTest, ParseScientificWithDecimal) {
    auto result = SIMDDoubleParser::parse_double("1.5e-10", 7);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 1.5e-10, 1e-15);
}

TEST_F(SIMDDoubleParserTest, ParseScientificUpperE) {
    auto result = SIMDDoubleParser::parse_double("2.5E+5", 6);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 2.5e5, 1);
}

// Special values
TEST_F(SIMDDoubleParserTest, ParseNaN) {
    auto result = SIMDDoubleParser::parse_double("NaN", 3);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isnan(result.get()));
}

TEST_F(SIMDDoubleParserTest, ParseNaNLowercase) {
    auto result = SIMDDoubleParser::parse_double("nan", 3);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isnan(result.get()));
}

TEST_F(SIMDDoubleParserTest, ParseInf) {
    auto result = SIMDDoubleParser::parse_double("Inf", 3);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isinf(result.get()));
    EXPECT_GT(result.get(), 0);
}

TEST_F(SIMDDoubleParserTest, ParseInfinity) {
    auto result = SIMDDoubleParser::parse_double("Infinity", 8);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isinf(result.get()));
}

TEST_F(SIMDDoubleParserTest, ParseNegInf) {
    auto result = SIMDDoubleParser::parse_double("-Inf", 4);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isinf(result.get()));
    EXPECT_LT(result.get(), 0);
}

TEST_F(SIMDDoubleParserTest, ParseNegInfinity) {
    auto result = SIMDDoubleParser::parse_double("-Infinity", 9);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isinf(result.get()));
    EXPECT_LT(result.get(), 0);
}

// Zero handling
TEST_F(SIMDDoubleParserTest, ParseZero) {
    auto result = SIMDDoubleParser::parse_double("0", 1);
    EXPECT_TRUE(result.ok());
    EXPECT_DOUBLE_EQ(result.get(), 0.0);
}

TEST_F(SIMDDoubleParserTest, ParseNegativeZero) {
    auto result = SIMDDoubleParser::parse_double("-0.0", 4);
    EXPECT_TRUE(result.ok());
    EXPECT_DOUBLE_EQ(result.get(), -0.0);
    EXPECT_TRUE(std::signbit(result.get()));
}

// Whitespace
TEST_F(SIMDDoubleParserTest, WhitespaceTrimming) {
    auto result = SIMDDoubleParser::parse_double("  3.14  ", 8);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 3.14, 0.001);
}

// Error cases
TEST_F(SIMDDoubleParserTest, EmptyIsNA) {
    auto result = SIMDDoubleParser::parse_double("", 0);
    EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDDoubleParserTest, MalformedScientificNoDigits) {
    auto result = SIMDDoubleParser::parse_double("1e", 2);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDDoubleParserTest, MalformedScientificJustSign) {
    auto result = SIMDDoubleParser::parse_double("1e-", 3);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDDoubleParserTest, TrailingCharacters) {
    auto result = SIMDDoubleParser::parse_double("3.14abc", 7);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

TEST_F(SIMDDoubleParserTest, InvalidInfinityVariant) {
    auto result = SIMDDoubleParser::parse_double("INFxxxxx", 8);
    EXPECT_FALSE(result.ok());
}

// Column parsing
TEST_F(SIMDDoubleParserTest, ParseDoubleColumn) {
    const char* fields[] = {"1.5", "-2.5", "3e10", "", "nan"};
    size_t lengths[] = {3, 4, 4, 0, 3};
    double results[5];
    bool valid[5];

    SIMDDoubleParser::parse_double_column(fields, lengths, 5, results, valid);

    EXPECT_TRUE(valid[0]); EXPECT_NEAR(results[0], 1.5, 0.001);
    EXPECT_TRUE(valid[1]); EXPECT_NEAR(results[1], -2.5, 0.001);
    EXPECT_TRUE(valid[2]); EXPECT_NEAR(results[2], 3e10, 1e5);
    EXPECT_FALSE(valid[3]);  // Empty
    EXPECT_TRUE(valid[4]); EXPECT_TRUE(std::isnan(results[4]));
}

// =============================================================================
// SIMD Type Validator Tests
// =============================================================================

class SIMDTypeValidatorTest : public ::testing::Test {};

// Integer validation
TEST_F(SIMDTypeValidatorTest, CouldBeIntegerPositive) {
    const uint8_t data[] = "12345";
    EXPECT_TRUE(SIMDTypeValidator::could_be_integer(data, 5));
}

TEST_F(SIMDTypeValidatorTest, CouldBeIntegerNegative) {
    const uint8_t data[] = "-12345";
    EXPECT_TRUE(SIMDTypeValidator::could_be_integer(data, 6));
}

TEST_F(SIMDTypeValidatorTest, CouldBeIntegerWithWhitespace) {
    const uint8_t data[] = "  123  ";
    EXPECT_TRUE(SIMDTypeValidator::could_be_integer(data, 7));
}

TEST_F(SIMDTypeValidatorTest, NotIntegerWithDecimal) {
    const uint8_t data[] = "12.34";
    EXPECT_FALSE(SIMDTypeValidator::could_be_integer(data, 5));
}

TEST_F(SIMDTypeValidatorTest, NotIntegerWithLetters) {
    const uint8_t data[] = "12abc";
    EXPECT_FALSE(SIMDTypeValidator::could_be_integer(data, 5));
}

// Float validation
TEST_F(SIMDTypeValidatorTest, CouldBeFloatDecimal) {
    const uint8_t data[] = "3.14";
    EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 4));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatScientific) {
    const uint8_t data[] = "1e10";
    EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 4));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatNaN) {
    const uint8_t data[] = "nan";
    EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 3));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatInf) {
    const uint8_t data[] = "inf";
    EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 3));
}

TEST_F(SIMDTypeValidatorTest, CouldBeFloatNegInf) {
    const uint8_t data[] = "-infinity";
    EXPECT_TRUE(SIMDTypeValidator::could_be_float(data, 9));
}

TEST_F(SIMDTypeValidatorTest, NotFloatJustInteger) {
    const uint8_t data[] = "12345";
    EXPECT_FALSE(SIMDTypeValidator::could_be_float(data, 5));  // Integer, not float
}

TEST_F(SIMDTypeValidatorTest, NotFloatString) {
    const uint8_t data[] = "hello";
    EXPECT_FALSE(SIMDTypeValidator::could_be_float(data, 5));
}

// Batch validation
TEST_F(SIMDTypeValidatorTest, ValidateBatch) {
    const uint8_t* fields[] = {
        reinterpret_cast<const uint8_t*>("123"),
        reinterpret_cast<const uint8_t*>("3.14"),
        reinterpret_cast<const uint8_t*>("hello"),
        reinterpret_cast<const uint8_t*>("-456"),
        reinterpret_cast<const uint8_t*>("1e10")
    };
    size_t lengths[] = {3, 4, 5, 4, 4};

    size_t int_count, float_count, other_count;
    SIMDTypeValidator::validate_batch(fields, lengths, 5, int_count, float_count, other_count);

    EXPECT_EQ(int_count, 2);   // "123" and "-456"
    EXPECT_EQ(float_count, 2); // "3.14" and "1e10"
    EXPECT_EQ(other_count, 1); // "hello"
}

// =============================================================================
// SIMD DateTime Parser Tests
// =============================================================================

class SIMDDateTimeParserTest : public ::testing::Test {};

// Basic date parsing
TEST_F(SIMDDateTimeParserTest, ParseISODate) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15", 10);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.year, 2024);
    EXPECT_EQ(dt.month, 1);
    EXPECT_EQ(dt.day, 15);
}

TEST_F(SIMDDateTimeParserTest, ParseCompactDate) {
    auto result = SIMDDateTimeParser::parse_datetime("20240115", 8);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.year, 2024);
    EXPECT_EQ(dt.month, 1);
    EXPECT_EQ(dt.day, 15);
}

// Date and time
TEST_F(SIMDDateTimeParserTest, ParseDateTimeT) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45", 19);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.year, 2024);
    EXPECT_EQ(dt.month, 1);
    EXPECT_EQ(dt.day, 15);
    EXPECT_EQ(dt.hour, 14);
    EXPECT_EQ(dt.minute, 30);
    EXPECT_EQ(dt.second, 45);
}

TEST_F(SIMDDateTimeParserTest, ParseDateTimeSpace) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15 14:30:45", 19);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.hour, 14);
    EXPECT_EQ(dt.minute, 30);
    EXPECT_EQ(dt.second, 45);
}

// Fractional seconds
TEST_F(SIMDDateTimeParserTest, ParseFractionalSeconds) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45.123", 23);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.nanoseconds, 123000000);
}

TEST_F(SIMDDateTimeParserTest, ParseFractionalSecondsNano) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45.123456789", 29);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.nanoseconds, 123456789);
}

// Timezone
TEST_F(SIMDDateTimeParserTest, ParseTimezoneZ) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45Z", 20);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.tz_offset_minutes, 0);
}

TEST_F(SIMDDateTimeParserTest, ParseTimezonePositive) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45+05:30", 25);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.tz_offset_minutes, 5 * 60 + 30);
}

TEST_F(SIMDDateTimeParserTest, ParseTimezoneNegative) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45-08:00", 25);
    EXPECT_TRUE(result.ok());
    auto dt = result.get();
    EXPECT_EQ(dt.tz_offset_minutes, -(8 * 60));
}

// Date validation
TEST_F(SIMDDateTimeParserTest, InvalidMonth) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-13-15", 10);
    EXPECT_FALSE(result.ok());
}

TEST_F(SIMDDateTimeParserTest, InvalidDay) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-02-30", 10);
    EXPECT_FALSE(result.ok());
}

TEST_F(SIMDDateTimeParserTest, ValidLeapDay) {
    auto result = SIMDDateTimeParser::parse_datetime("2024-02-29", 10);
    EXPECT_TRUE(result.ok());
}

TEST_F(SIMDDateTimeParserTest, InvalidLeapDay) {
    auto result = SIMDDateTimeParser::parse_datetime("2023-02-29", 10);
    EXPECT_FALSE(result.ok());
}

// Timezone limit tests (UTC-12 to UTC+14)
TEST_F(SIMDDateTimeParserTest, TimezoneMaxPositive) {
    // UTC+14:00 (Line Islands, Kiribati)
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45+14:00", 25);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get().tz_offset_minutes, 14 * 60);
}

TEST_F(SIMDDateTimeParserTest, TimezoneMaxNegative) {
    // UTC-12:00 (Baker Island)
    auto result = SIMDDateTimeParser::parse_datetime("2024-01-15T14:30:45-12:00", 25);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get().tz_offset_minutes, -12 * 60);
}

// NA handling
TEST_F(SIMDDateTimeParserTest, EmptyIsNA) {
    auto result = SIMDDateTimeParser::parse_datetime("", 0);
    EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDDateTimeParserTest, WhitespaceIsNA) {
    auto result = SIMDDateTimeParser::parse_datetime("   ", 3);
    EXPECT_TRUE(result.is_na());
}

// Column parsing
TEST_F(SIMDDateTimeParserTest, ParseDateTimeColumn) {
    const char* fields[] = {"2024-01-15", "2024-02-20", "", "invalid"};
    size_t lengths[] = {10, 10, 0, 7};

    auto results = SIMDDateTimeParser::parse_datetime_column(fields, lengths, 4);

    EXPECT_EQ(results.size(), 4);
    EXPECT_TRUE(results[0].has_value());
    EXPECT_EQ(results[0]->month, 1);
    EXPECT_TRUE(results[1].has_value());
    EXPECT_EQ(results[1]->month, 2);
    EXPECT_FALSE(results[2].has_value());  // Empty
    EXPECT_FALSE(results[3].has_value());  // Invalid
}

// =============================================================================
// SIMDParseResult Tests
// =============================================================================

class SIMDParseResultTest : public ::testing::Test {};

TEST_F(SIMDParseResultTest, SuccessResult) {
    auto result = SIMDParseResult<int>::success(42);
    EXPECT_TRUE(result.ok());
    EXPECT_FALSE(result.is_na());
    EXPECT_EQ(result.get(), 42);
    EXPECT_EQ(result.get_or(0), 42);
}

TEST_F(SIMDParseResultTest, FailureResult) {
    auto result = SIMDParseResult<int>::failure("test error");
    EXPECT_FALSE(result.ok());
    EXPECT_FALSE(result.is_na());
    EXPECT_STREQ(result.error, "test error");
    EXPECT_EQ(result.get_or(99), 99);
}

TEST_F(SIMDParseResultTest, NAResult) {
    auto result = SIMDParseResult<int>::na();
    EXPECT_FALSE(result.ok());
    EXPECT_TRUE(result.is_na());
    EXPECT_EQ(result.error, nullptr);
    EXPECT_EQ(result.get_or(99), 99);
}

TEST_F(SIMDParseResultTest, ToExtractResult) {
    auto simd_result = SIMDParseResult<int64_t>::success(42);
    auto extract_result = simd_result.to_extract_result();
    EXPECT_TRUE(extract_result.ok());
    EXPECT_EQ(extract_result.get(), 42);
}

TEST_F(SIMDParseResultTest, GetThrowsOnFailure) {
    auto result = SIMDParseResult<int>::failure("error");
    EXPECT_THROW(result.get(), std::runtime_error);
}

// =============================================================================
// Performance comparison helpers (not benchmarks, just functional tests)
// =============================================================================

class SIMDPerformanceTest : public ::testing::Test {};

TEST_F(SIMDPerformanceTest, ParseManyIntegers) {
    // Test that we can parse many integers correctly
    std::vector<std::string> numbers;
    for (int i = -1000; i <= 1000; ++i) {
        numbers.push_back(std::to_string(i));
    }

    for (int i = -1000; i <= 1000; ++i) {
        const std::string& s = numbers[i + 1000];
        auto result = SIMDIntegerParser::parse_int64(s.c_str(), s.size());
        EXPECT_TRUE(result.ok()) << "Failed to parse: " << s;
        EXPECT_EQ(result.get(), i) << "Wrong value for: " << s;
    }
}

TEST_F(SIMDPerformanceTest, ParseManyDoubles) {
    // Test that we can parse various double formats correctly
    std::vector<std::pair<std::string, double>> test_cases = {
        {"0", 0.0},
        {"1", 1.0},
        {"-1", -1.0},
        {"0.5", 0.5},
        {"-0.5", -0.5},
        {"123.456", 123.456},
        {"1e5", 1e5},
        {"1e-5", 1e-5},
        {"1.5e10", 1.5e10},
        {"-1.5e-10", -1.5e-10}
    };

    for (const auto& [str, expected] : test_cases) {
        auto result = SIMDDoubleParser::parse_double(str.c_str(), str.size());
        EXPECT_TRUE(result.ok()) << "Failed to parse: " << str;
        EXPECT_NEAR(result.get(), expected, std::abs(expected) * 1e-10 + 1e-15)
            << "Wrong value for: " << str;
    }
}

// =============================================================================
// SIMD Value Extraction Integration Tests
// =============================================================================

class SIMDValueExtractionTest : public ::testing::Test {
protected:
    ExtractionConfig config_ = ExtractionConfig::defaults();
};

// Test parse_integer_simd with ExtractionConfig
TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDBasic) {
    auto result = parse_integer_simd<int64_t>("12345", 5, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDNegative) {
    auto result = parse_integer_simd<int64_t>("-12345", 6, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), -12345);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDWithWhitespace) {
    auto result = parse_integer_simd<int64_t>("  42  ", 6, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 42);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDNAValue) {
    auto result = parse_integer_simd<int64_t>("NA", 2, config_);
    EXPECT_TRUE(result.is_na());
    EXPECT_FALSE(result.ok());
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDEmptyIsNA) {
    auto result = parse_integer_simd<int64_t>("", 0, config_);
    EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDInt32) {
    auto result = parse_integer_simd<int32_t>("12345", 5, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDValueExtractionTest, ParseIntegerSIMDInt32Overflow) {
    auto result = parse_integer_simd<int32_t>("9999999999", 10, config_);
    EXPECT_FALSE(result.ok());
    EXPECT_NE(result.error, nullptr);
}

// Test parse_double_simd with ExtractionConfig
TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDBasic) {
    auto result = parse_double_simd("3.14159", 7, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 3.14159, 0.00001);
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDScientific) {
    auto result = parse_double_simd("1.5e10", 6, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 1.5e10, 1e5);
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDNaN) {
    auto result = parse_double_simd("NaN", 3, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(std::isnan(result.get()));
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDNaNNotTreatedAsNA) {
    // NaN should be parsed as the float value, not as NA
    auto result = parse_double_simd("NaN", 3, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_FALSE(result.is_na());
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDNAValue) {
    // Note: parse_double_simd doesn't check NA values (matching scalar behavior)
    // It returns a parse error, not NA
    auto result = parse_double_simd("NA", 2, config_);
    EXPECT_FALSE(result.ok());
    EXPECT_FALSE(result.is_na());  // It's a parse error, not NA
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDEmptyIsNA) {
    auto result = parse_double_simd("", 0, config_);
    EXPECT_TRUE(result.is_na());
}

TEST_F(SIMDValueExtractionTest, ParseDoubleSIMDWithWhitespace) {
    auto result = parse_double_simd("  3.14  ", 8, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 3.14, 0.001);
}

// Test extract_value_simd
TEST_F(SIMDValueExtractionTest, ExtractValueSIMDInt64) {
    auto result = extract_value_simd<int64_t>("12345", 5, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 12345);
}

TEST_F(SIMDValueExtractionTest, ExtractValueSIMDDouble) {
    auto result = extract_value_simd<double>("3.14", 4, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_NEAR(result.get(), 3.14, 0.001);
}

TEST_F(SIMDValueExtractionTest, ExtractValueSIMDBool) {
    auto result = extract_value_simd<bool>("true", 4, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_TRUE(result.get());
}

TEST_F(SIMDValueExtractionTest, ExtractValueSIMDInt32) {
    auto result = extract_value_simd<int32_t>("42", 2, config_);
    EXPECT_TRUE(result.ok());
    EXPECT_EQ(result.get(), 42);
}

// Test that SIMD and scalar produce equivalent results
TEST_F(SIMDValueExtractionTest, SIMDEquivalentToScalar) {
    std::vector<std::string> test_values = {
        "0", "1", "-1", "42", "-42",
        "12345", "-12345",
        "9223372036854775807",  // INT64_MAX
        "-9223372036854775808"  // INT64_MIN
    };

    for (const auto& value : test_values) {
        auto scalar_result = parse_integer<int64_t>(value.c_str(), value.size(), config_);
        auto simd_result = parse_integer_simd<int64_t>(value.c_str(), value.size(), config_);

        EXPECT_EQ(scalar_result.ok(), simd_result.ok()) << "Mismatch for: " << value;
        if (scalar_result.ok() && simd_result.ok()) {
            EXPECT_EQ(scalar_result.get(), simd_result.get()) << "Value mismatch for: " << value;
        }
    }
}

TEST_F(SIMDValueExtractionTest, SIMDDoubleEquivalentToScalar) {
    std::vector<std::string> test_values = {
        "0", "0.0", "1", "-1", "3.14", "-3.14",
        "1e10", "1e-10", "1.5e10", "-1.5e-10",
        "Inf", "-Inf", "Infinity", "-Infinity"
    };

    for (const auto& value : test_values) {
        auto scalar_result = parse_double(value.c_str(), value.size(), config_);
        auto simd_result = parse_double_simd(value.c_str(), value.size(), config_);

        EXPECT_EQ(scalar_result.ok(), simd_result.ok()) << "Mismatch for: " << value;
        if (scalar_result.ok() && simd_result.ok()) {
            if (std::isnan(scalar_result.get())) {
                EXPECT_TRUE(std::isnan(simd_result.get())) << "NaN mismatch for: " << value;
            } else if (std::isinf(scalar_result.get())) {
                EXPECT_TRUE(std::isinf(simd_result.get())) << "Inf mismatch for: " << value;
                EXPECT_EQ(std::signbit(scalar_result.get()), std::signbit(simd_result.get()))
                    << "Inf sign mismatch for: " << value;
            } else {
                EXPECT_NEAR(scalar_result.get(), simd_result.get(),
                            std::abs(scalar_result.get()) * 1e-10 + 1e-15)
                    << "Value mismatch for: " << value;
            }
        }
    }
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
