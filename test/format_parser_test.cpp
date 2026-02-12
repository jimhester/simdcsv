#include "libvroom/format_parser.h"

#include <gtest/gtest.h>

using namespace libvroom;

// ============================================================================
// ParsedDateTime epoch conversion tests
// ============================================================================

class ParsedDateTimeTest : public ::testing::Test {};

TEST_F(ParsedDateTimeTest, UnixEpoch) {
  ParsedDateTime dt{1970, 1, 1, 0, 0, 0, 0.0, 0};
  EXPECT_EQ(dt.to_epoch_days(), 0);
  EXPECT_EQ(dt.to_epoch_micros(), 0);
}

TEST_F(ParsedDateTimeTest, Y2K) {
  ParsedDateTime dt{2000, 1, 1, 0, 0, 0, 0.0, 0};
  EXPECT_EQ(dt.to_epoch_days(), 10957);
  EXPECT_EQ(dt.to_epoch_micros(), 10957LL * 86400LL * 1000000LL);
}

TEST_F(ParsedDateTimeTest, LeapYearFeb29) {
  ParsedDateTime dt{2024, 2, 29, 0, 0, 0, 0.0, 0};
  EXPECT_EQ(dt.to_epoch_days(), 19782);
}

TEST_F(ParsedDateTimeTest, DateBeforeEpoch) {
  ParsedDateTime dt{1969, 12, 31, 0, 0, 0, 0.0, 0};
  EXPECT_EQ(dt.to_epoch_days(), -1);
}

TEST_F(ParsedDateTimeTest, TimeComponents) {
  ParsedDateTime dt{1970, 1, 1, 14, 30, 45, 0.0, 0};
  int64_t expected = (14LL * 3600 + 30 * 60 + 45) * 1000000LL;
  EXPECT_EQ(dt.to_epoch_micros(), expected);
}

TEST_F(ParsedDateTimeTest, FractionalSeconds) {
  ParsedDateTime dt{1970, 1, 1, 0, 0, 0, 0.5, 0};
  EXPECT_EQ(dt.to_epoch_micros(), 500000LL);
}

TEST_F(ParsedDateTimeTest, TimezoneOffset) {
  ParsedDateTime dt{2024, 1, 1, 0, 0, 0, 0.0, 330};
  int64_t base = 19723LL * 86400LL * 1000000LL;
  int64_t offset = 330LL * 60LL * 1000000LL;
  EXPECT_EQ(dt.to_epoch_micros(), base - offset);
}

TEST_F(ParsedDateTimeTest, NegativeTimezoneOffset) {
  ParsedDateTime dt{2024, 1, 1, 0, 0, 0, 0.0, -300};
  int64_t base = 19723LL * 86400LL * 1000000LL;
  int64_t offset = -300LL * 60LL * 1000000LL;
  EXPECT_EQ(dt.to_epoch_micros(), base - offset);
}

TEST_F(ParsedDateTimeTest, SecondsSinceMidnight) {
  ParsedDateTime dt{0, 0, 0, 14, 30, 45, 0.123456, 0};
  int64_t expected = (14LL * 3600 + 30 * 60 + 45) * 1000000LL + 123456;
  EXPECT_EQ(dt.to_seconds_since_midnight_micros(), expected);
}

TEST_F(ParsedDateTimeTest, MidnightTime) {
  ParsedDateTime dt{0, 0, 0, 0, 0, 0, 0.0, 0};
  EXPECT_EQ(dt.to_seconds_since_midnight_micros(), 0);
}

// ============================================================================
// FormatLocale tests
// ============================================================================

class FormatLocaleTest : public ::testing::Test {};

TEST_F(FormatLocaleTest, DefaultEnglishMonthNames) {
  FormatLocale locale = FormatLocale::english();
  EXPECT_EQ(locale.month_names[0], "January");
  EXPECT_EQ(locale.month_names[11], "December");
  EXPECT_EQ(locale.month_abbrev[0], "Jan");
  EXPECT_EQ(locale.month_abbrev[11], "Dec");
}

TEST_F(FormatLocaleTest, DefaultEnglishDayNames) {
  FormatLocale locale = FormatLocale::english();
  EXPECT_EQ(locale.day_names[0], "Sunday");
  EXPECT_EQ(locale.day_names[6], "Saturday");
  EXPECT_EQ(locale.day_abbrev[0], "Sun");
  EXPECT_EQ(locale.day_abbrev[6], "Sat");
}

TEST_F(FormatLocaleTest, DefaultEnglishAmPm) {
  FormatLocale locale = FormatLocale::english();
  EXPECT_EQ(locale.am, "AM");
  EXPECT_EQ(locale.pm, "PM");
}

// ============================================================================
// FormatParser tests
// ============================================================================

class FormatParserTest : public ::testing::Test {
protected:
  FormatLocale locale = FormatLocale::english();
};

// --- Date formats ---

TEST_F(FormatParserTest, ISO8601Date) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-03-15", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
}

TEST_F(FormatParserTest, EuropeanDate) {
  FormatParser parser("%d/%m/%Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("15/03/2024", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
}

TEST_F(FormatParserTest, USDate) {
  FormatParser parser("%m/%d/%Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("03/15/2024", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
}

TEST_F(FormatParserTest, TwoDigitYear) {
  FormatParser parser("%m/%d/%y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("03/15/24", dt));
  EXPECT_EQ(dt.year, 2024);

  ParsedDateTime dt2;
  ASSERT_TRUE(parser.parse("03/15/99", dt2));
  EXPECT_EQ(dt2.year, 1999);
}

TEST_F(FormatParserTest, AbbreviatedMonthName) {
  FormatParser parser("%d-%b-%Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("15-Mar-2024", dt));
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.year, 2024);
}

TEST_F(FormatParserTest, FullMonthName) {
  FormatParser parser("%B %d, %Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("March 15, 2024", dt));
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.year, 2024);
}

TEST_F(FormatParserTest, AbbreviatedDayName) {
  FormatParser parser("%a, %d %b %Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("Fri, 15 Mar 2024", dt));
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.year, 2024);
}

TEST_F(FormatParserTest, FullDayName) {
  FormatParser parser("%A, %B %d, %Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("Friday, March 15, 2024", dt));
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.year, 2024);
}

TEST_F(FormatParserTest, DayWithLeadingSpace) {
  FormatParser parser("%Y-%m-%e", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-03- 5", dt));
  EXPECT_EQ(dt.day, 5);

  ParsedDateTime dt2;
  ASSERT_TRUE(parser.parse("2024-03-15", dt2));
  EXPECT_EQ(dt2.day, 15);
}

// --- Time formats ---

TEST_F(FormatParserTest, BasicTime24h) {
  FormatParser parser("%H:%M:%S", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("14:30:45", dt));
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
  EXPECT_EQ(dt.second, 45);
}

TEST_F(FormatParserTest, Time12hAMPM) {
  FormatParser parser("%I:%M %p", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("02:30 PM", dt));
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
}

TEST_F(FormatParserTest, Time12hAM) {
  FormatParser parser("%I:%M %p", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("09:15 AM", dt));
  EXPECT_EQ(dt.hour, 9);
  EXPECT_EQ(dt.minute, 15);
}

TEST_F(FormatParserTest, Time12Noon) {
  FormatParser parser("%I:%M %p", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("12:00 PM", dt));
  EXPECT_EQ(dt.hour, 12);
}

TEST_F(FormatParserTest, Time12Midnight) {
  FormatParser parser("%I:%M %p", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("12:00 AM", dt));
  EXPECT_EQ(dt.hour, 0);
}

TEST_F(FormatParserTest, FractionalSecondsOS) {
  FormatParser parser("%H:%M:%OS", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("14:30:45.123", dt));
  EXPECT_EQ(dt.second, 45);
  EXPECT_NEAR(dt.fractional_seconds, 0.123, 0.0001);
}

TEST_F(FormatParserTest, FractionalSecondsMicros) {
  FormatParser parser("%H:%M:%OS", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("14:30:45.123456", dt));
  EXPECT_EQ(dt.second, 45);
  EXPECT_NEAR(dt.fractional_seconds, 0.123456, 0.000001);
}

TEST_F(FormatParserTest, FractionalSecondsNoFrac) {
  FormatParser parser("%H:%M:%OS", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("14:30:45", dt));
  EXPECT_EQ(dt.second, 45);
  EXPECT_DOUBLE_EQ(dt.fractional_seconds, 0.0);
}

// --- Timezone ---

TEST_F(FormatParserTest, TimezoneOffsetPositive) {
  FormatParser parser("%Y-%m-%d %H:%M:%S%z", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-01-01 00:00:00+0530", dt));
  EXPECT_EQ(dt.tz_offset_minutes, 330);
}

TEST_F(FormatParserTest, TimezoneOffsetNegative) {
  FormatParser parser("%Y-%m-%d %H:%M:%S%z", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-01-01 00:00:00-0500", dt));
  EXPECT_EQ(dt.tz_offset_minutes, -300);
}

TEST_F(FormatParserTest, TimezoneOffsetWithColon) {
  FormatParser parser("%Y-%m-%d %H:%M:%S%z", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-01-01 00:00:00+05:30", dt));
  EXPECT_EQ(dt.tz_offset_minutes, 330);
}

TEST_F(FormatParserTest, TimezoneZ) {
  FormatParser parser("%Y-%m-%d %H:%M:%S%z", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-01-01 00:00:00Z", dt));
  EXPECT_EQ(dt.tz_offset_minutes, 0);
}

// --- Literal percent ---

TEST_F(FormatParserTest, LiteralPercent) {
  FormatParser parser("%%date: %Y-%m-%d", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("%date: 2024-03-15", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
}

// --- Datetime combinations ---

TEST_F(FormatParserTest, FullDatetime) {
  FormatParser parser("%Y-%m-%d %H:%M:%S", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-03-15 14:30:45", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
  EXPECT_EQ(dt.second, 45);
}

TEST_F(FormatParserTest, DatetimeWithMonthName) {
  FormatParser parser("%d %b %Y %I:%M %p", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("15 Mar 2024 02:30 PM", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
}

// --- Error cases ---

TEST_F(FormatParserTest, MismatchedLiteral) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  EXPECT_FALSE(parser.parse("2024/03/15", dt));
}

TEST_F(FormatParserTest, TruncatedInput) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  EXPECT_FALSE(parser.parse("2024-03", dt));
}

TEST_F(FormatParserTest, InvalidMonth) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  EXPECT_FALSE(parser.parse("2024-13-01", dt));
}

TEST_F(FormatParserTest, InvalidDay) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  EXPECT_FALSE(parser.parse("2024-02-30", dt));
}

TEST_F(FormatParserTest, NonLeapYear) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  EXPECT_FALSE(parser.parse("2023-02-29", dt));
}

TEST_F(FormatParserTest, TrailingGarbage) {
  FormatParser parser("%Y-%m-%d", locale);
  ParsedDateTime dt;
  EXPECT_FALSE(parser.parse("2024-03-15 extra", dt));
}

TEST_F(FormatParserTest, CaseInsensitiveMonthName) {
  FormatParser parser("%d-%b-%Y", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("15-mar-2024", dt));
  EXPECT_EQ(dt.month, 3);
}

TEST_F(FormatParserTest, CaseInsensitiveAMPM) {
  FormatParser parser("%I:%M %p", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("02:30 pm", dt));
  EXPECT_EQ(dt.hour, 14);
}

// --- Non-English locale ---

TEST_F(FormatParserTest, FrenchMonthNames) {
  FormatLocale fr;
  fr.month_abbrev = {"janv.", "fevr.", "mars",  "avr.", "mai",  "juin",
                     "juil.", "aout",  "sept.", "oct.", "nov.", "dec."};
  fr.month_names = {"janvier", "fevrier", "mars",      "avril",   "mai",      "juin",
                    "juillet", "aout",    "septembre", "octobre", "novembre", "decembre"};
  fr.day_names = {"dimanche", "lundi", "mardi", "mercredi", "jeudi", "vendredi", "samedi"};
  fr.day_abbrev = {"dim.", "lun.", "mar.", "mer.", "jeu.", "ven.", "sam."};
  fr.am = "AM";
  fr.pm = "PM";

  FormatParser parser("%d %B %Y", fr);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("15 mars 2024", dt));
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
  EXPECT_EQ(dt.year, 2024);
}

// --- Compound specifiers ---

TEST_F(FormatParserTest, CompoundD) {
  FormatParser parser("%D", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("03/15/24", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
}

TEST_F(FormatParserTest, CompoundF) {
  FormatParser parser("%F", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("2024-03-15", dt));
  EXPECT_EQ(dt.year, 2024);
  EXPECT_EQ(dt.month, 3);
  EXPECT_EQ(dt.day, 15);
}

TEST_F(FormatParserTest, CompoundT) {
  FormatParser parser("%T", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("14:30:45", dt));
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
  EXPECT_EQ(dt.second, 45);
}

TEST_F(FormatParserTest, CompoundR) {
  FormatParser parser("%R", locale);
  ParsedDateTime dt;
  ASSERT_TRUE(parser.parse("14:30", dt));
  EXPECT_EQ(dt.hour, 14);
  EXPECT_EQ(dt.minute, 30);
}

// ============================================================================
// Integration: FormatParser + ArrowColumnBuilder
// ============================================================================

#include "libvroom/arrow_column_builder.h"

class FormatBuilderIntegrationTest : public ::testing::Test {
protected:
  FormatLocale locale = FormatLocale::english();
};

TEST_F(FormatBuilderIntegrationTest, DateColumnWithFormat) {
  auto parser = std::make_shared<const FormatParser>("%d/%m/%Y", locale);
  auto builder = ArrowColumnBuilder::create_date(parser);
  auto ctx = builder->create_context();

  ctx.append("15/03/2024");
  ctx.append("01/01/1970");
  ctx.append_null();

  EXPECT_EQ(builder->size(), 3);
  auto& values = static_cast<ArrowDateColumnBuilder&>(*builder).values();
  EXPECT_EQ(values.get(0), 19797); // 2024-03-15
  EXPECT_EQ(values.get(1), 0);     // 1970-01-01
}

TEST_F(FormatBuilderIntegrationTest, TimestampColumnWithFormat) {
  auto parser = std::make_shared<const FormatParser>("%d/%m/%Y %H:%M:%S", locale);
  auto builder = ArrowColumnBuilder::create_timestamp(parser);
  auto ctx = builder->create_context();

  ctx.append("01/01/1970 00:00:00");
  EXPECT_EQ(builder->size(), 1);
  auto& values = static_cast<ArrowTimestampColumnBuilder&>(*builder).values();
  EXPECT_EQ(values.get(0), 0);
}

TEST_F(FormatBuilderIntegrationTest, TimeColumnWithFormat) {
  auto parser = std::make_shared<const FormatParser>("%I:%M %p", locale);
  auto builder = ArrowColumnBuilder::create_time(parser);
  auto ctx = builder->create_context();

  ctx.append("02:30 PM");
  EXPECT_EQ(builder->size(), 1);
  auto& values = static_cast<ArrowTimeColumnBuilder&>(*builder).values();
  EXPECT_EQ(values.get(0), 52200000000LL);
}

TEST_F(FormatBuilderIntegrationTest, FormatParseErrorBecomesNull) {
  auto parser = std::make_shared<const FormatParser>("%Y-%m-%d", locale);
  auto builder = ArrowColumnBuilder::create_date(parser);
  auto ctx = builder->create_context();

  ctx.append("not-a-date");
  EXPECT_EQ(builder->size(), 1);
  EXPECT_EQ(builder->null_count(), 1);
}
