#include "libvroom/vroom.h"

#include <gtest/gtest.h>

using namespace libvroom;

class TimeParsingTest : public ::testing::Test {};

TEST_F(TimeParsingTest, BasicHHMMSS) {
  int64_t micros;
  ASSERT_TRUE(parse_time("14:30:00", micros));
  EXPECT_EQ(micros, 52200000000LL);
}

TEST_F(TimeParsingTest, Midnight) {
  int64_t micros;
  ASSERT_TRUE(parse_time("00:00:00", micros));
  EXPECT_EQ(micros, 0LL);
}

TEST_F(TimeParsingTest, EndOfDay) {
  int64_t micros;
  ASSERT_TRUE(parse_time("23:59:59", micros));
  EXPECT_EQ(micros, 86399000000LL);
}

TEST_F(TimeParsingTest, FractionalSeconds) {
  int64_t micros;
  ASSERT_TRUE(parse_time("23:59:59.999", micros));
  EXPECT_EQ(micros, 86399999000LL);
}

TEST_F(TimeParsingTest, FractionalMicroseconds) {
  int64_t micros;
  ASSERT_TRUE(parse_time("12:00:00.123456", micros));
  EXPECT_EQ(micros, 43200123456LL);
}

TEST_F(TimeParsingTest, AMPM_12Hour) {
  int64_t micros;
  ASSERT_TRUE(parse_time("2:15:30 PM", micros));
  EXPECT_EQ(micros, 51330000000LL);
}

TEST_F(TimeParsingTest, AMPM_12AM_IsMidnight) {
  int64_t micros;
  ASSERT_TRUE(parse_time("12:00:00 AM", micros));
  EXPECT_EQ(micros, 0LL);
}

TEST_F(TimeParsingTest, AMPM_12PM_IsNoon) {
  int64_t micros;
  ASSERT_TRUE(parse_time("12:00:00 PM", micros));
  EXPECT_EQ(micros, 43200000000LL);
}

TEST_F(TimeParsingTest, AMPM_Morning) {
  int64_t micros;
  ASSERT_TRUE(parse_time("9:30:00 AM", micros));
  EXPECT_EQ(micros, 34200000000LL);
}

TEST_F(TimeParsingTest, AMPM_LowercaseAM) {
  int64_t micros;
  ASSERT_TRUE(parse_time("9:30:00 am", micros));
  EXPECT_EQ(micros, 34200000000LL);
}

TEST_F(TimeParsingTest, AMPM_LowercasePM) {
  int64_t micros;
  ASSERT_TRUE(parse_time("2:15:30 pm", micros));
  EXPECT_EQ(micros, 51330000000LL);
}

TEST_F(TimeParsingTest, AMPM_TwoDigitHour) {
  int64_t micros;
  ASSERT_TRUE(parse_time("02:15:30 PM", micros));
  EXPECT_EQ(micros, 51330000000LL);
}

TEST_F(TimeParsingTest, HHMM_NoSeconds) {
  int64_t micros;
  ASSERT_TRUE(parse_time("14:30", micros));
  EXPECT_EQ(micros, 52200000000LL);
}

TEST_F(TimeParsingTest, InvalidHour) {
  int64_t micros;
  EXPECT_FALSE(parse_time("24:00:00", micros));
}

TEST_F(TimeParsingTest, InvalidMinute) {
  int64_t micros;
  EXPECT_FALSE(parse_time("12:60:00", micros));
}

TEST_F(TimeParsingTest, InvalidSecond) {
  int64_t micros;
  EXPECT_FALSE(parse_time("12:00:60", micros));
}

TEST_F(TimeParsingTest, EmptyString) {
  int64_t micros;
  EXPECT_FALSE(parse_time("", micros));
}

TEST_F(TimeParsingTest, NotATime) {
  int64_t micros;
  EXPECT_FALSE(parse_time("hello", micros));
  EXPECT_FALSE(parse_time("12345", micros));
}

TEST_F(TimeParsingTest, AMPM_InvalidHour) {
  int64_t micros;
  EXPECT_FALSE(parse_time("13:00:00 PM", micros));
  EXPECT_FALSE(parse_time("0:00:00 PM", micros));
}
