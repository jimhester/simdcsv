/**
 * @file utf8_test.cpp
 * @brief Tests for UTF-8 string utilities (display width and truncation).
 */

#include "utf8.h"

#include <gtest/gtest.h>

using namespace libvroom;

class Utf8Test : public ::testing::Test {};

// =============================================================================
// UTF-8 Decode Tests
// =============================================================================

TEST_F(Utf8Test, DecodeAscii) {
  uint32_t cp;
  std::string_view str = "ABC";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 'A');

  EXPECT_EQ(utf8_decode(str, 1, cp), 1);
  EXPECT_EQ(cp, 'B');

  EXPECT_EQ(utf8_decode(str, 2, cp), 1);
  EXPECT_EQ(cp, 'C');
}

TEST_F(Utf8Test, DecodeTwoByteSequence) {
  uint32_t cp;
  // ñ (U+00F1) is encoded as C3 B1
  std::string_view str = "ñ";

  EXPECT_EQ(utf8_decode(str, 0, cp), 2);
  EXPECT_EQ(cp, 0x00F1);
}

TEST_F(Utf8Test, DecodeThreeByteSequence) {
  uint32_t cp;
  // 日 (U+65E5) is encoded as E6 97 A5
  std::string_view str = "日";

  EXPECT_EQ(utf8_decode(str, 0, cp), 3);
  EXPECT_EQ(cp, 0x65E5);
}

TEST_F(Utf8Test, DecodeFourByteSequence) {
  uint32_t cp;
  // 🎉 (U+1F389) is encoded as F0 9F 8E 89
  std::string_view str = "🎉";

  EXPECT_EQ(utf8_decode(str, 0, cp), 4);
  EXPECT_EQ(cp, 0x1F389);
}

TEST_F(Utf8Test, DecodeInvalidSequence) {
  uint32_t cp;
  // Invalid continuation byte (0x80 alone)
  std::string str = "\x80";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD); // Replacement character
}

TEST_F(Utf8Test, DecodeTruncatedSequence) {
  uint32_t cp;
  // Truncated 3-byte sequence (only first byte)
  std::string str = "\xE6";

  EXPECT_EQ(utf8_decode(str, 0, cp), 1);
  EXPECT_EQ(cp, 0xFFFD); // Replacement character
}

// =============================================================================
// Codepoint Width Tests
// =============================================================================

TEST_F(Utf8Test, CodepointWidthAscii) {
  // ASCII characters are width 1
  EXPECT_EQ(codepoint_width('A'), 1);
  EXPECT_EQ(codepoint_width('z'), 1);
  EXPECT_EQ(codepoint_width('0'), 1);
  EXPECT_EQ(codepoint_width(' '), 1);
}

TEST_F(Utf8Test, CodepointWidthControlChars) {
  // Control characters have width 0
  EXPECT_EQ(codepoint_width('\0'), 0);
  EXPECT_EQ(codepoint_width('\t'), 0);
  EXPECT_EQ(codepoint_width('\n'), 0);
  EXPECT_EQ(codepoint_width('\r'), 0);
}

TEST_F(Utf8Test, CodepointWidthCJK) {
  // CJK characters are width 2
  EXPECT_EQ(codepoint_width(0x65E5), 2); // 日
  EXPECT_EQ(codepoint_width(0x672C), 2); // 本
  EXPECT_EQ(codepoint_width(0x8A9E), 2); // 語
}

TEST_F(Utf8Test, CodepointWidthHiragana) {
  // Hiragana characters are width 2
  EXPECT_EQ(codepoint_width(0x3042), 2); // あ
  EXPECT_EQ(codepoint_width(0x3044), 2); // い
}

TEST_F(Utf8Test, CodepointWidthKatakana) {
  // Katakana characters are width 2
  EXPECT_EQ(codepoint_width(0x30A2), 2); // ア
  EXPECT_EQ(codepoint_width(0x30A4), 2); // イ
}

TEST_F(Utf8Test, CodepointWidthEmoji) {
  // Emoji are width 2
  EXPECT_EQ(codepoint_width(0x1F389), 2); // 🎉
  EXPECT_EQ(codepoint_width(0x1F600), 2); // 😀
  EXPECT_EQ(codepoint_width(0x1F30D), 2); // 🌍
}

TEST_F(Utf8Test, CodepointWidthCombiningMark) {
  // Combining marks have width 0
  EXPECT_EQ(codepoint_width(0x0301), 0); // Combining acute accent
  EXPECT_EQ(codepoint_width(0x0308), 0); // Combining diaeresis
}

TEST_F(Utf8Test, CodepointWidthZeroWidthChars) {
  // Zero-width characters
  EXPECT_EQ(codepoint_width(0x200B), 0); // Zero Width Space
  EXPECT_EQ(codepoint_width(0x200D), 0); // Zero Width Joiner
  EXPECT_EQ(codepoint_width(0xFEFF), 0); // BOM
}

// =============================================================================
// UTF-8 Display Width Tests
// =============================================================================

TEST_F(Utf8Test, DisplayWidthAscii) {
  EXPECT_EQ(utf8_display_width("Hello"), 5);
  EXPECT_EQ(utf8_display_width(""), 0);
  EXPECT_EQ(utf8_display_width("A"), 1);
}

TEST_F(Utf8Test, DisplayWidthCJK) {
  // Each CJK character is 2 columns
  EXPECT_EQ(utf8_display_width("日本語"), 6); // 3 chars * 2 = 6
}

TEST_F(Utf8Test, DisplayWidthMixed) {
  // "Hello世界" = 5 ASCII + 2 CJK = 5*1 + 2*2 = 9
  EXPECT_EQ(utf8_display_width("Hello世界"), 9);
}

TEST_F(Utf8Test, DisplayWidthEmoji) {
  // Single emoji is 2 columns
  EXPECT_EQ(utf8_display_width("🎉"), 2);
  EXPECT_EQ(utf8_display_width("🎉🎊"), 4);
}

TEST_F(Utf8Test, DisplayWidthWithCombiningMarks) {
  // "é" as e + combining accent = 1 + 0 = 1
  std::string e_accent = "e\xCC\x81"; // e + combining acute
  EXPECT_EQ(utf8_display_width(e_accent), 1);
}

// =============================================================================
// UTF-8 Truncate Tests
// =============================================================================

TEST_F(Utf8Test, TruncateAsciiNoTruncation) {
  // String fits, no truncation needed
  EXPECT_EQ(utf8_truncate("Hello", 10), "Hello");
  EXPECT_EQ(utf8_truncate("Hello", 5), "Hello");
}

TEST_F(Utf8Test, TruncateAsciiWithEllipsis) {
  // String too long, truncate with ellipsis
  std::string result = utf8_truncate("Hello World", 8);
  EXPECT_EQ(result, "Hello...");
  EXPECT_EQ(utf8_display_width(result), 8);
}

TEST_F(Utf8Test, TruncateAsciiTooShortForEllipsis) {
  // Max width too short for ellipsis
  std::string result = utf8_truncate("Hello", 2);
  EXPECT_EQ(result, "He");
  EXPECT_EQ(utf8_display_width(result), 2);
}

TEST_F(Utf8Test, TruncateCJK) {
  // CJK characters are 2 columns each
  // "日本語" = 6 columns, truncate to 5 should give "日..."
  std::string result = utf8_truncate("日本語", 5);
  EXPECT_EQ(result, "日...");
  EXPECT_EQ(utf8_display_width(result), 5);
}

TEST_F(Utf8Test, TruncateCJKExact) {
  // Truncate to 4 should give "..." only (no room for even one 2-col char + ellipsis)
  // Actually: 4 columns allows for 1 CJK char (2 cols) + ... (3 cols) = 5, too big
  // So we get just "..." with display width 3, but wait we have 4 columns
  // Let me recalculate: max_width=4, target_width=1 (4-3)
  // Can fit 0 CJK chars (each is 2), so result is "..."
  std::string result = utf8_truncate("日本語", 4);
  // We can only fit "..." since target_width is 1, and CJK needs 2
  EXPECT_EQ(result, "...");
  EXPECT_EQ(utf8_display_width(result), 3);
}

TEST_F(Utf8Test, TruncateEmoji) {
  // Emoji are 4 bytes but 2 display columns
  std::string input = "Hello🎉World";
  // "Hello" = 5, "🎉" = 2, "World" = 5, total = 12
  // Truncate to 10: we can fit "Hello🎉" (7) + "..." (3) = 10
  std::string result = utf8_truncate(input, 10);
  EXPECT_EQ(result, "Hello🎉...");
  EXPECT_EQ(utf8_display_width(result), 10);
}

TEST_F(Utf8Test, TruncateDoesNotSplitMultibyte) {
  // Ensure we don't split a multi-byte sequence
  // "日本語" = 6 columns (3 CJK chars * 2), truncate to 4
  // Can't fit "日" (2) + "..." (3) = 5 > 4
  // So we get "..." only
  std::string result = utf8_truncate("日本語", 4);
  EXPECT_EQ(result, "...");
}

TEST_F(Utf8Test, TruncateZeroWidth) {
  EXPECT_EQ(utf8_truncate("Hello", 0), "");
}

TEST_F(Utf8Test, TruncateMixedContent) {
  // "Hello世界🌍日本語テスト" - mixed ASCII, CJK, emoji
  std::string input = "Hello世界🌍日本語テスト";
  // Let's truncate to 15 columns
  // H(1) e(1) l(1) l(1) o(1) 世(2) 界(2) 🌍(2) = 11
  // Next would be 日(2) = 13, 本(2) = 15 - but we need room for ...
  // Target: 15 - 3 = 12 columns
  // H(1) e(1) l(1) l(1) o(1) 世(2) 界(2) 🌍(2) = 11, then 日 would make 13 > 12
  // So we get "Hello世界🌍..."
  std::string result = utf8_truncate(input, 15);
  EXPECT_EQ(result, "Hello世界🌍...");
  EXPECT_EQ(utf8_display_width(result), 14); // 11 + 3 = 14
}

TEST_F(Utf8Test, TruncateLongAsciiField) {
  // Simulate the original bug scenario with ASCII that ends with emoji
  std::string input = "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJ🎉🎊";
  // 36 ASCII chars + 2 emoji = 36 + 4 = 40 columns
  // Truncate to 40 (MAX_COLUMN_WIDTH) should be exact fit if width is exactly 40
  // Actually: 36*1 + 2*2 = 40, exactly fits
  EXPECT_EQ(utf8_display_width(input), 40);

  // Should not truncate if it fits exactly
  EXPECT_EQ(utf8_truncate(input, 40), input);

  // Truncate to 39: need to truncate
  std::string result = utf8_truncate(input, 39);
  // Target: 39 - 3 = 36 columns
  // Can fit all 36 ASCII chars exactly
  EXPECT_EQ(result, "ABCDEFGHIJKLMNOPQRSTUVWXYZABCDEFGHIJ...");
  EXPECT_EQ(utf8_display_width(result), 39);
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_F(Utf8Test, EmptyString) {
  EXPECT_EQ(utf8_display_width(""), 0);
  EXPECT_EQ(utf8_truncate("", 10), "");
}

TEST_F(Utf8Test, SingleCharacter) {
  EXPECT_EQ(utf8_truncate("A", 1), "A");
  EXPECT_EQ(utf8_truncate("日", 2), "日");
  EXPECT_EQ(utf8_truncate("🎉", 2), "🎉");
}

TEST_F(Utf8Test, TruncateExactFit) {
  // String exactly fits, no truncation
  EXPECT_EQ(utf8_truncate("Hello", 5), "Hello");
  EXPECT_EQ(utf8_truncate("日本", 4), "日本");
}

TEST_F(Utf8Test, FullwidthForms) {
  // Fullwidth ASCII (U+FF01-U+FF5E) should be width 2
  // Ａ (U+FF21) is fullwidth A
  EXPECT_EQ(codepoint_width(0xFF21), 2);
}

TEST_F(Utf8Test, HangulSyllables) {
  // Korean Hangul syllables (U+AC00-U+D7AF) should be width 2
  // 한 (U+D55C)
  EXPECT_EQ(codepoint_width(0xD55C), 2);
  EXPECT_EQ(utf8_display_width("한글"), 4); // 2 chars * 2 = 4
}
