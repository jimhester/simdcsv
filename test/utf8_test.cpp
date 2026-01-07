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

// =============================================================================
// Comprehensive Codepoint Width Tests by Unicode Block
// =============================================================================

// -----------------------------------------------------------------------------
// Zero-width character blocks
// -----------------------------------------------------------------------------

TEST_F(Utf8Test, CodepointWidthC1ControlChars) {
  // C1 control characters (U+007F-U+009F) have width 0
  EXPECT_EQ(codepoint_width(0x7F), 0); // DEL
  EXPECT_EQ(codepoint_width(0x80), 0); // PAD
  EXPECT_EQ(codepoint_width(0x85), 0); // NEL
  EXPECT_EQ(codepoint_width(0x9F), 0); // APC
}

TEST_F(Utf8Test, CodepointWidthCombiningDiacriticalMarksExtended) {
  // U+1AB0-U+1AFF: Combining Diacritical Marks Extended
  EXPECT_EQ(codepoint_width(0x1AB0), 0); // Start of range
  EXPECT_EQ(codepoint_width(0x1AB5), 0); // Mid-range
  EXPECT_EQ(codepoint_width(0x1AFF), 0); // End of range
}

TEST_F(Utf8Test, CodepointWidthCombiningDiacriticalMarksSupplement) {
  // U+1DC0-U+1DFF: Combining Diacritical Marks Supplement
  EXPECT_EQ(codepoint_width(0x1DC0), 0); // Start of range
  EXPECT_EQ(codepoint_width(0x1DCF), 0); // Mid-range
  EXPECT_EQ(codepoint_width(0x1DFF), 0); // End of range
}

TEST_F(Utf8Test, CodepointWidthCombiningDiacriticalMarksForSymbols) {
  // U+20D0-U+20FF: Combining Diacritical Marks for Symbols
  EXPECT_EQ(codepoint_width(0x20D0), 0); // Combining left harpoon above
  EXPECT_EQ(codepoint_width(0x20E0), 0); // Combining enclosing circle backslash
  EXPECT_EQ(codepoint_width(0x20FF), 0); // End of range
}

TEST_F(Utf8Test, CodepointWidthCombiningHalfMarks) {
  // U+FE20-U+FE2F: Combining Half Marks
  EXPECT_EQ(codepoint_width(0xFE20), 0); // Combining ligature left half
  EXPECT_EQ(codepoint_width(0xFE26), 0); // Combining conjoining macron
  EXPECT_EQ(codepoint_width(0xFE2F), 0); // End of range
}

TEST_F(Utf8Test, CodepointWidthAllZeroWidthChars) {
  // Zero-width characters - comprehensive test
  EXPECT_EQ(codepoint_width(0x200B), 0); // Zero Width Space
  EXPECT_EQ(codepoint_width(0x200C), 0); // Zero Width Non-Joiner
  EXPECT_EQ(codepoint_width(0x200D), 0); // Zero Width Joiner
  EXPECT_EQ(codepoint_width(0x2060), 0); // Word Joiner
  EXPECT_EQ(codepoint_width(0xFEFF), 0); // Zero Width No-Break Space (BOM)
}

// -----------------------------------------------------------------------------
// Wide (2-column) character blocks - CJK and related
// -----------------------------------------------------------------------------

TEST_F(Utf8Test, CodepointWidthCJKRadicalsSupplement) {
  // U+2E80-U+2EFF: CJK Radicals Supplement
  EXPECT_EQ(codepoint_width(0x2E80), 2); // CJK Radical Repeat
  EXPECT_EQ(codepoint_width(0x2EC0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x2EFF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthKangxiRadicals) {
  // U+2F00-U+2FDF: Kangxi Radicals
  EXPECT_EQ(codepoint_width(0x2F00), 2); // Kangxi Radical One
  EXPECT_EQ(codepoint_width(0x2F70), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x2FD5), 2); // Kangxi Radical Flute (last defined)
  EXPECT_EQ(codepoint_width(0x2FDF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthIdeographicDescriptionChars) {
  // U+2FF0-U+2FFF: Ideographic Description Characters
  EXPECT_EQ(codepoint_width(0x2FF0), 2); // Ideographic Description Left to Right
  EXPECT_EQ(codepoint_width(0x2FF5), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x2FFB), 2); // Last defined in range
  EXPECT_EQ(codepoint_width(0x2FFF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKSymbolsAndPunctuation) {
  // U+3000-U+303F: CJK Symbols and Punctuation
  EXPECT_EQ(codepoint_width(0x3000), 2); // Ideographic Space
  EXPECT_EQ(codepoint_width(0x3001), 2); // Ideographic Comma
  EXPECT_EQ(codepoint_width(0x3002), 2); // Ideographic Full Stop
  EXPECT_EQ(codepoint_width(0x300A), 2); // Left Double Angle Bracket
  EXPECT_EQ(codepoint_width(0x303F), 2); // Ideographic Half Fill Space
}

TEST_F(Utf8Test, CodepointWidthBopomofo) {
  // U+3100-U+312F: Bopomofo
  EXPECT_EQ(codepoint_width(0x3100), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x3105), 2); // Bopomofo Letter B
  EXPECT_EQ(codepoint_width(0x3110), 2); // Bopomofo Letter D
  EXPECT_EQ(codepoint_width(0x312F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthHangulCompatibilityJamo) {
  // U+3130-U+318F: Hangul Compatibility Jamo
  EXPECT_EQ(codepoint_width(0x3130), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x3131), 2); // Hangul Letter Kiyeok
  EXPECT_EQ(codepoint_width(0x3160), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x318F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthKanbun) {
  // U+3190-U+319F: Kanbun (annotation marks for classical Chinese)
  EXPECT_EQ(codepoint_width(0x3190), 2); // Ideographic Annotation Linking Mark
  EXPECT_EQ(codepoint_width(0x3195), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x319F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthBopomofoExtended) {
  // U+31A0-U+31BF: Bopomofo Extended
  EXPECT_EQ(codepoint_width(0x31A0), 2); // Bopomofo Letter Bu
  EXPECT_EQ(codepoint_width(0x31B0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x31BF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKStrokes) {
  // U+31C0-U+31EF: CJK Strokes
  EXPECT_EQ(codepoint_width(0x31C0), 2); // CJK Stroke T
  EXPECT_EQ(codepoint_width(0x31D0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x31EF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthKatakanaPhoneticExtensions) {
  // U+31F0-U+31FF: Katakana Phonetic Extensions
  EXPECT_EQ(codepoint_width(0x31F0), 2); // Katakana Letter Small Ku
  EXPECT_EQ(codepoint_width(0x31F5), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x31FF), 2); // Katakana Letter Small Ro
}

TEST_F(Utf8Test, CodepointWidthEnclosedCJKLettersAndMonths) {
  // U+3200-U+32FF: Enclosed CJK Letters and Months
  EXPECT_EQ(codepoint_width(0x3200), 2); // Parenthesized Hangul Kiyeok
  EXPECT_EQ(codepoint_width(0x3220), 2); // Parenthesized Ideograph One
  EXPECT_EQ(codepoint_width(0x3280), 2); // Circled Ideograph One
  EXPECT_EQ(codepoint_width(0x32FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKCompatibility) {
  // U+3300-U+33FF: CJK Compatibility
  EXPECT_EQ(codepoint_width(0x3300), 2); // Square Apaato
  EXPECT_EQ(codepoint_width(0x3350), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x33FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKUnifiedIdeographsExtensionA) {
  // U+3400-U+4DBF: CJK Unified Ideographs Extension A
  EXPECT_EQ(codepoint_width(0x3400), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x4000), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x4DBF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthYijingHexagramSymbols) {
  // U+4DC0-U+4DFF: Yijing Hexagram Symbols
  EXPECT_EQ(codepoint_width(0x4DC0), 2); // Hexagram for the Creative Heaven
  EXPECT_EQ(codepoint_width(0x4DE0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x4DFF), 2); // Hexagram for Before Completion
}

TEST_F(Utf8Test, CodepointWidthYiSyllables) {
  // U+A000-U+A48F: Yi Syllables
  EXPECT_EQ(codepoint_width(0xA000), 2); // Yi Syllable It
  EXPECT_EQ(codepoint_width(0xA200), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xA48F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthYiRadicals) {
  // U+A490-U+A4CF: Yi Radicals
  EXPECT_EQ(codepoint_width(0xA490), 2); // Yi Radical Qot
  EXPECT_EQ(codepoint_width(0xA4B0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xA4CF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthHangulJamoExtendedA) {
  // U+A960-U+A97F: Hangul Jamo Extended-A
  EXPECT_EQ(codepoint_width(0xA960), 2); // Start of range
  EXPECT_EQ(codepoint_width(0xA970), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xA97F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthHangulJamoExtendedB) {
  // U+D7B0-U+D7FF: Hangul Jamo Extended-B
  EXPECT_EQ(codepoint_width(0xD7B0), 2); // Start of range
  EXPECT_EQ(codepoint_width(0xD7D0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xD7FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKCompatibilityIdeographs) {
  // U+F900-U+FAFF: CJK Compatibility Ideographs
  EXPECT_EQ(codepoint_width(0xF900), 2); // CJK Compatibility Ideograph F900
  EXPECT_EQ(codepoint_width(0xFA00), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xFAFF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthVerticalForms) {
  // U+FE10-U+FE1F: Vertical Forms
  EXPECT_EQ(codepoint_width(0xFE10), 2); // Presentation Form for Vertical Comma
  EXPECT_EQ(codepoint_width(0xFE15), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xFE1F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKCompatibilityForms) {
  // U+FE30-U+FE4F: CJK Compatibility Forms
  EXPECT_EQ(codepoint_width(0xFE30), 2); // Presentation Form for Vertical Two Dot Leader
  EXPECT_EQ(codepoint_width(0xFE40), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xFE4F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthSmallFormVariants) {
  // U+FE50-U+FE6F: Small Form Variants
  EXPECT_EQ(codepoint_width(0xFE50), 2); // Small Comma
  EXPECT_EQ(codepoint_width(0xFE60), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0xFE6F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthHalfwidthAndFullwidthFormsSecondRange) {
  // U+FFE0-U+FFE6: Second range of Halfwidth and Fullwidth Forms
  EXPECT_EQ(codepoint_width(0xFFE0), 2); // Fullwidth Cent Sign
  EXPECT_EQ(codepoint_width(0xFFE1), 2); // Fullwidth Pound Sign
  EXPECT_EQ(codepoint_width(0xFFE5), 2); // Fullwidth Yen Sign
  EXPECT_EQ(codepoint_width(0xFFE6), 2); // Fullwidth Won Sign
}

// -----------------------------------------------------------------------------
// Wide (2-column) character blocks - Supplementary planes
// -----------------------------------------------------------------------------

TEST_F(Utf8Test, CodepointWidthCJKExtensionB) {
  // U+20000-U+2FFFF: CJK Unified Ideographs Extension B-I and other supplementary CJK
  EXPECT_EQ(codepoint_width(0x20000), 2); // Start of Extension B
  EXPECT_EQ(codepoint_width(0x25000), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x2A700), 2); // Extension C
  EXPECT_EQ(codepoint_width(0x2FFFF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthCJKExtensionG_H_I) {
  // U+30000-U+3FFFF: CJK Unified Ideographs Extension G, H, I
  EXPECT_EQ(codepoint_width(0x30000), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x35000), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x3FFFF), 2); // End of range
}

// -----------------------------------------------------------------------------
// Wide (2-column) character blocks - Emoji and symbols
// -----------------------------------------------------------------------------

TEST_F(Utf8Test, CodepointWidthMiscellaneousSymbolsAndPictographs) {
  // U+1F300-U+1F5FF: Miscellaneous Symbols and Pictographs
  EXPECT_EQ(codepoint_width(0x1F300), 2); // Cyclone
  EXPECT_EQ(codepoint_width(0x1F3A0), 2); // Carousel Horse
  EXPECT_EQ(codepoint_width(0x1F4A0), 2); // Diamond Shape with Dot Inside
  EXPECT_EQ(codepoint_width(0x1F5FF), 2); // Moyai (statue)
}

TEST_F(Utf8Test, CodepointWidthOrnamentalDingbats) {
  // U+1F650-U+1F67F: Ornamental Dingbats
  EXPECT_EQ(codepoint_width(0x1F650), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1F660), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1F67F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthTransportAndMapSymbols) {
  // U+1F680-U+1F6FF: Transport and Map Symbols
  EXPECT_EQ(codepoint_width(0x1F680), 2); // Rocket
  EXPECT_EQ(codepoint_width(0x1F697), 2); // Automobile
  EXPECT_EQ(codepoint_width(0x1F6FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthAlchemicalSymbols) {
  // U+1F700-U+1F77F: Alchemical Symbols
  EXPECT_EQ(codepoint_width(0x1F700), 2); // Alchemical Symbol for Quintessence
  EXPECT_EQ(codepoint_width(0x1F740), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1F77F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthGeometricShapesExtended) {
  // U+1F780-U+1F7FF: Geometric Shapes Extended
  EXPECT_EQ(codepoint_width(0x1F780), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1F7C0), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1F7FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthSupplementalArrowsC) {
  // U+1F800-U+1F8FF: Supplemental Arrows-C
  EXPECT_EQ(codepoint_width(0x1F800), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1F850), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1F8FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthSupplementalSymbolsAndPictographs) {
  // U+1F900-U+1F9FF: Supplemental Symbols and Pictographs
  EXPECT_EQ(codepoint_width(0x1F900), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1F920), 2); // Cowboy Hat Face
  EXPECT_EQ(codepoint_width(0x1F9FF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthChessSymbols) {
  // U+1FA00-U+1FA6F: Chess Symbols
  EXPECT_EQ(codepoint_width(0x1FA00), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1FA30), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1FA6F), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthSymbolsAndPictographsExtendedA) {
  // U+1FA70-U+1FAFF: Symbols and Pictographs Extended-A
  EXPECT_EQ(codepoint_width(0x1FA70), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1FA80), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1FAFF), 2); // End of range
}

TEST_F(Utf8Test, CodepointWidthSymbolsForLegacyComputing) {
  // U+1FB00-U+1FBFF: Symbols for Legacy Computing
  EXPECT_EQ(codepoint_width(0x1FB00), 2); // Start of range
  EXPECT_EQ(codepoint_width(0x1FB80), 2); // Mid-range
  EXPECT_EQ(codepoint_width(0x1FBFF), 2); // End of range
}

// -----------------------------------------------------------------------------
// Default width (1-column) character tests
// -----------------------------------------------------------------------------

TEST_F(Utf8Test, CodepointWidthDefaultSingleWidth) {
  // Characters outside of any special range should have width 1
  EXPECT_EQ(codepoint_width(0x00A0), 1); // Non-breaking space (not in 0-width range)
  EXPECT_EQ(codepoint_width(0x00FF), 1); // Latin small letter y with diaeresis
  EXPECT_EQ(codepoint_width(0x0400), 1); // Cyrillic capital letter Ie with grave
  EXPECT_EQ(codepoint_width(0x0600), 1); // Arabic number sign
  EXPECT_EQ(codepoint_width(0x2000), 1); // En quad (general punctuation)
  EXPECT_EQ(codepoint_width(0x2100), 1); // Account of (Letterlike Symbols)
}

// -----------------------------------------------------------------------------
// Boundary tests
// -----------------------------------------------------------------------------

TEST_F(Utf8Test, CodepointWidthBoundaries) {
  // Test boundary conditions where ranges meet

  // Control character boundary
  EXPECT_EQ(codepoint_width(0x1F), 0); // Last control char
  EXPECT_EQ(codepoint_width(0x20), 1); // Space (first printable)

  // C1 control boundary
  EXPECT_EQ(codepoint_width(0x7E), 1); // Tilde (last ASCII printable)
  EXPECT_EQ(codepoint_width(0x7F), 0); // DEL (control)
  EXPECT_EQ(codepoint_width(0x9F), 0); // Last C1 control
  EXPECT_EQ(codepoint_width(0xA0), 1); // Non-breaking space (first Latin-1 Supplement)

  // Combining diacritical marks boundaries
  EXPECT_EQ(codepoint_width(0x02FF), 1); // Before Combining Diacritical Marks
  EXPECT_EQ(codepoint_width(0x0300), 0); // Start of Combining Diacritical Marks
  EXPECT_EQ(codepoint_width(0x036F), 0); // End of Combining Diacritical Marks
  EXPECT_EQ(codepoint_width(0x0370), 1); // Greek capital letter Heta (after range)

  // CJK Radicals Supplement boundaries
  EXPECT_EQ(codepoint_width(0x2E7F), 1); // Before CJK Radicals Supplement
  EXPECT_EQ(codepoint_width(0x2E80), 2); // Start of CJK Radicals Supplement
  EXPECT_EQ(codepoint_width(0x2EFF), 2); // End of CJK Radicals Supplement
  // Note: 0x2F00 is in Kangxi Radicals, still width 2

  // Fullwidth forms boundaries
  EXPECT_EQ(codepoint_width(0xFEFF), 0); // BOM (zero width)
  EXPECT_EQ(codepoint_width(0xFF00), 2); // Fullwidth exclamation mark
  EXPECT_EQ(codepoint_width(0xFF60), 2); // End of first fullwidth range
  EXPECT_EQ(codepoint_width(0xFF61), 1); // Halfwidth ideographic full stop (not wide)
}
