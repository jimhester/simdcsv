/**
 * @file utf8.cpp
 * @brief Implementation of UTF-8 string utilities.
 */

#include "utf8.h"

namespace libvroom {

size_t utf8_decode(std::string_view str, size_t pos, uint32_t& codepoint) {
  if (pos >= str.size()) {
    codepoint = 0xFFFD; // Replacement character
    return 0;
  }

  uint8_t byte = static_cast<uint8_t>(str[pos]);

  // ASCII (0xxxxxxx)
  if ((byte & 0x80) == 0) {
    codepoint = byte;
    return 1;
  }

  // Determine sequence length and initial bits
  size_t len;
  uint32_t cp;

  if ((byte & 0xE0) == 0xC0) {
    // Two-byte sequence (110xxxxx)
    len = 2;
    cp = byte & 0x1F;
  } else if ((byte & 0xF0) == 0xE0) {
    // Three-byte sequence (1110xxxx)
    len = 3;
    cp = byte & 0x0F;
  } else if ((byte & 0xF8) == 0xF0) {
    // Four-byte sequence (11110xxx)
    len = 4;
    cp = byte & 0x07;
  } else {
    // Invalid leading byte or continuation byte
    codepoint = 0xFFFD;
    return 1;
  }

  // Check if we have enough bytes
  if (pos + len > str.size()) {
    codepoint = 0xFFFD;
    return 1;
  }

  // Decode continuation bytes (10xxxxxx)
  for (size_t i = 1; i < len; ++i) {
    uint8_t cont = static_cast<uint8_t>(str[pos + i]);
    if ((cont & 0xC0) != 0x80) {
      // Invalid continuation byte
      codepoint = 0xFFFD;
      return 1;
    }
    cp = (cp << 6) | (cont & 0x3F);
  }

  // Validate code point
  // Check for overlong encodings
  if ((len == 2 && cp < 0x80) || (len == 3 && cp < 0x800) || (len == 4 && cp < 0x10000)) {
    codepoint = 0xFFFD;
    return len;
  }

  // Check for surrogates (U+D800-U+DFFF) and too large
  if ((cp >= 0xD800 && cp <= 0xDFFF) || cp > 0x10FFFF) {
    codepoint = 0xFFFD;
    return len;
  }

  codepoint = cp;
  return len;
}

int codepoint_width(uint32_t cp) {
  // Control characters and combining marks have zero width
  if (cp < 0x20 || (cp >= 0x7F && cp < 0xA0)) {
    return 0;
  }

  // Combining diacritical marks and other zero-width characters
  // U+0300-U+036F: Combining Diacritical Marks
  // U+1AB0-U+1AFF: Combining Diacritical Marks Extended
  // U+1DC0-U+1DFF: Combining Diacritical Marks Supplement
  // U+20D0-U+20FF: Combining Diacritical Marks for Symbols
  // U+FE20-U+FE2F: Combining Half Marks
  if ((cp >= 0x0300 && cp <= 0x036F) || (cp >= 0x1AB0 && cp <= 0x1AFF) ||
      (cp >= 0x1DC0 && cp <= 0x1DFF) || (cp >= 0x20D0 && cp <= 0x20FF) ||
      (cp >= 0xFE20 && cp <= 0xFE2F)) {
    return 0;
  }

  // Zero-width characters
  // U+200B: Zero Width Space
  // U+200C: Zero Width Non-Joiner
  // U+200D: Zero Width Joiner
  // U+2060: Word Joiner
  // U+FEFF: Zero Width No-Break Space (BOM)
  if (cp == 0x200B || cp == 0x200C || cp == 0x200D || cp == 0x2060 || cp == 0xFEFF) {
    return 0;
  }

  // Wide characters (2 columns)

  // CJK Radicals Supplement
  if (cp >= 0x2E80 && cp <= 0x2EFF)
    return 2;

  // Kangxi Radicals
  if (cp >= 0x2F00 && cp <= 0x2FDF)
    return 2;

  // Ideographic Description Characters
  if (cp >= 0x2FF0 && cp <= 0x2FFF)
    return 2;

  // CJK Symbols and Punctuation
  if (cp >= 0x3000 && cp <= 0x303F)
    return 2;

  // Hiragana
  if (cp >= 0x3040 && cp <= 0x309F)
    return 2;

  // Katakana
  if (cp >= 0x30A0 && cp <= 0x30FF)
    return 2;

  // Bopomofo
  if (cp >= 0x3100 && cp <= 0x312F)
    return 2;

  // Hangul Compatibility Jamo
  if (cp >= 0x3130 && cp <= 0x318F)
    return 2;

  // Kanbun
  if (cp >= 0x3190 && cp <= 0x319F)
    return 2;

  // Bopomofo Extended
  if (cp >= 0x31A0 && cp <= 0x31BF)
    return 2;

  // CJK Strokes
  if (cp >= 0x31C0 && cp <= 0x31EF)
    return 2;

  // Katakana Phonetic Extensions
  if (cp >= 0x31F0 && cp <= 0x31FF)
    return 2;

  // Enclosed CJK Letters and Months
  if (cp >= 0x3200 && cp <= 0x32FF)
    return 2;

  // CJK Compatibility
  if (cp >= 0x3300 && cp <= 0x33FF)
    return 2;

  // CJK Unified Ideographs Extension A
  if (cp >= 0x3400 && cp <= 0x4DBF)
    return 2;

  // Yijing Hexagram Symbols
  if (cp >= 0x4DC0 && cp <= 0x4DFF)
    return 2;

  // CJK Unified Ideographs
  if (cp >= 0x4E00 && cp <= 0x9FFF)
    return 2;

  // Yi Syllables
  if (cp >= 0xA000 && cp <= 0xA48F)
    return 2;

  // Yi Radicals
  if (cp >= 0xA490 && cp <= 0xA4CF)
    return 2;

  // Hangul Jamo Extended-A
  if (cp >= 0xA960 && cp <= 0xA97F)
    return 2;

  // Hangul Syllables
  if (cp >= 0xAC00 && cp <= 0xD7AF)
    return 2;

  // Hangul Jamo Extended-B
  if (cp >= 0xD7B0 && cp <= 0xD7FF)
    return 2;

  // CJK Compatibility Ideographs
  if (cp >= 0xF900 && cp <= 0xFAFF)
    return 2;

  // Vertical Forms
  if (cp >= 0xFE10 && cp <= 0xFE1F)
    return 2;

  // CJK Compatibility Forms
  if (cp >= 0xFE30 && cp <= 0xFE4F)
    return 2;

  // Small Form Variants
  if (cp >= 0xFE50 && cp <= 0xFE6F)
    return 2;

  // Halfwidth and Fullwidth Forms (fullwidth only)
  if (cp >= 0xFF00 && cp <= 0xFF60)
    return 2;
  if (cp >= 0xFFE0 && cp <= 0xFFE6)
    return 2;

  // CJK Unified Ideographs Extension B-I and other supplementary CJK
  if (cp >= 0x20000 && cp <= 0x2FFFF)
    return 2;
  if (cp >= 0x30000 && cp <= 0x3FFFF)
    return 2;

  // Emoji (most are wide)
  // Miscellaneous Symbols and Pictographs
  if (cp >= 0x1F300 && cp <= 0x1F5FF)
    return 2;

  // Emoticons
  if (cp >= 0x1F600 && cp <= 0x1F64F)
    return 2;

  // Ornamental Dingbats
  if (cp >= 0x1F650 && cp <= 0x1F67F)
    return 2;

  // Transport and Map Symbols
  if (cp >= 0x1F680 && cp <= 0x1F6FF)
    return 2;

  // Alchemical Symbols
  if (cp >= 0x1F700 && cp <= 0x1F77F)
    return 2;

  // Geometric Shapes Extended
  if (cp >= 0x1F780 && cp <= 0x1F7FF)
    return 2;

  // Supplemental Arrows-C
  if (cp >= 0x1F800 && cp <= 0x1F8FF)
    return 2;

  // Supplemental Symbols and Pictographs
  if (cp >= 0x1F900 && cp <= 0x1F9FF)
    return 2;

  // Chess Symbols
  if (cp >= 0x1FA00 && cp <= 0x1FA6F)
    return 2;

  // Symbols and Pictographs Extended-A
  if (cp >= 0x1FA70 && cp <= 0x1FAFF)
    return 2;

  // Symbols for Legacy Computing
  if (cp >= 0x1FB00 && cp <= 0x1FBFF)
    return 2;

  // Default: single width
  return 1;
}

size_t utf8_display_width(std::string_view str) {
  size_t width = 0;
  size_t pos = 0;

  while (pos < str.size()) {
    uint32_t cp;
    size_t len = utf8_decode(str, pos, cp);
    if (len == 0)
      break;

    width += codepoint_width(cp);
    pos += len;
  }

  return width;
}

std::string utf8_truncate(std::string_view str, size_t max_width) {
  if (max_width == 0) {
    return "";
  }

  size_t width = 0;
  size_t pos = 0;
  size_t last_valid_pos = 0;

  // Calculate total width and find truncation point
  while (pos < str.size()) {
    uint32_t cp;
    size_t len = utf8_decode(str, pos, cp);
    if (len == 0)
      break;

    int cp_width = codepoint_width(cp);

    // Check if adding this character would exceed the limit
    if (width + cp_width > max_width) {
      // We need to truncate
      break;
    }

    last_valid_pos = pos + len;
    width += cp_width;
    pos += len;
  }

  // If the entire string fits, return it as-is
  if (pos >= str.size()) {
    return std::string(str);
  }

  // Need to truncate - determine if we have room for ellipsis
  constexpr size_t ELLIPSIS_WIDTH = 3; // "..." is 3 columns

  if (max_width <= ELLIPSIS_WIDTH) {
    // Not enough room for ellipsis, just truncate
    // Find how much we can fit
    width = 0;
    pos = 0;
    while (pos < str.size()) {
      uint32_t cp;
      size_t len = utf8_decode(str, pos, cp);
      if (len == 0)
        break;

      int cp_width = codepoint_width(cp);
      if (width + cp_width > max_width)
        break;

      width += cp_width;
      pos += len;
    }
    return std::string(str.substr(0, pos));
  }

  // Find truncation point that leaves room for "..."
  size_t target_width = max_width - ELLIPSIS_WIDTH;
  width = 0;
  pos = 0;
  last_valid_pos = 0;

  while (pos < str.size()) {
    uint32_t cp;
    size_t len = utf8_decode(str, pos, cp);
    if (len == 0)
      break;

    int cp_width = codepoint_width(cp);
    if (width + cp_width > target_width)
      break;

    width += cp_width;
    pos += len;
    last_valid_pos = pos;
  }

  return std::string(str.substr(0, last_valid_pos)) + "...";
}

} // namespace libvroom
