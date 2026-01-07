/**
 * @file utf8.h
 * @brief UTF-8 string utilities for display width calculation and truncation.
 *
 * This header provides utilities for working with UTF-8 encoded strings,
 * including display width calculation (accounting for wide characters like
 * CJK and emoji) and truncation that respects code point boundaries.
 *
 * Display Width:
 * - ASCII characters: 1 column
 * - CJK characters (Han, Hiragana, Katakana, etc.): 2 columns
 * - Fullwidth characters: 2 columns
 * - Emoji (most): 2 columns
 * - Other characters: 1 column
 *
 * @see utf8_display_width() for calculating string display width
 * @see utf8_truncate() for safe truncation at code point boundaries
 */

#ifndef LIBVROOM_UTF8_H
#define LIBVROOM_UTF8_H

#include <cstddef>
#include <cstdint>
#include <string>
#include <string_view>

namespace libvroom {

/**
 * @brief Get the display width of a Unicode code point.
 *
 * Returns the number of terminal columns a character occupies:
 * - 0 for non-printable control characters and combining marks
 * - 1 for most characters (ASCII, Latin, Cyrillic, etc.)
 * - 2 for wide characters (CJK, fullwidth, emoji)
 *
 * @param codepoint The Unicode code point
 * @return The display width (0, 1, or 2)
 */
int codepoint_width(uint32_t codepoint);

/**
 * @brief Calculate the display width of a UTF-8 string.
 *
 * Sums the display width of all characters in the string, accounting for
 * wide characters (CJK, emoji, fullwidth) that occupy 2 terminal columns.
 *
 * Invalid UTF-8 sequences are counted as 1 column per byte.
 *
 * @param str The UTF-8 encoded string
 * @return The total display width in terminal columns
 */
size_t utf8_display_width(std::string_view str);

/**
 * @brief Truncate a UTF-8 string to fit within a maximum display width.
 *
 * Truncates the string at a code point boundary to ensure the resulting
 * string fits within max_width terminal columns. Never splits multi-byte
 * UTF-8 sequences.
 *
 * If truncation occurs and there's room, appends an ellipsis ("...").
 * The ellipsis itself counts as 3 columns, so the actual content will
 * fit in (max_width - 3) columns.
 *
 * @param str The UTF-8 encoded string to truncate
 * @param max_width Maximum display width in terminal columns
 * @return The truncated string, potentially with "..." appended
 */
std::string utf8_truncate(std::string_view str, size_t max_width);

/**
 * @brief Decode a UTF-8 sequence starting at the given position.
 *
 * Decodes one Unicode code point from the UTF-8 sequence.
 *
 * @param str The UTF-8 string
 * @param pos Starting byte position (updated to point past the decoded sequence)
 * @param[out] codepoint The decoded code point (set to 0xFFFD for invalid sequences)
 * @return The number of bytes consumed (1-4, or 1 for invalid sequences)
 */
size_t utf8_decode(std::string_view str, size_t pos, uint32_t& codepoint);

} // namespace libvroom

#endif // LIBVROOM_UTF8_H
