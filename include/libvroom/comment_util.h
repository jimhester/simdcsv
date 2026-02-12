#pragma once

#include <cstddef>
#include <cstring>
#include <string>

namespace libvroom {

/// Check if data at the given position starts with the comment string.
/// Returns true if the comment string is non-empty and matches the prefix at data[0..].
/// Uses memcmp for efficient comparison.
inline bool starts_with_comment(const char* data, size_t remaining, const std::string& comment) {
  if (comment.empty() || remaining < comment.size()) {
    return false;
  }
  return std::memcmp(data, comment.data(), comment.size()) == 0;
}

/// Skip to the end of the current line, handling \n, \r\n, and bare \r.
/// Returns the offset past the line ending.
inline size_t skip_to_next_line(const char* data, size_t size, size_t offset) {
  while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
    offset++;
  }
  if (offset < size && data[offset] == '\r') {
    offset++;
    if (offset < size && data[offset] == '\n') {
      offset++; // CRLF
    }
  } else if (offset < size && data[offset] == '\n') {
    offset++;
  }
  return offset;
}

/// Skip leading empty/whitespace-only lines and comment lines before the header.
/// Returns the number of bytes to skip. Handles interleaved blank and comment
/// lines, and comment lines with leading whitespace (e.g., "  # comment").
inline size_t skip_leading_empty_and_comment_lines(const char* data, size_t size,
                                                   const std::string& comment) {
  size_t offset = 0;
  while (offset < size) {
    size_t line_start = offset;

    // Skip leading whitespace on this line
    while (offset < size && data[offset] != '\n' && data[offset] != '\r' &&
           (data[offset] == ' ' || data[offset] == '\t')) {
      offset++;
    }

    // Check what follows the whitespace
    if (offset >= size || data[offset] == '\n' || data[offset] == '\r') {
      // Empty/whitespace-only line - skip past line ending
      if (offset < size && data[offset] == '\r') {
        offset++;
        if (offset < size && data[offset] == '\n') {
          offset++; // CRLF
        }
      } else if (offset < size && data[offset] == '\n') {
        offset++;
      }
      continue;
    }

    // Check if this line is a comment (possibly after whitespace)
    if (!comment.empty() && size - offset >= comment.size() &&
        std::memcmp(data + offset, comment.data(), comment.size()) == 0) {
      // Comment line - skip to end
      offset = skip_to_next_line(data, size, offset);
      continue;
    }

    // This line has real content - stop here
    return line_start;
  }
  return offset; // All lines were empty/comments
}

} // namespace libvroom
