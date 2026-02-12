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

} // namespace libvroom
