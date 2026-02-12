#include "libvroom/vroom.h"

#include <cstring>

namespace libvroom {

LineParser::LineParser(const CsvOptions& options) : options_(options) {
  init_null_values();
}

void LineParser::init_null_values() {
  std::string_view null_values = options_.null_values;
  size_t start = 0;

  while (start <= null_values.size()) {
    size_t end = null_values.find(',', start);
    if (end == std::string_view::npos) {
      end = null_values.size();
    }

    std::string_view null_val = null_values.substr(start, end - start);
    if (null_val.empty()) {
      empty_is_null_ = true;
    } else {
      null_value_set_.emplace(null_val);
      if (null_val.size() > max_null_length_) {
        max_null_length_ = null_val.size();
      }
    }

    start = end + 1;
  }
}

std::vector<std::string> LineParser::parse_header(const char* data, size_t size) {
  std::vector<std::string> headers;

  if (size == 0) {
    return headers;
  }

  // Helper to match separator at a given position
  auto matches_sep_at = [&](size_t pos) -> bool {
    if (options_.separator.empty())
      return false;
    if (options_.separator.size() == 1)
      return data[pos] == options_.separator[0];
    return pos + options_.separator.size() <= size &&
           std::memcmp(data + pos, options_.separator.data(), options_.separator.size()) == 0;
  };

  bool in_quote = false;
  std::string current_field;
  current_field.reserve(64);

  for (size_t i = 0; i < size; ++i) {
    char c = data[i];

    // Check for end of line
    if (!in_quote && (c == '\n' || c == '\r')) {
      // Trim trailing whitespace from field
      while (!current_field.empty() &&
             (current_field.back() == ' ' || current_field.back() == '\t')) {
        current_field.pop_back();
      }
      headers.push_back(std::move(current_field));
      current_field.clear();
      break;
    }

    if (options_.escape_backslash && c == '\\' && i + 1 < size) {
      // Backslash escape: add the escaped character
      char next = data[i + 1];
      switch (next) {
      case '\\':
        current_field += '\\';
        break;
      case 'n':
        current_field += '\n';
        break;
      case 't':
        current_field += '\t';
        break;
      case 'r':
        current_field += '\r';
        break;
      default:
        if (next == options_.quote) {
          current_field += options_.quote;
        } else {
          current_field += next;
        }
        break;
      }
      ++i; // Skip escaped character
    } else if (c == options_.quote) {
      if (!options_.escape_backslash && in_quote && i + 1 < size && data[i + 1] == options_.quote) {
        // Escaped quote (doubled) - only in non-backslash mode
        current_field += options_.quote;
        ++i; // Skip next quote
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote && matches_sep_at(i)) {
      // Trim trailing whitespace from field
      while (!current_field.empty() &&
             (current_field.back() == ' ' || current_field.back() == '\t')) {
        current_field.pop_back();
      }
      headers.push_back(std::move(current_field));
      current_field.clear();
      // Advance past multi-byte separator (loop will do +1)
      i += options_.separator.size() - 1;
    } else {
      // Skip leading whitespace if field is empty and not in quote
      if (current_field.empty() && !in_quote && (c == ' ' || c == '\t')) {
        continue;
      }
      current_field += c;
    }
  }

  // Handle last field if line didn't end with newline
  if (!current_field.empty() || headers.empty()) {
    while (!current_field.empty() &&
           (current_field.back() == ' ' || current_field.back() == '\t')) {
      current_field.pop_back();
    }
    headers.push_back(std::move(current_field));
  }

  return headers;
}

size_t LineParser::parse_line(const char* data, size_t size,
                              std::vector<std::unique_ptr<ColumnBuilder>>& columns) {
  if (size == 0 || columns.empty()) {
    return 0;
  }

  // Helper to match separator at a given position
  auto matches_sep_at = [&](size_t pos) -> bool {
    if (options_.separator.empty())
      return false;
    if (options_.separator.size() == 1)
      return data[pos] == options_.separator[0];
    return pos + options_.separator.size() <= size &&
           std::memcmp(data + pos, options_.separator.data(), options_.separator.size()) == 0;
  };

  bool in_quote = false;
  std::string current_field;
  current_field.reserve(64);
  size_t field_index = 0;

  for (size_t i = 0; i < size && field_index < columns.size(); ++i) {
    char c = data[i];

    // Check for end of line (outside quotes)
    if (!in_quote && (c == '\n' || c == '\r')) {
      // Trim field and append to current column
      while (!current_field.empty() &&
             (current_field.back() == ' ' || current_field.back() == '\t')) {
        current_field.pop_back();
      }

      // Check if this is a null value
      if (is_null_value(current_field)) {
        columns[field_index]->append_null();
      } else {
        columns[field_index]->append(current_field);
      }
      current_field.clear(); // Clear after appending
      ++field_index;
      break;
    }

    if (options_.escape_backslash && c == '\\' && i + 1 < size) {
      // Backslash escape: add the escaped character
      char next = data[i + 1];
      switch (next) {
      case '\\':
        current_field += '\\';
        break;
      case 'n':
        current_field += '\n';
        break;
      case 't':
        current_field += '\t';
        break;
      case 'r':
        current_field += '\r';
        break;
      default:
        if (next == options_.quote) {
          current_field += options_.quote;
        } else {
          current_field += next;
        }
        break;
      }
      ++i; // Skip escaped character
    } else if (c == options_.quote) {
      if (!options_.escape_backslash && in_quote && i + 1 < size && data[i + 1] == options_.quote) {
        // Escaped quote (doubled) - only in non-backslash mode
        current_field += options_.quote;
        ++i;
      } else {
        in_quote = !in_quote;
      }
    } else if (!in_quote && matches_sep_at(i)) {
      // End of field - trim and append
      while (!current_field.empty() &&
             (current_field.back() == ' ' || current_field.back() == '\t')) {
        current_field.pop_back();
      }

      if (is_null_value(current_field)) {
        columns[field_index]->append_null();
      } else {
        columns[field_index]->append(current_field);
      }

      current_field.clear();
      ++field_index;
      // Advance past multi-byte separator (loop will do +1)
      i += options_.separator.size() - 1;
    } else {
      // Skip leading whitespace if field is empty and not in quote
      if (current_field.empty() && !in_quote && (c == ' ' || c == '\t')) {
        continue;
      }
      current_field += c;
    }
  }

  // Handle the last field if we didn't hit a newline
  if (field_index < columns.size() && !current_field.empty()) {
    while (!current_field.empty() &&
           (current_field.back() == ' ' || current_field.back() == '\t')) {
      current_field.pop_back();
    }

    if (is_null_value(current_field)) {
      columns[field_index]->append_null();
    } else {
      columns[field_index]->append(current_field);
    }
    ++field_index;
  }

  // Fill remaining columns with nulls if the line has fewer fields
  while (field_index < columns.size()) {
    columns[field_index]->append_null();
    ++field_index;
  }

  return field_index;
}

bool LineParser::is_null_value(std::string_view value) const {
  // Fast path: empty string check
  if (value.empty()) {
    return empty_is_null_;
  }

  // Fast path: length check (most null values are short: NA, null, NULL, etc.)
  if (value.size() > max_null_length_) {
    return false;
  }

  // Use heterogeneous lookup if available (C++20), otherwise linear search
#if defined(__cpp_lib_generic_unordered_lookup) && __cpp_lib_generic_unordered_lookup >= 201811L
  return null_value_set_.find(value) != null_value_set_.end();
#else
  // Linear search fallback for older compilers (fast for small sets, avoids allocation)
  for (const auto& null_val : null_value_set_) {
    if (null_val == value) {
      return true;
    }
  }
  return false;
#endif
}

} // namespace libvroom
