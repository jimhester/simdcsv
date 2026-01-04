/**
 * @file type_detector.h
 * @brief Field type detection for CSV data.
 */

#ifndef SIMDCSV_TYPE_DETECTOR_H
#define SIMDCSV_TYPE_DETECTOR_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <vector>
#include <algorithm>
#include <cstring>
#include "common_defs.h"
#include "simd_highway.h"

namespace simdcsv {

enum class FieldType : uint8_t {
  BOOLEAN = 0,
  INTEGER = 1,
  FLOAT = 2,
  DATE = 3,
  STRING = 4,
  EMPTY = 5
};

inline const char* field_type_to_string(FieldType type) {
  switch (type) {
    case FieldType::BOOLEAN: return "boolean";
    case FieldType::INTEGER: return "integer";
    case FieldType::FLOAT:   return "float";
    case FieldType::DATE:    return "date";
    case FieldType::STRING:  return "string";
    case FieldType::EMPTY:   return "empty";
  }
  return "unknown";
}

struct TypeDetectionOptions {
  bool bool_as_int = true;
  bool trim_whitespace = true;
  bool allow_exponential = true;
  bool allow_thousands_sep = false;
  char thousands_sep = ',';
  char decimal_point = '.';
  double confidence_threshold = 0.9;

  static TypeDetectionOptions defaults() { return TypeDetectionOptions(); }
};

struct ColumnTypeStats {
  size_t total_count = 0;
  size_t empty_count = 0;
  size_t boolean_count = 0;
  size_t integer_count = 0;
  size_t float_count = 0;
  size_t date_count = 0;
  size_t string_count = 0;

  FieldType dominant_type(double threshold = 0.9) const {
    size_t non_empty = total_count - empty_count;
    if (non_empty == 0) return FieldType::EMPTY;

    // Check each type independently - highest specific count that meets threshold wins
    // Priority order: BOOLEAN > INTEGER > FLOAT > DATE > STRING

    // Check if booleans dominate
    if (static_cast<double>(boolean_count) / non_empty >= threshold)
      return FieldType::BOOLEAN;

    // Check if integers dominate (integers can include booleans like 0/1)
    // But only if booleans don't already dominate
    if (static_cast<double>(integer_count) / non_empty >= threshold)
      return FieldType::INTEGER;

    // Check if floats dominate (floats include integers which are valid floats)
    // Use cumulative count: floats + integers (not booleans, as "true" is not a float)
    if (static_cast<double>(float_count + integer_count) / non_empty >= threshold)
      return FieldType::FLOAT;

    // Check if dates dominate
    if (static_cast<double>(date_count) / non_empty >= threshold)
      return FieldType::DATE;

    return FieldType::STRING;
  }

  void add(FieldType type) {
    ++total_count;
    switch (type) {
      case FieldType::EMPTY:   ++empty_count; break;
      case FieldType::BOOLEAN: ++boolean_count; break;
      case FieldType::INTEGER: ++integer_count; break;
      case FieldType::FLOAT:   ++float_count; break;
      case FieldType::DATE:    ++date_count; break;
      case FieldType::STRING:  ++string_count; break;
    }
  }
};

class TypeDetector {
public:
  static FieldType detect_field(const uint8_t* data, size_t length,
                                const TypeDetectionOptions& options = TypeDetectionOptions()) {
    if (length == 0) return FieldType::EMPTY;

    size_t start = 0;
    size_t end = length;
    if (options.trim_whitespace) {
      while (start < end && is_whitespace(data[start])) ++start;
      while (end > start && is_whitespace(data[end - 1])) --end;
    }

    if (start >= end) return FieldType::EMPTY;

    const uint8_t* field = data + start;
    size_t len = end - start;

    // Check date first for compact format (8 digits like YYYYMMDD)
    // to avoid misdetecting as integer
    if (is_date(field, len)) return FieldType::DATE;
    if (is_boolean(field, len, options)) return FieldType::BOOLEAN;
    if (is_integer(field, len, options)) return FieldType::INTEGER;
    if (is_float(field, len, options)) return FieldType::FLOAT;

    return FieldType::STRING;
  }

  static FieldType detect_field(const std::string& value,
                                const TypeDetectionOptions& options = TypeDetectionOptions()) {
    return detect_field(reinterpret_cast<const uint8_t*>(value.data()),
                        value.size(), options);
  }

  static FieldType detect_field(const char* value,
                                const TypeDetectionOptions& options = TypeDetectionOptions()) {
    return detect_field(reinterpret_cast<const uint8_t*>(value),
                        std::strlen(value), options);
  }

  static bool is_boolean(const uint8_t* data, size_t length,
                         const TypeDetectionOptions& options = TypeDetectionOptions()) {
    if (length == 0) return false;

    if (options.bool_as_int && length == 1) {
      if (data[0] == '0' || data[0] == '1') return true;
    }

    return is_bool_string(data, length);
  }

  static bool is_integer(const uint8_t* data, size_t length,
                         const TypeDetectionOptions& options = TypeDetectionOptions()) {
    if (length == 0) return false;

    size_t i = 0;

    if (data[i] == '+' || data[i] == '-') {
      ++i;
      if (i >= length) return false;
    }

    if (!is_digit(data[i])) return false;

    if (!options.allow_thousands_sep) {
      // Simple case: just digits
      while (i < length) {
        if (!is_digit(data[i])) return false;
        ++i;
      }
      return true;
    }

    // With thousands separator: validate proper grouping
    // First group can be 1-3 digits, subsequent groups must be exactly 3 digits
    size_t first_group_digits = 0;

    // Count first group (1-3 digits before first separator or end)
    while (i < length && is_digit(data[i])) {
      ++first_group_digits;
      ++i;
    }

    if (first_group_digits == 0) return false;

    // If no separator found, it's valid
    if (i >= length) return true;

    // First group must be 1-3 digits if followed by separator
    if (first_group_digits > 3) return false;

    // Process remaining groups (must be exactly 3 digits each)
    while (i < length) {
      if (data[i] != static_cast<uint8_t>(options.thousands_sep)) {
        return false;  // Invalid character
      }
      ++i;  // Skip separator

      // Must have exactly 3 digits after separator
      if (i + 3 > length) return false;
      if (!is_digit(data[i]) || !is_digit(data[i + 1]) || !is_digit(data[i + 2])) {
        return false;
      }
      i += 3;
    }

    return true;
  }

  static bool is_float(const uint8_t* data, size_t length,
                       const TypeDetectionOptions& options = TypeDetectionOptions()) {
    if (length == 0) return false;

    size_t i = 0;
    bool has_digit = false;
    bool has_decimal = false;
    bool has_exponent = false;

    if (data[i] == '+' || data[i] == '-') {
      ++i;
      if (i >= length) return false;
    }

    if (is_special_float(data + i, length - i)) return true;

    while (i < length && is_digit(data[i])) {
      has_digit = true;
      ++i;
    }

    if (i < length && data[i] == static_cast<uint8_t>(options.decimal_point)) {
      has_decimal = true;
      ++i;
      while (i < length && is_digit(data[i])) {
        has_digit = true;
        ++i;
      }
    }

    if (options.allow_exponential && i < length && (data[i] == 'e' || data[i] == 'E')) {
      has_exponent = true;
      ++i;
      if (i < length && (data[i] == '+' || data[i] == '-')) ++i;
      if (i >= length || !is_digit(data[i])) return false;
      while (i < length && is_digit(data[i])) ++i;
    }

    return has_digit && (has_decimal || has_exponent) && i == length;
  }

  /**
   * Detect if a field contains a date value.
   *
   * Supports the following formats:
   * - ISO: YYYY-MM-DD or YYYY/MM/DD
   * - US: MM/DD/YYYY or MM-DD-YYYY
   * - EU: DD/MM/YYYY or DD-MM-YYYY
   * - Compact: YYYYMMDD
   *
   * Note: For dates like "01/02/2024", there is ambiguity between US format
   * (January 2nd) and EU format (February 1st). The current implementation
   * checks US format first, so ambiguous dates will be interpreted as US format.
   * If your data uses EU format exclusively, you may need to handle this at
   * a higher level (e.g., by specifying locale preferences).
   */
  static bool is_date(const uint8_t* data, size_t length) {
    if (length < 8) return false;

    if (is_date_iso(data, length)) return true;
    if (is_date_us(data, length)) return true;
    if (is_date_eu(data, length)) return true;
    if (is_date_compact(data, length)) return true;

    return false;
  }

private:
  really_inline static bool is_whitespace(uint8_t c) {
    return c == ' ' || c == '\t' || c == '\r' || c == '\n';
  }

  really_inline static bool is_digit(uint8_t c) {
    return c >= '0' && c <= '9';
  }

  really_inline static uint8_t to_lower(uint8_t c) {
    return (c >= 'A' && c <= 'Z') ? (c + 32) : c;
  }

  static bool is_leap_year(int year) {
    return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
  }

  static int days_in_month(int year, int month) {
    static const int days[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};
    if (month < 1 || month > 12) return 0;
    if (month == 2 && is_leap_year(year)) return 29;
    return days[month];
  }

  static bool is_valid_date(int year, int month, int day) {
    if (year < 1000 || year > 9999) return false;
    if (month < 1 || month > 12) return false;
    if (day < 1 || day > days_in_month(year, month)) return false;
    return true;
  }

  static bool is_bool_string(const uint8_t* data, size_t length) {
    switch (length) {
      case 1: {
        uint8_t c = to_lower(data[0]);
        return c == 't' || c == 'f' || c == 'y' || c == 'n';
      }
      case 2: {
        uint8_t c0 = to_lower(data[0]);
        uint8_t c1 = to_lower(data[1]);
        return (c0 == 'n' && c1 == 'o') || (c0 == 'o' && c1 == 'n');
      }
      case 3: {
        uint8_t c0 = to_lower(data[0]);
        uint8_t c1 = to_lower(data[1]);
        uint8_t c2 = to_lower(data[2]);
        return (c0 == 'y' && c1 == 'e' && c2 == 's') ||
               (c0 == 'o' && c1 == 'f' && c2 == 'f');
      }
      case 4: {
        uint8_t c0 = to_lower(data[0]);
        uint8_t c1 = to_lower(data[1]);
        uint8_t c2 = to_lower(data[2]);
        uint8_t c3 = to_lower(data[3]);
        return c0 == 't' && c1 == 'r' && c2 == 'u' && c3 == 'e';
      }
      case 5: {
        uint8_t c0 = to_lower(data[0]);
        uint8_t c1 = to_lower(data[1]);
        uint8_t c2 = to_lower(data[2]);
        uint8_t c3 = to_lower(data[3]);
        uint8_t c4 = to_lower(data[4]);
        return c0 == 'f' && c1 == 'a' && c2 == 'l' && c3 == 's' && c4 == 'e';
      }
      default:
        return false;
    }
  }

  static bool is_special_float(const uint8_t* data, size_t length) {
    if (length == 3) {
      uint8_t c0 = to_lower(data[0]);
      uint8_t c1 = to_lower(data[1]);
      uint8_t c2 = to_lower(data[2]);
      return (c0 == 'i' && c1 == 'n' && c2 == 'f') ||
             (c0 == 'n' && c1 == 'a' && c2 == 'n');
    }
    if (length == 8) {
      uint8_t buf[8];
      for (size_t i = 0; i < 8; ++i) buf[i] = to_lower(data[i]);
      return buf[0] == 'i' && buf[1] == 'n' && buf[2] == 'f' &&
             buf[3] == 'i' && buf[4] == 'n' && buf[5] == 'i' &&
             buf[6] == 't' && buf[7] == 'y';
    }
    return false;
  }

  static bool is_date_iso(const uint8_t* data, size_t length) {
    if (length != 10) return false;

    char sep = static_cast<char>(data[4]);
    if (sep != '-' && sep != '/') return false;
    if (data[7] != static_cast<uint8_t>(sep)) return false;

    for (int i = 0; i < 4; ++i) if (!is_digit(data[i])) return false;
    for (int i = 5; i < 7; ++i) if (!is_digit(data[i])) return false;
    for (int i = 8; i < 10; ++i) if (!is_digit(data[i])) return false;

    int year = (data[0] - '0') * 1000 + (data[1] - '0') * 100 +
               (data[2] - '0') * 10 + (data[3] - '0');
    int month = (data[5] - '0') * 10 + (data[6] - '0');
    int day = (data[8] - '0') * 10 + (data[9] - '0');

    return is_valid_date(year, month, day);
  }

  static bool is_date_us(const uint8_t* data, size_t length) {
    if (length != 10) return false;

    char sep = static_cast<char>(data[2]);
    if (sep != '-' && sep != '/') return false;
    if (data[5] != static_cast<uint8_t>(sep)) return false;

    for (int i = 0; i < 2; ++i) if (!is_digit(data[i])) return false;
    for (int i = 3; i < 5; ++i) if (!is_digit(data[i])) return false;
    for (int i = 6; i < 10; ++i) if (!is_digit(data[i])) return false;

    int month = (data[0] - '0') * 10 + (data[1] - '0');
    int day = (data[3] - '0') * 10 + (data[4] - '0');
    int year = (data[6] - '0') * 1000 + (data[7] - '0') * 100 +
               (data[8] - '0') * 10 + (data[9] - '0');

    return is_valid_date(year, month, day);
  }

  static bool is_date_eu(const uint8_t* data, size_t length) {
    if (length != 10) return false;

    char sep = static_cast<char>(data[2]);
    if (sep != '-' && sep != '/') return false;
    if (data[5] != static_cast<uint8_t>(sep)) return false;

    for (int i = 0; i < 2; ++i) if (!is_digit(data[i])) return false;
    for (int i = 3; i < 5; ++i) if (!is_digit(data[i])) return false;
    for (int i = 6; i < 10; ++i) if (!is_digit(data[i])) return false;

    int day = (data[0] - '0') * 10 + (data[1] - '0');
    int month = (data[3] - '0') * 10 + (data[4] - '0');
    int year = (data[6] - '0') * 1000 + (data[7] - '0') * 100 +
               (data[8] - '0') * 10 + (data[9] - '0');

    return is_valid_date(year, month, day);
  }

  static bool is_date_compact(const uint8_t* data, size_t length) {
    if (length != 8) return false;

    for (int i = 0; i < 8; ++i) if (!is_digit(data[i])) return false;

    int year = (data[0] - '0') * 1000 + (data[1] - '0') * 100 +
               (data[2] - '0') * 10 + (data[3] - '0');
    int month = (data[4] - '0') * 10 + (data[5] - '0');
    int day = (data[6] - '0') * 10 + (data[7] - '0');

    return is_valid_date(year, month, day);
  }
};

/**
 * BatchTypeDetector provides batch processing for type detection.
 *
 * Note: Despite creating simd_input structures, this class currently uses
 * scalar loops for actual digit classification. The SIMD infrastructure is
 * in place for future optimization using Highway intrinsics. For now, this
 * class primarily serves as a batch API wrapper around TypeDetector.
 *
 * TODO: Implement actual SIMD digit classification using Highway's
 * comparison operations (similar to cmp_mask_against_input pattern).
 */
class SIMDTypeDetector {
public:
  static uint64_t classify_digits(const uint8_t* data, size_t length) {
    if (length == 0) return 0;

    simd_input in = fill_input(data);
    uint64_t mask = ~0ULL;
    if (length < 64) {
      mask = blsmsk_u64(1ULL << length);
    }

    uint64_t le_9 = 0;
    for (size_t i = 0; i < std::min(length, size_t(64)); ++i) {
      if (data[i] >= '0' && data[i] <= '9') {
        le_9 |= (1ULL << i);
      }
    }

    return le_9 & mask;
  }

  static bool all_digits(const uint8_t* data, size_t length) {
    if (length == 0) return false;

    if (length <= 8) {
      for (size_t i = 0; i < length; ++i) {
        if (data[i] < '0' || data[i] > '9') return false;
      }
      return true;
    }

    size_t i = 0;
    for (; i + 64 <= length; i += 64) {
      simd_input in = fill_input(data + i);
      (void)in;
      for (size_t j = 0; j < 64; ++j) {
        if (data[i + j] < '0' || data[i + j] > '9') return false;
      }
    }

    for (; i < length; ++i) {
      if (data[i] < '0' || data[i] > '9') return false;
    }

    return true;
  }

  static void detect_batch(const uint8_t** fields, const size_t* lengths,
                           size_t count, FieldType* results,
                           const TypeDetectionOptions& options = TypeDetectionOptions()) {
    for (size_t i = 0; i < count; ++i) {
      results[i] = TypeDetector::detect_field(fields[i], lengths[i], options);
    }
  }
};

class ColumnTypeInference {
public:
  explicit ColumnTypeInference(size_t num_columns = 0,
                               const TypeDetectionOptions& options = TypeDetectionOptions())
      : options_(options) {
    if (num_columns > 0) {
      stats_.resize(num_columns);
    }
  }

  void set_options(const TypeDetectionOptions& options) {
    options_ = options;
  }

  void add_row(const std::vector<std::string>& fields) {
    if (fields.size() > stats_.size()) {
      stats_.resize(fields.size());
    }

    for (size_t i = 0; i < fields.size(); ++i) {
      FieldType type = TypeDetector::detect_field(fields[i], options_);
      stats_[i].add(type);
    }
  }

  void add_field(size_t column, const uint8_t* data, size_t length) {
    if (column >= stats_.size()) {
      stats_.resize(column + 1);
    }
    FieldType type = TypeDetector::detect_field(data, length, options_);
    stats_[column].add(type);
  }

  std::vector<FieldType> infer_types() const {
    std::vector<FieldType> types(stats_.size());
    for (size_t i = 0; i < stats_.size(); ++i) {
      types[i] = stats_[i].dominant_type(options_.confidence_threshold);
    }
    return types;
  }

  const ColumnTypeStats& column_stats(size_t column) const {
    return stats_.at(column);
  }

  const std::vector<ColumnTypeStats>& all_stats() const {
    return stats_;
  }

  size_t num_columns() const {
    return stats_.size();
  }

  size_t num_rows() const {
    if (stats_.empty()) return 0;
    return stats_[0].total_count;
  }

  void reset() {
    for (auto& s : stats_) {
      s = ColumnTypeStats();
    }
  }

  void merge(const ColumnTypeInference& other) {
    if (other.stats_.size() > stats_.size()) {
      stats_.resize(other.stats_.size());
    }
    for (size_t i = 0; i < other.stats_.size(); ++i) {
      stats_[i].total_count += other.stats_[i].total_count;
      stats_[i].empty_count += other.stats_[i].empty_count;
      stats_[i].boolean_count += other.stats_[i].boolean_count;
      stats_[i].integer_count += other.stats_[i].integer_count;
      stats_[i].float_count += other.stats_[i].float_count;
      stats_[i].date_count += other.stats_[i].date_count;
      stats_[i].string_count += other.stats_[i].string_count;
    }
  }

private:
  std::vector<ColumnTypeStats> stats_;
  TypeDetectionOptions options_;
};

/**
 * TypeHints allows users to override auto-detected types for specific columns.
 *
 * Note: Uses linear search O(n) for column lookups. For CSVs with typical
 * column counts (<100), this is sufficient. For very wide CSVs, consider
 * using std::unordered_map instead.
 */
struct TypeHints {
  std::vector<std::pair<std::string, FieldType>> column_types;

  void add(const std::string& column, FieldType type) {
    column_types.emplace_back(column, type);
  }

  FieldType get(const std::string& column) const {
    for (const auto& pair : column_types) {
      if (pair.first == column) return pair.second;
    }
    return FieldType::STRING;
  }

  bool has_hint(const std::string& column) const {
    for (const auto& pair : column_types) {
      if (pair.first == column) return true;
    }
    return false;
  }
};

}  // namespace simdcsv

#endif  // SIMDCSV_TYPE_DETECTOR_H
