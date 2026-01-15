#include "value_extraction.h"

#include "column_index.h"
#include "two_pass.h"

#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace libvroom {

// Helper: Skip over comment lines starting at the given position.
// Returns the position after all consecutive comment lines, or the original
// position if no comment lines are present.
static size_t skip_comment_lines_from(const uint8_t* buf, size_t len, size_t pos,
                                      char comment_char) {
  if (comment_char == '\0' || pos >= len) {
    return pos;
  }

  while (pos < len) {
    // Skip any leading whitespace (spaces and tabs only)
    size_t line_start = pos;
    while (pos < len && (buf[pos] == ' ' || buf[pos] == '\t')) {
      ++pos;
    }

    // Check if this line starts with the comment character
    if (pos < len && buf[pos] == static_cast<uint8_t>(comment_char)) {
      // This is a comment line - skip to end of line
      while (pos < len && buf[pos] != '\n' && buf[pos] != '\r') {
        ++pos;
      }
      // Skip the line ending (LF, CR, or CRLF)
      if (pos < len) {
        if (buf[pos] == '\r') {
          ++pos;
          if (pos < len && buf[pos] == '\n') {
            ++pos;
          }
        } else if (buf[pos] == '\n') {
          ++pos;
        }
      }
      // Continue checking for more comment lines
    } else {
      // Not a comment line - revert to start of this line content
      return line_start;
    }
  }

  return pos;
}

// Helper to count columns by finding first newline using k-way merge
static size_t count_columns_via_merge(const ParseIndex& idx, const uint8_t* buf, size_t len) {
  SortedIndexIterator iter(idx);
  size_t first_nl = 0;
  size_t pos = 0;

  while (iter.has_next()) {
    uint64_t sep = iter.next();
    if (sep < len && (buf[sep] == '\n' || buf[sep] == '\r')) {
      first_nl = pos;
      break;
    }
    ++pos;
  }

  return first_nl + 1;
}

ValueExtractor::ValueExtractor(const uint8_t* buf, size_t len, const ParseIndex& idx,
                               const Dialect& dialect, const ExtractionConfig& config)
    : buf_(buf), len_(len), idx_ptr_(&idx), dialect_(dialect), config_(config) {
  // Determine column count using lazy k-way merge (O(columns) not O(n log n))
  num_columns_ = count_columns_via_merge(idx, buf, len);

  // Calculate row count from total indexes (no sorting needed)
  recalculate_num_rows();
}

ValueExtractor::ValueExtractor(const uint8_t* buf, size_t len, const ParseIndex& idx,
                               const Dialect& dialect, const ExtractionConfig& config,
                               const ColumnConfigMap& column_configs)
    : buf_(buf), len_(len), idx_ptr_(&idx), dialect_(dialect), config_(config),
      column_configs_(column_configs) {
  // Determine column count using lazy k-way merge
  num_columns_ = count_columns_via_merge(idx, buf, len);
  recalculate_num_rows();

  // Resolve any name-based column configs now that we have headers
  resolve_column_configs();
}

ValueExtractor::ValueExtractor(std::shared_ptr<const ParseIndex> shared_idx, const Dialect& dialect,
                               const ExtractionConfig& config)
    : buf_(nullptr), len_(0), idx_ptr_(nullptr), dialect_(dialect), config_(config),
      shared_idx_(std::move(shared_idx)) {
  if (!shared_idx_) {
    throw std::invalid_argument("shared_idx cannot be null");
  }
  if (!shared_idx_->has_buffer()) {
    throw std::invalid_argument("ParseIndex must have buffer set for shared ownership");
  }

  // Get buffer from the shared ParseIndex
  shared_buffer_ = shared_idx_->buffer();
  buf_ = shared_buffer_->data();
  len_ = shared_buffer_->size();

  // Determine column count using lazy k-way merge
  num_columns_ = count_columns_via_merge(*shared_idx_, buf_, len_);
  recalculate_num_rows();
}

void ValueExtractor::ensure_sorted() const {
  if (indexes_sorted_) {
    return;
  }

  const ParseIndex& idx_ref = idx();
  uint64_t total_indexes = idx_ref.total_indexes();
  linear_indexes_.clear();
  linear_indexes_.reserve(total_indexes);

  // Read indexes handling three possible layouts:
  // - region_offsets != nullptr: Right-sized per-thread regions (from init_counted_per_thread)
  // - region_size > 0: Uniform per-thread regions at indexes[t * region_size]
  // - region_size == 0 && region_offsets == nullptr: Contiguous from deserialization
  for (uint16_t t = 0; t < idx_ref.n_threads; ++t) {
    uint64_t* thread_base;
    if (idx_ref.region_offsets != nullptr) {
      thread_base = idx_ref.indexes + idx_ref.region_offsets[t];
    } else if (idx_ref.region_size > 0) {
      thread_base = idx_ref.indexes + t * idx_ref.region_size;
    } else {
      // Contiguous layout: compute offset for this thread
      size_t offset = 0;
      for (uint16_t i = 0; i < t; ++i) {
        offset += idx_ref.n_indexes[i];
      }
      thread_base = idx_ref.indexes + offset;
    }
    for (uint64_t j = 0; j < idx_ref.n_indexes[t]; ++j)
      linear_indexes_.push_back(thread_base[j]);
  }

  std::sort(linear_indexes_.begin(), linear_indexes_.end());
  indexes_sorted_ = true;
}

std::string_view ValueExtractor::get_string_view(size_t row, size_t col) const {
  if (row >= num_rows_)
    throw std::out_of_range("Row index out of range");
  if (col >= num_columns_)
    throw std::out_of_range("Column index out of range");
  return get_string_view_internal(row, col);
}

std::string_view ValueExtractor::get_string_view_internal(size_t row, size_t col) const {
  // Ensure sorted indexes are available for field access
  ensure_sorted();

  size_t field_idx = compute_field_index(row, col);
  // Return empty view with valid pointer to avoid undefined behavior
  if (field_idx >= linear_indexes_.size())
    return std::string_view(reinterpret_cast<const char*>(buf_), 0);

  size_t start = (field_idx == 0) ? 0 : linear_indexes_[field_idx - 1] + 1;
  size_t end = linear_indexes_[field_idx];

  if (end > len_)
    end = len_;
  if (start > len_)
    start = len_;

  // If this is the first column of a row (col == 0) and not the first field overall,
  // check if the previous field ended with a newline. If so, skip any comment lines
  if (col == 0 && field_idx > 0 && dialect_.comment_char != '\0') {
    size_t prev_end_pos = linear_indexes_[field_idx - 1];
    if (prev_end_pos < len_ && (buf_[prev_end_pos] == '\n' || buf_[prev_end_pos] == '\r')) {
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }
  }

  if (end > start && buf_[end - 1] == '\r')
    --end;
  if (end > start && buf_[start] == static_cast<uint8_t>(dialect_.quote_char))
    if (buf_[end - 1] == static_cast<uint8_t>(dialect_.quote_char)) {
      ++start;
      --end;
    }
  if (end < start)
    end = start;
  assert(end >= start && "Invalid range: end must be >= start");
  return std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start);
}

std::string ValueExtractor::get_string(size_t row, size_t col) const {
  // Ensure sorted indexes are available for field access
  ensure_sorted();

  size_t field_idx = compute_field_index(row, col);
  if (field_idx >= linear_indexes_.size())
    return std::string();

  size_t start = (field_idx == 0) ? 0 : linear_indexes_[field_idx - 1] + 1;
  size_t end = linear_indexes_[field_idx];

  if (end > len_)
    end = len_;
  if (start > len_)
    start = len_;

  // If this is the first column of a row (col == 0) and not the first field overall,
  // check if the previous field ended with a newline. If so, skip any comment lines
  if (col == 0 && field_idx > 0 && dialect_.comment_char != '\0') {
    size_t prev_end_pos = linear_indexes_[field_idx - 1];
    if (prev_end_pos < len_ && (buf_[prev_end_pos] == '\n' || buf_[prev_end_pos] == '\r')) {
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }
  }

  if (end > start && buf_[end - 1] == '\r')
    --end;
  if (end < start)
    end = start;
  assert(end >= start && "Invalid range: end must be >= start");
  return unescape_field(std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start));
}

size_t ValueExtractor::compute_field_index(size_t row, size_t col) const {
  return (has_header_ ? row + 1 : row) * num_columns_ + col;
}

std::string ValueExtractor::unescape_field(std::string_view field) const {
  if (field.empty() || field.front() != dialect_.quote_char)
    return std::string(field);
  if (field.size() < 2 || field.back() != dialect_.quote_char)
    return std::string(field);
  std::string_view inner = field.substr(1, field.size() - 2);
  std::string result;
  result.reserve(inner.size());
  for (size_t i = 0; i < inner.size(); ++i) {
    char c = inner[i];
    if (c == dialect_.escape_char && i + 1 < inner.size() && inner[i + 1] == dialect_.quote_char) {
      result += dialect_.quote_char;
      ++i;
    } else
      result += c;
  }
  return result;
}

std::vector<std::string_view> ValueExtractor::extract_column_string_view(size_t col) const {
  if (col >= num_columns_)
    throw std::out_of_range("Column index out of range");
  std::vector<std::string_view> result;
  result.reserve(num_rows_);
  for (size_t row = 0; row < num_rows_; ++row)
    result.push_back(get_string_view_internal(row, col));
  return result;
}

std::vector<std::string> ValueExtractor::extract_column_string(size_t col) const {
  if (col >= num_columns_)
    throw std::out_of_range("Column index out of range");
  std::vector<std::string> result;
  result.reserve(num_rows_);
  for (size_t row = 0; row < num_rows_; ++row)
    result.push_back(get_string(row, col));
  return result;
}

std::vector<std::string> ValueExtractor::get_header() const {
  if (!has_header_)
    throw std::runtime_error("CSV has no header row");

  // Ensure sorted indexes are available
  ensure_sorted();

  std::vector<std::string> headers;
  headers.reserve(num_columns_);

  for (size_t col = 0; col < num_columns_; ++col) {
    if (col >= linear_indexes_.size())
      break;

    size_t start = (col == 0) ? 0 : linear_indexes_[col - 1] + 1;
    size_t end = linear_indexes_[col];

    if (end > len_)
      end = len_;
    if (start > len_)
      start = len_;

    // For the first header column (col == 0), skip any comment lines at the
    // beginning of the file
    if (col == 0 && dialect_.comment_char != '\0') {
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }

    if (end > start && buf_[end - 1] == '\r')
      --end;
    if (end < start)
      end = start;
    assert(end >= start && "Invalid range: end must be >= start");
    headers.push_back(
        unescape_field(std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start)));
  }
  return headers;
}

bool ValueExtractor::get_field_bounds(size_t row, size_t col, size_t& start, size_t& end) const {
  if (row >= num_rows_ || col >= num_columns_)
    return false;

  // Ensure sorted indexes are available
  ensure_sorted();

  size_t field_idx = compute_field_index(row, col);
  if (field_idx >= linear_indexes_.size())
    return false;

  start = (field_idx == 0) ? 0 : linear_indexes_[field_idx - 1] + 1;
  end = linear_indexes_[field_idx];

  // If this is the first column of a row (col == 0) and not the first field overall,
  // check if the previous field ended with a newline. If so, skip any comment lines
  if (col == 0 && field_idx > 0 && dialect_.comment_char != '\0') {
    size_t prev_end_pos = linear_indexes_[field_idx - 1];
    if (prev_end_pos < len_ && (buf_[prev_end_pos] == '\n' || buf_[prev_end_pos] == '\r')) {
      start = skip_comment_lines_from(buf_, len_, start, dialect_.comment_char);
    }
  }

  assert(end >= start && "Invalid field bounds: end must be >= start");
  return true;
}

ValueExtractor::Location ValueExtractor::byte_offset_to_location(size_t byte_offset) const {
  // Handle edge cases
  const ParseIndex& idx_ref = idx();
  uint64_t total = idx_ref.total_indexes();
  if (total == 0 || num_columns_ == 0) {
    return {0, 0, false};
  }

  // Use lazy sorted index for binary search - O(log n) amortized
  // This defers the O(n log n) sort until actually needed
  if (!lazy_sorted_index_) {
    lazy_sorted_index_ = std::make_unique<LazySortedIndex>(idx_ref);
  }

  // Check if byte_offset is beyond the last separator
  // We need to materialize at least the last element to check this
  if (lazy_sorted_index_->size() > 0) {
    // Binary search using lazy materialization
    size_t field_index = lazy_sorted_index_->lower_bound(byte_offset);

    if (field_index >= lazy_sorted_index_->size()) {
      return {0, 0, false};
    }

    // Check if the found position is actually >= byte_offset
    uint64_t found_pos = (*lazy_sorted_index_)[field_index];
    if (found_pos < byte_offset) {
      return {0, 0, false};
    }

    // Convert field index to row/column
    size_t row = field_index / num_columns_;
    size_t col = field_index % num_columns_;

    return {row, col, true};
  }

  return {0, 0, false};
}

} // namespace libvroom
