#include "column_index.h"

#include <algorithm>
#include <stdexcept>

namespace libvroom {

// ============================================================================
// ColumnIndex Implementation
// ============================================================================

ColumnIndex::ColumnIndex(const ParseIndex& idx, const uint8_t* buf, size_t len) {
  if (!idx.is_valid() || idx.columns == 0) {
    return;
  }

  num_columns_ = idx.columns;

  // Use k-way merge to iterate through separators in sorted order
  // and organize them by column
  SortedIndexIterator iter(idx);
  size_t total_seps = iter.total_count();

  if (total_seps == 0) {
    return;
  }

  // Pre-allocate storage
  separators_.reserve(total_seps);
  column_offsets_.resize(num_columns_ + 1, 0);

  // First pass: collect all separators in sorted order
  std::vector<uint64_t> all_seps;
  all_seps.reserve(total_seps);
  while (iter.has_next()) {
    all_seps.push_back(iter.next());
  }

  // Count total rows (each row ends with a newline)
  size_t total_rows = 0;
  for (uint64_t sep : all_seps) {
    if (sep < len && (buf[sep] == '\n' || buf[sep] == '\r')) {
      ++total_rows;
    }
  }

  if (total_rows == 0) {
    return;
  }

  // num_rows_ excludes header row
  num_rows_ = total_rows > 0 ? total_rows - 1 : 0;

  // Pre-allocate row starts
  row_starts_.reserve(total_rows + 1);
  row_starts_.push_back(0); // First row starts at byte 0

  // Organize separators by column
  // Each row has num_columns_ fields, so we iterate through
  // the sorted separators and assign each to its column
  size_t current_col = 0;
  size_t sep_idx = 0;

  for (uint64_t sep : all_seps) {
    separators_.push_back(sep);

    // Track row boundaries
    if (sep < len && (buf[sep] == '\n' || buf[sep] == '\r')) {
      // This separator ends the current row
      // Next row starts after this newline
      size_t next_start = sep + 1;
      // Handle CRLF
      if (sep + 1 < len && buf[sep] == '\r' && buf[sep + 1] == '\n') {
        next_start = sep + 2;
      }
      if (row_starts_.size() < total_rows + 1) {
        row_starts_.push_back(next_start);
      }
    }

    ++sep_idx;
  }

  // Build column offsets
  // Each column has (total_rows) separators
  size_t seps_per_column = total_rows;
  for (size_t c = 0; c <= num_columns_; ++c) {
    column_offsets_[c] = c * seps_per_column;
  }

  // Reorganize separators from row-major to column-major order
  // Current layout: [row0_col0, row0_col1, ..., row1_col0, row1_col1, ...]
  // Target layout:  [col0_row0, col0_row1, ..., col1_row0, col1_row1, ...]
  std::vector<uint64_t> col_major(separators_.size());
  for (size_t row = 0; row < total_rows; ++row) {
    for (size_t col = 0; col < num_columns_; ++col) {
      size_t row_major_idx = row * num_columns_ + col;
      size_t col_major_idx = col * total_rows + row;
      if (row_major_idx < separators_.size() && col_major_idx < col_major.size()) {
        col_major[col_major_idx] = separators_[row_major_idx];
      }
    }
  }
  separators_ = std::move(col_major);
}

FieldSpan ColumnIndex::get_field_span(size_t row, size_t col) const {
  if (col >= num_columns_ || row >= num_rows_) {
    return FieldSpan();
  }

  // Row in separators includes header, so actual data row is row + 1
  size_t sep_row = row + 1; // +1 to skip header row

  // Get the separator position for this field (end of field)
  size_t col_offset = column_offsets_[col];
  size_t total_rows_with_header = num_rows_ + 1;

  if (sep_row >= total_rows_with_header) {
    return FieldSpan();
  }

  uint64_t end = separators_[col_offset + sep_row];

  // Get the start position
  uint64_t start;
  if (col == 0) {
    // First column: start is the row start
    if (sep_row < row_starts_.size()) {
      start = row_starts_[sep_row];
    } else {
      return FieldSpan();
    }
  } else {
    // Other columns: start is previous column's end + 1
    size_t prev_col_offset = column_offsets_[col - 1];
    start = separators_[prev_col_offset + sep_row] + 1;
  }

  return FieldSpan(start, end);
}

FieldSpan ColumnIndex::get_header_span(size_t col) const {
  if (col >= num_columns_) {
    return FieldSpan();
  }

  size_t col_offset = column_offsets_[col];
  uint64_t end = separators_[col_offset]; // Row 0 is header

  uint64_t start;
  if (col == 0) {
    start = 0; // Header starts at byte 0
  } else {
    size_t prev_col_offset = column_offsets_[col - 1];
    start = separators_[prev_col_offset] + 1;
  }

  return FieldSpan(start, end);
}

// ============================================================================
// SortedIndexIterator Implementation
// ============================================================================

SortedIndexIterator::SortedIndexIterator(const ParseIndex& idx) : idx_(&idx) {
  if (!idx.is_valid()) {
    return;
  }

  // Initialize heap with first element from each non-empty thread
  for (uint16_t t = 0; t < idx.n_threads; ++t) {
    if (idx.n_indexes[t] > 0) {
      IndexView view = idx.thread_data(t);
      if (view.size() > 0) {
        heap_.push(MergeElement{view[0], t, 1});
        total_count_ += view.size();
      }
    }
  }
}

uint64_t SortedIndexIterator::next() {
  if (heap_.empty()) {
    throw std::out_of_range("SortedIndexIterator: no more elements");
  }

  MergeElement top = heap_.top();
  heap_.pop();
  ++consumed_count_;

  // Push next element from same thread if available
  IndexView view = idx_->thread_data(top.thread_id);
  if (top.next_idx < view.size()) {
    heap_.push(MergeElement{view[top.next_idx], top.thread_id, top.next_idx + 1});
  }

  return top.value;
}

// ============================================================================
// LazySortedIndex Implementation
// ============================================================================

LazySortedIndex::LazySortedIndex(const ParseIndex& idx)
    : iterator_(std::make_unique<SortedIndexIterator>(idx)) {
  total_size_ = iterator_->total_count();
}

uint64_t LazySortedIndex::operator[](size_t idx) {
  if (idx >= total_size_) {
    throw std::out_of_range("LazySortedIndex: index out of range");
  }

  materialize_to(idx);
  return materialized_[idx];
}

void LazySortedIndex::materialize_to(size_t idx) {
  while (materialized_.size() <= idx && iterator_->has_next()) {
    materialized_.push_back(iterator_->next());
  }
}

size_t LazySortedIndex::lower_bound(uint64_t value) {
  // Binary search with lazy materialization
  // Start by checking if we can answer from already-materialized data
  if (!materialized_.empty()) {
    auto it = std::lower_bound(materialized_.begin(), materialized_.end(), value);
    size_t pos = static_cast<size_t>(it - materialized_.begin());

    // If we found an element >= value in materialized data, we're done
    if (it != materialized_.end()) {
      return pos;
    }

    // Need to check if there are more elements
    if (materialized_.size() >= total_size_) {
      return total_size_;
    }
  }

  // Materialize more elements as needed
  // Use exponential search to find the range, then binary search
  size_t low = materialized_.size();
  size_t high = total_size_;

  // Exponentially expand the materialized region
  while (low < high) {
    size_t mid = low + (high - low) / 2;
    materialize_to(mid);

    if (materialized_[mid] < value) {
      low = mid + 1;
    } else {
      high = mid;
    }
  }

  return low;
}

void LazySortedIndex::materialize_all() {
  while (iterator_->has_next()) {
    materialized_.push_back(iterator_->next());
  }
}

} // namespace libvroom
