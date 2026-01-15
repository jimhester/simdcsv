#ifndef LIBVROOM_COLUMN_INDEX_H
#define LIBVROOM_COLUMN_INDEX_H

/**
 * @file column_index.h
 * @brief Column-oriented index storage for efficient lazy column access.
 *
 * This header provides a column-oriented reorganization of field separator
 * positions from the row-oriented ParseIndex. This design aligns with
 * columnar data models (Arrow, R) and enables O(1) column access without
 * the O(n log n) sorting overhead required by ValueExtractor.
 *
 * The key insight is that while ParseIndex stores separators in file order
 * (row-major), most data analysis accesses data column-by-column. By
 * reorganizing to column-major layout on-demand, we:
 * - Avoid sorting entirely for column access patterns
 * - Enable lazy per-column materialization
 * - Reduce memory pressure when only accessing a few columns
 *
 * @see ParseIndex for the source row-oriented index
 * @see LazyColumn for lazy per-column access without sorting
 * @see ValueExtractor for eager row-oriented access (requires sorting)
 */

#include "two_pass.h"

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <memory>
#include <queue>
#include <vector>

namespace libvroom {

/**
 * @brief Column-oriented index providing O(1) per-column access.
 *
 * ColumnIndex reorganizes the field separator positions from ParseIndex
 * into a column-major layout. This enables efficient column access without
 * sorting by storing, for each column, the list of separator positions
 * for that column across all rows.
 *
 * ## Memory Layout
 *
 * For a CSV with C columns and R rows:
 * ```
 * column_offsets_[0..C]: Start offset in separators_ for each column
 * separators_[0..R*C]: Separator positions organized by column
 *
 * Column c's separators are at:
 *   separators_[column_offsets_[c] .. column_offsets_[c+1])
 * ```
 *
 * ## Construction Cost
 *
 * - Full materialization: O(n) where n = total fields
 * - Per-column lazy: O(n/C) per column accessed
 *
 * ## Comparison with ValueExtractor
 *
 * | Operation          | ValueExtractor      | ColumnIndex          |
 * |--------------------|---------------------|----------------------|
 * | Construction       | O(n log n) sort     | O(1) or O(n) once    |
 * | Single column      | Pay full sort       | O(rows) per column   |
 * | Row access         | O(1) after sort     | O(columns) per row   |
 * | Memory overhead    | 2x (linear_indexes) | ~1.02x (col offsets) |
 *
 * @example
 * @code
 * // Parse CSV
 * auto result = parser.parse(buf, len);
 *
 * // Create column-oriented index
 * ColumnIndex col_idx(result.idx);
 *
 * // Access column 2 (O(1) lookup)
 * auto col2_seps = col_idx.column_separators(2);
 * for (size_t row = 0; row < col_idx.num_rows(); ++row) {
 *     FieldSpan span = col_idx.get_field_span(row, 2);
 *     // Process field...
 * }
 * @endcode
 */
class ColumnIndex {
public:
  /**
   * @brief Default constructor creates an empty index.
   */
  ColumnIndex() = default;

  /**
   * @brief Construct column-oriented index from a ParseIndex.
   *
   * This constructor reorganizes the ParseIndex data into column-major
   * order. The construction is O(n) where n is the total number of fields,
   * but this cost is paid once and enables O(1) column access thereafter.
   *
   * @param idx Source ParseIndex with row-oriented field positions
   * @param buf Pointer to the CSV buffer (needed to find row boundaries)
   * @param len Length of the buffer
   *
   * @note The ParseIndex must be valid (is_valid() returns true).
   * @note This constructor does NOT sort - it reorganizes in O(n).
   */
  ColumnIndex(const ParseIndex& idx, const uint8_t* buf, size_t len);

  /**
   * @brief Check if the index has been populated.
   */
  bool is_valid() const { return !separators_.empty(); }

  /**
   * @brief Get the number of columns.
   */
  size_t num_columns() const { return num_columns_; }

  /**
   * @brief Get the number of rows (excluding header).
   */
  size_t num_rows() const { return num_rows_; }

  /**
   * @brief Get the separator positions for a specific column.
   *
   * Returns a span of separator positions for the given column.
   * The span contains num_rows() + 1 entries (including header row).
   *
   * @param col Column index (0-based)
   * @return Pointer to the separator positions, or nullptr if invalid
   */
  const uint64_t* column_separators(size_t col) const {
    if (col >= num_columns_)
      return nullptr;
    return separators_.data() + column_offsets_[col];
  }

  /**
   * @brief Get the number of separators for a column.
   *
   * @param col Column index (0-based)
   * @return Number of separator positions for this column
   */
  size_t column_separator_count(size_t col) const {
    if (col >= num_columns_)
      return 0;
    return column_offsets_[col + 1] - column_offsets_[col];
  }

  /**
   * @brief Get field span by row and column in O(1).
   *
   * This is the primary access method, providing O(1) lookup once
   * the column index has been built.
   *
   * @param row Row index (0-based, data rows only, excludes header)
   * @param col Column index (0-based)
   * @return FieldSpan with byte boundaries, or invalid if out of bounds
   */
  FieldSpan get_field_span(size_t row, size_t col) const;

  /**
   * @brief Get field span for header row.
   *
   * @param col Column index (0-based)
   * @return FieldSpan for the header field
   */
  FieldSpan get_header_span(size_t col) const;

private:
  /// Number of columns in the CSV
  size_t num_columns_ = 0;

  /// Number of data rows (excluding header)
  size_t num_rows_ = 0;

  /// Offset into separators_ for each column (size = num_columns_ + 1)
  std::vector<size_t> column_offsets_;

  /// Separator positions organized by column
  /// Layout: [col0_row0, col0_row1, ..., col1_row0, col1_row1, ...]
  std::vector<uint64_t> separators_;

  /// Row start positions (byte offset of each row's first field)
  /// Size = num_rows_ + 1 (includes header row)
  std::vector<uint64_t> row_starts_;
};

// ============================================================================
// Lazy k-way Merge Iterator
// ============================================================================

/**
 * @brief Element in the priority queue for k-way merge.
 *
 * Used by SortedIndexIterator to merge per-thread index regions
 * in sorted order without materializing the full sorted array.
 */
struct MergeElement {
  uint64_t value;     ///< Current separator position
  uint16_t thread_id; ///< Source thread ID
  size_t next_idx;    ///< Next index within this thread's region

  /// Comparison for min-heap (smallest value first)
  bool operator>(const MergeElement& other) const { return value > other.value; }
};

/**
 * @brief Lazy k-way merge iterator over ParseIndex.
 *
 * This iterator provides sorted access to field separator positions
 * without materializing the full O(n) sorted array. Instead, it uses
 * a priority queue of size O(n_threads) to perform k-way merge on demand.
 *
 * ## Complexity
 *
 * - Construction: O(n_threads) to initialize heap
 * - Next element: O(log n_threads) heap operation
 * - Full traversal: O(n log n_threads) vs O(n log n) for full sort
 *
 * For typical thread counts (4-64), this provides ~4-6x speedup over
 * full sorting when only accessing partial data.
 *
 * ## Use Case
 *
 * This iterator is useful for:
 * - byte_offset_to_location() which needs sorted order for binary search
 * - Streaming scenarios where not all data is accessed
 * - Memory-constrained environments
 *
 * @note Most column access patterns should use ColumnIndex instead,
 *       which avoids sorting entirely. This iterator is for cases
 *       that truly need global sorted order.
 *
 * @example
 * @code
 * SortedIndexIterator it(idx);
 * while (it.has_next()) {
 *     uint64_t sep_pos = it.next();
 *     // Process separator at position sep_pos
 * }
 * @endcode
 */
class SortedIndexIterator {
public:
  /**
   * @brief Construct iterator over a ParseIndex.
   *
   * @param idx ParseIndex to iterate over
   */
  explicit SortedIndexIterator(const ParseIndex& idx);

  /**
   * @brief Check if there are more elements.
   */
  bool has_next() const { return !heap_.empty(); }

  /**
   * @brief Get the next separator position in sorted order.
   *
   * @return Next separator position
   * @throws std::out_of_range if no more elements
   */
  uint64_t next();

  /**
   * @brief Peek at the next element without advancing.
   *
   * @return Next separator position, or UINT64_MAX if empty
   */
  uint64_t peek() const { return heap_.empty() ? UINT64_MAX : heap_.top().value; }

  /**
   * @brief Get the total number of elements.
   */
  size_t total_count() const { return total_count_; }

  /**
   * @brief Get the number of elements consumed so far.
   */
  size_t consumed_count() const { return consumed_count_; }

private:
  const ParseIndex* idx_;
  std::priority_queue<MergeElement, std::vector<MergeElement>, std::greater<MergeElement>> heap_;
  size_t total_count_ = 0;
  size_t consumed_count_ = 0;
};

/**
 * @brief Materialize sorted indexes on demand using lazy k-way merge.
 *
 * This class provides the same interface as a sorted vector but
 * constructs the sorted array lazily. It's useful when you need
 * sorted access but may not need all elements.
 *
 * ## Strategy
 *
 * - On construction: Only initialize the merge iterator (O(n_threads))
 * - On access: Materialize up to the requested index
 * - Full sort: O(n log n_threads) when fully materialized
 *
 * @example
 * @code
 * LazySortedIndex sorted(idx);
 *
 * // Only materializes first 100 elements
 * for (size_t i = 0; i < 100; ++i) {
 *     uint64_t sep = sorted[i];
 * }
 *
 * // Binary search materializes only what's needed
 * size_t pos = sorted.lower_bound(byte_offset);
 * @endcode
 */
class LazySortedIndex {
public:
  /**
   * @brief Construct lazy sorted index from ParseIndex.
   */
  explicit LazySortedIndex(const ParseIndex& idx);

  /**
   * @brief Access element at index, materializing as needed.
   *
   * @param idx Index into the sorted array
   * @return Separator position at that index
   * @throws std::out_of_range if idx >= size()
   */
  uint64_t operator[](size_t idx);

  /**
   * @brief Get total size (without materializing).
   */
  size_t size() const { return total_size_; }

  /**
   * @brief Check if empty.
   */
  bool empty() const { return total_size_ == 0; }

  /**
   * @brief Binary search for first element >= value.
   *
   * Materializes elements as needed during the search.
   *
   * @param value Value to search for
   * @return Index of first element >= value, or size() if none
   */
  size_t lower_bound(uint64_t value);

  /**
   * @brief Fully materialize the sorted index.
   *
   * After calling this, all access is O(1).
   */
  void materialize_all();

  /**
   * @brief Check if fully materialized.
   */
  bool is_fully_materialized() const { return materialized_.size() == total_size_; }

private:
  /// Materialize up to and including the given index
  void materialize_to(size_t idx);

  std::unique_ptr<SortedIndexIterator> iterator_;
  std::vector<uint64_t> materialized_;
  size_t total_size_ = 0;
};

} // namespace libvroom

#endif // LIBVROOM_COLUMN_INDEX_H
