/**
 * @file column_major_test.cpp
 * @brief Tests for column-major index layout (compact_column_major).
 *
 * Tests the column-major transpose functionality added for ALTREP/Arrow
 * access patterns. Verifies correctness of transpose, column access,
 * and row reconstruction.
 */

#include "libvroom.h"

#include "two_pass.h"

#include <gtest/gtest.h>
#include <numeric>
#include <string>
#include <thread>
#include <vector>

using namespace libvroom;

class ColumnMajorTest : public ::testing::Test {
protected:
  // Helper to create CSV content with known values
  std::string makeCSV(size_t rows, size_t cols) {
    std::string csv;
    for (size_t r = 0; r < rows; ++r) {
      for (size_t c = 0; c < cols; ++c) {
        if (c > 0)
          csv += ",";
        csv += std::to_string(r * cols + c);
      }
      csv += "\n";
    }
    return csv;
  }

  // Parse CSV and return the index
  ParseIndex parseCSV(const std::string& content, size_t n_threads = 1) {
    Parser parser(n_threads);
    ParseOptions opts;
    opts.dialect = Dialect::csv();
    auto result =
        parser.parse(reinterpret_cast<const uint8_t*>(content.data()), content.size(), opts);
    EXPECT_TRUE(result.successful);
    return std::move(result.idx);
  }
};

// Basic functionality tests

TEST_F(ColumnMajorTest, CompactColumnMajor_BasicFunctionality) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  EXPECT_FALSE(idx.is_column_major());
  idx.compact_column_major();
  EXPECT_TRUE(idx.is_column_major());

  // After compact_column_major, flat_indexes should be freed
  EXPECT_FALSE(idx.is_flat());
}

TEST_F(ColumnMajorTest, CompactColumnMajor_Idempotent) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  idx.compact_column_major();
  uint64_t* first_ptr = idx.col_indexes;

  idx.compact_column_major(); // Second call should be no-op
  EXPECT_EQ(first_ptr, idx.col_indexes);
}

TEST_F(ColumnMajorTest, NumRows_ReturnsCorrectCount) {
  std::string csv = makeCSV(100, 10);
  auto idx = parseCSV(csv);

  idx.compact_column_major();
  EXPECT_EQ(100u, idx.num_rows());
  EXPECT_EQ(10u, idx.columns);
}

TEST_F(ColumnMajorTest, Column_ReturnsValidPointer) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  idx.compact_column_major();

  for (size_t col = 0; col < 5; ++col) {
    const uint64_t* col_data = idx.column(col);
    ASSERT_NE(nullptr, col_data) << "Column " << col << " returned nullptr";
  }

  // Out of bounds should return nullptr
  EXPECT_EQ(nullptr, idx.column(5));
  EXPECT_EQ(nullptr, idx.column(100));
}

TEST_F(ColumnMajorTest, Column_ReturnsNullBeforeCompact) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  // Before compact_column_major, column() should return nullptr
  EXPECT_EQ(nullptr, idx.column(0));
}

// Correctness tests - verify transpose is correct

TEST_F(ColumnMajorTest, TransposeCorrectness_SmallMatrix) {
  std::string csv = makeCSV(5, 3);
  auto idx = parseCSV(csv);

  // First compact to row-major to get expected values
  idx.compact();
  ASSERT_TRUE(idx.is_flat());

  // Store expected row-major values
  std::vector<uint64_t> row_major(idx.flat_indexes, idx.flat_indexes + idx.flat_indexes_count);

  // Now compact to column-major (this will free flat_indexes)
  idx.compact_column_major();
  ASSERT_TRUE(idx.is_column_major());

  // Verify transpose: col_indexes[col * nrows + row] == row_major[row * ncols + col]
  uint64_t nrows = idx.num_rows();
  uint64_t ncols = idx.columns;

  for (uint64_t row = 0; row < nrows; ++row) {
    for (uint64_t col = 0; col < ncols; ++col) {
      uint64_t row_major_idx = row * ncols + col;
      uint64_t col_major_idx = col * nrows + row;

      EXPECT_EQ(row_major[row_major_idx], idx.col_indexes[col_major_idx])
          << "Mismatch at row=" << row << ", col=" << col;
    }
  }
}

TEST_F(ColumnMajorTest, TransposeCorrectness_LargerMatrix) {
  std::string csv = makeCSV(100, 20);
  auto idx = parseCSV(csv);

  idx.compact();
  std::vector<uint64_t> row_major(idx.flat_indexes, idx.flat_indexes + idx.flat_indexes_count);

  idx.compact_column_major();

  uint64_t nrows = idx.num_rows();
  uint64_t ncols = idx.columns;

  // Sample some positions to verify
  std::vector<std::pair<uint64_t, uint64_t>> test_positions = {{0, 0},   {0, 19}, {99, 0}, {99, 19},
                                                               {50, 10}, {25, 5}, {75, 15}};

  for (const auto& [row, col] : test_positions) {
    uint64_t row_major_idx = row * ncols + col;
    uint64_t col_major_idx = col * nrows + row;

    EXPECT_EQ(row_major[row_major_idx], idx.col_indexes[col_major_idx])
        << "Mismatch at row=" << row << ", col=" << col;
  }
}

// Column access tests

TEST_F(ColumnMajorTest, ColumnAccess_SequentialMemory) {
  std::string csv = makeCSV(100, 10);
  auto idx = parseCSV(csv);

  idx.compact_column_major();

  // Each column's data should be at contiguous memory locations
  for (size_t col = 0; col < idx.columns; ++col) {
    const uint64_t* col_data = idx.column(col);
    uint64_t expected_offset = col * idx.num_rows();

    EXPECT_EQ(&idx.col_indexes[expected_offset], col_data)
        << "Column " << col << " not at expected offset";
  }
}

// Row reconstruction tests

TEST_F(ColumnMajorTest, GetRowFields_ReturnsCorrectValues) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  idx.compact();
  std::vector<uint64_t> row_major(idx.flat_indexes, idx.flat_indexes + idx.flat_indexes_count);

  idx.compact_column_major();

  std::vector<uint64_t> row_fields;

  for (size_t row = 0; row < idx.num_rows(); ++row) {
    ASSERT_TRUE(idx.get_row_fields(row, row_fields));
    ASSERT_EQ(idx.columns, row_fields.size());

    for (size_t col = 0; col < idx.columns; ++col) {
      uint64_t expected = row_major[row * idx.columns + col];
      EXPECT_EQ(expected, row_fields[col]) << "Row " << row << ", col " << col;
    }
  }
}

TEST_F(ColumnMajorTest, GetRowFields_OutOfBounds) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  idx.compact_column_major();

  std::vector<uint64_t> row_fields;

  EXPECT_FALSE(idx.get_row_fields(10, row_fields));  // Out of bounds
  EXPECT_FALSE(idx.get_row_fields(100, row_fields)); // Way out of bounds
}

TEST_F(ColumnMajorTest, GetRowFields_ReturnsFalseBeforeCompact) {
  std::string csv = makeCSV(10, 5);
  auto idx = parseCSV(csv);

  std::vector<uint64_t> row_fields;
  EXPECT_FALSE(idx.get_row_fields(0, row_fields));
}

// Multi-threaded tests

TEST_F(ColumnMajorTest, MultiThreaded_CorrectTranspose) {
  std::string csv = makeCSV(1000, 50);
  auto idx = parseCSV(csv, 4); // Parse with 4 threads

  idx.compact();
  std::vector<uint64_t> row_major(idx.flat_indexes, idx.flat_indexes + idx.flat_indexes_count);

  // Transpose with multiple threads
  idx.compact_column_major(4);

  uint64_t nrows = idx.num_rows();
  uint64_t ncols = idx.columns;

  // Verify all positions
  for (uint64_t row = 0; row < nrows; ++row) {
    for (uint64_t col = 0; col < ncols; ++col) {
      uint64_t row_major_idx = row * ncols + col;
      uint64_t col_major_idx = col * nrows + row;

      EXPECT_EQ(row_major[row_major_idx], idx.col_indexes[col_major_idx])
          << "Mismatch at row=" << row << ", col=" << col;
    }
  }
}

TEST_F(ColumnMajorTest, MultiThreaded_VariousThreadCounts) {
  std::string csv = makeCSV(500, 20);

  for (size_t threads : {1, 2, 4, 8}) {
    auto idx = parseCSV(csv, threads);

    idx.compact();
    std::vector<uint64_t> row_major(idx.flat_indexes, idx.flat_indexes + idx.flat_indexes_count);

    idx.compact_column_major(threads);

    // Spot check
    uint64_t nrows = idx.num_rows();
    EXPECT_EQ(row_major[0], idx.col_indexes[0]);                            // (0,0)
    EXPECT_EQ(row_major[19], idx.col_indexes[19 * nrows]);                  // (0,19)
    EXPECT_EQ(row_major[499 * 20], idx.col_indexes[499]);                   // (499,0)
    EXPECT_EQ(row_major[499 * 20 + 19], idx.col_indexes[19 * nrows + 499]); // (499,19)
  }
}

// Edge cases

TEST_F(ColumnMajorTest, SingleRow) {
  std::string csv = "a,b,c,d,e\n";
  auto idx = parseCSV(csv);

  idx.compact_column_major();

  EXPECT_EQ(1u, idx.num_rows());
  EXPECT_EQ(5u, idx.columns);
  EXPECT_TRUE(idx.is_column_major());

  // All columns should be accessible
  for (size_t col = 0; col < 5; ++col) {
    EXPECT_NE(nullptr, idx.column(col));
  }
}

TEST_F(ColumnMajorTest, SingleColumn) {
  std::string csv = "a\nb\nc\nd\ne\n";
  auto idx = parseCSV(csv);

  idx.compact_column_major();

  EXPECT_EQ(5u, idx.num_rows());
  EXPECT_EQ(1u, idx.columns);
  EXPECT_TRUE(idx.is_column_major());

  EXPECT_NE(nullptr, idx.column(0));
  EXPECT_EQ(nullptr, idx.column(1));
}

TEST_F(ColumnMajorTest, EmptyCSV) {
  std::string csv = "";
  Parser parser(1);
  ParseOptions opts;
  opts.dialect = Dialect::csv();
  auto result = parser.parse(reinterpret_cast<const uint8_t*>(csv.data()), csv.size(), opts);

  // Empty CSV - compact_column_major should handle gracefully
  result.idx.compact_column_major();

  // Should not crash, and is_column_major should be false (no data)
  EXPECT_FALSE(result.idx.is_column_major());
}

// Memory tests

TEST_F(ColumnMajorTest, FlatIndexFreedAfterColumnMajor) {
  std::string csv = makeCSV(100, 10);
  auto idx = parseCSV(csv);

  idx.compact();
  EXPECT_TRUE(idx.is_flat());
  EXPECT_NE(nullptr, idx.flat_indexes);

  idx.compact_column_major();

  // flat_indexes should be null after column-major compaction
  EXPECT_EQ(nullptr, idx.flat_indexes);
  EXPECT_EQ(0u, idx.flat_indexes_count);
  EXPECT_FALSE(idx.is_flat());
}

// Move semantics

TEST_F(ColumnMajorTest, MoveConstructor_PreservesColumnMajor) {
  std::string csv = makeCSV(50, 10);
  auto idx = parseCSV(csv);

  idx.compact_column_major();
  uint64_t* original_ptr = idx.col_indexes;
  uint64_t original_count = idx.col_indexes_count;

  ParseIndex moved(std::move(idx));

  EXPECT_EQ(original_ptr, moved.col_indexes);
  EXPECT_EQ(original_count, moved.col_indexes_count);
  EXPECT_TRUE(moved.is_column_major());

  // Original should be empty
  EXPECT_EQ(nullptr, idx.col_indexes);
  EXPECT_EQ(0u, idx.col_indexes_count);
}

TEST_F(ColumnMajorTest, MoveAssignment_PreservesColumnMajor) {
  std::string csv = makeCSV(50, 10);
  auto idx = parseCSV(csv);

  idx.compact_column_major();
  uint64_t* original_ptr = idx.col_indexes;

  ParseIndex moved;
  moved = std::move(idx);

  EXPECT_EQ(original_ptr, moved.col_indexes);
  EXPECT_TRUE(moved.is_column_major());
}

// Shared ownership tests

TEST_F(ColumnMajorTest, Share_PreservesColumnMajor) {
  std::string csv = makeCSV(50, 10);
  auto idx = parseCSV(csv);

  idx.compact_column_major();
  uint64_t* original_ptr = idx.col_indexes;
  uint64_t original_count = idx.col_indexes_count;

  auto shared = idx.share();

  EXPECT_EQ(original_ptr, shared->col_indexes);
  EXPECT_EQ(original_count, shared->col_indexes_count);
  EXPECT_TRUE(shared->is_column_major());

  // Verify column access works on shared index
  for (size_t col = 0; col < 10; ++col) {
    EXPECT_NE(nullptr, shared->column(col));
  }
}

TEST_F(ColumnMajorTest, Share_ColumnMajorAfterShare) {
  std::string csv = makeCSV(50, 10);
  auto idx = parseCSV(csv);

  // First share, then compact_column_major
  auto shared1 = idx.share();
  idx.compact_column_major();

  // Share again after column-major compaction
  auto shared2 = idx.share();

  EXPECT_TRUE(idx.is_column_major());
  EXPECT_TRUE(shared2->is_column_major());

  // Original shared index should not have column-major (was shared before compaction)
  EXPECT_FALSE(shared1->is_column_major());
}
