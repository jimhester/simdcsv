#pragma once

#include "arrow_c_data.h"
#include "arrow_column_builder.h"
#include "arrow_export.h"
#include "types.h"

#include <memory>
#include <string>
#include <vector>

namespace libvroom {

// Forward declaration
struct ParsedChunks;

/// Table holds parsed CSV data as Arrow-compatible columns.
///
/// Implements the Arrow C Data Interface (ArrowArrayStream) for zero-copy
/// interoperability with Arrow consumers (PyArrow, R arrow, Polars, DuckDB).
///
/// Uses shared_from_this() to keep data alive while ArrowArrayStream consumers
/// hold references to the exported stream.
///
/// IMPORTANT: Table must be constructed via std::make_shared<Table>() or
/// Table::from_parsed_chunks() because export_to_stream() calls
/// shared_from_this(). Constructing a Table on the stack or with raw new
/// and then calling export_to_stream() will crash.
class Table : public std::enable_shared_from_this<Table> {
public:
  Table(std::vector<ColumnSchema> schema, std::vector<std::unique_ptr<ArrowColumnBuilder>> columns,
        size_t num_rows);

  // Non-copyable (unique_ptr members)
  Table(const Table&) = delete;
  Table& operator=(const Table&) = delete;
  Table(Table&&) = default;
  Table& operator=(Table&&) = default;

  /// Number of rows in the table
  size_t num_rows() const { return num_rows_; }

  /// Number of columns in the table
  size_t num_columns() const { return columns_.size(); }

  /// Column schema (name, type, nullable)
  const std::vector<ColumnSchema>& schema() const { return schema_; }

  /// Column names
  std::vector<std::string> column_names() const;

  /// Access to column builders (for Arrow export)
  const std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns() const { return columns_; }

  /// Export table as an ArrowArrayStream (single-batch).
  /// The stream keeps the Table alive via shared_ptr.
  /// Caller must call stream->release(stream) when done.
  void export_to_stream(ArrowArrayStream* out);

  /// Export table schema as an ArrowSchema (struct type with column children).
  /// Caller must call schema->release(schema) when done.
  void export_schema(ArrowSchema* out) const;

  /// Create a Table from ParsedChunks.
  /// - Empty chunks -> 0-row table with empty column builders matching schema
  /// - Single chunk -> use directly (no copy)
  /// - Multiple chunks -> merge via ArrowColumnBuilder::merge_from()
  static std::shared_ptr<Table> from_parsed_chunks(const std::vector<ColumnSchema>& schema,
                                                   ParsedChunks& chunks);

private:
  std::vector<ColumnSchema> schema_;
  std::vector<std::unique_ptr<ArrowColumnBuilder>> columns_;
  size_t num_rows_;
};

} // namespace libvroom
