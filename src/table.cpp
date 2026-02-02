#include "libvroom/table.h"

#include "libvroom/vroom.h"

#include <stdexcept>

namespace libvroom {

// =============================================================================
// Arrow stream private data structures (internal to this TU)
// =============================================================================

namespace {

/// Private data for the ArrowArrayStream - keeps Table alive and tracks state
struct TableStreamPrivate {
  std::shared_ptr<Table> table;
  bool batch_returned = false;
  std::string last_error;
};

/// Private data for struct schema - owns child schemas
struct StructSchemaPrivate {
  std::string name_storage;
  std::vector<std::unique_ptr<ArrowSchema>> child_schemas;
  std::vector<ArrowSchema*> child_schema_ptrs;
};

/// Private data for struct array - owns child arrays and keeps table alive
struct StructArrayPrivate {
  std::shared_ptr<Table> table; // Keep table alive while array is in use
  std::vector<std::unique_ptr<ArrowArray>> child_arrays;
  std::vector<ArrowArray*> child_array_ptrs;
  std::vector<const void*> struct_buffers;
};

// =============================================================================
// Arrow stream callbacks
// =============================================================================

/// Release callback for struct schema
void release_struct_schema(ArrowSchema* schema) {
  if (schema->release == nullptr)
    return;

  // Release all child schemas first
  if (schema->children) {
    for (int64_t i = 0; i < schema->n_children; ++i) {
      if (schema->children[i] && schema->children[i]->release) {
        schema->children[i]->release(schema->children[i]);
      }
    }
  }

  // Delete our private data (which owns the child ArrowSchema objects)
  if (schema->private_data) {
    delete static_cast<StructSchemaPrivate*>(schema->private_data);
  }

  schema->release = nullptr;
}

/// Release callback for struct array
void release_struct_array(ArrowArray* array) {
  if (array->release == nullptr)
    return;

  // Release all child arrays first
  if (array->children) {
    for (int64_t i = 0; i < array->n_children; ++i) {
      if (array->children[i] && array->children[i]->release) {
        array->children[i]->release(array->children[i]);
      }
    }
  }

  // Delete our private data (which owns the child ArrowArray objects and the table)
  if (array->private_data) {
    delete static_cast<StructArrayPrivate*>(array->private_data);
  }

  array->release = nullptr;
}

/// Stream get_schema callback
int table_stream_get_schema(ArrowArrayStream* stream, ArrowSchema* out) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  try {
    priv->table->export_schema(out);
    return 0;
  } catch (const std::exception& e) {
    priv->last_error = e.what();
    return -1;
  }
}

/// Stream get_next callback
int table_stream_get_next(ArrowArrayStream* stream, ArrowArray* out) {
  auto* stream_priv = static_cast<TableStreamPrivate*>(stream->private_data);

  if (stream_priv->batch_returned) {
    // No more batches - signal end of stream
    init_empty_array(out);
    return 0;
  }

  auto& table = stream_priv->table;

  // Create private data to own the child arrays and keep table alive
  auto* array_priv = new StructArrayPrivate();
  array_priv->table = table; // Keep table alive

  // Create child arrays for each column
  for (size_t i = 0; i < table->num_columns(); ++i) {
    // ArrowColumnPrivate is owned by the ArrowArray's release callback
    auto* child_priv = new ArrowColumnPrivate();
    auto child = std::make_unique<ArrowArray>();
    table->columns()[i]->export_to_arrow(child.get(), child_priv);

    array_priv->child_array_ptrs.push_back(child.get());
    array_priv->child_arrays.push_back(std::move(child));
  }

  // Set up struct array
  array_priv->struct_buffers = {nullptr}; // Struct has no buffers itself

  out->length = static_cast<int64_t>(table->num_rows());
  out->null_count = 0;
  out->offset = 0;
  out->n_buffers = 1;
  out->n_children = static_cast<int64_t>(table->num_columns());
  out->buffers = array_priv->struct_buffers.data();
  out->children = array_priv->child_array_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_array;
  out->private_data = array_priv;

  stream_priv->batch_returned = true;
  return 0;
}

/// Stream get_last_error callback
const char* table_stream_get_last_error(ArrowArrayStream* stream) {
  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  return priv->last_error.empty() ? nullptr : priv->last_error.c_str();
}

/// Stream release callback
void table_stream_release(ArrowArrayStream* stream) {
  if (stream->release == nullptr)
    return;

  auto* priv = static_cast<TableStreamPrivate*>(stream->private_data);
  delete priv;

  stream->release = nullptr;
}

} // anonymous namespace

// =============================================================================
// Table implementation
// =============================================================================

Table::Table(std::vector<ColumnSchema> schema,
             std::vector<std::unique_ptr<ArrowColumnBuilder>> columns, size_t num_rows)
    : schema_(std::move(schema)), columns_(std::move(columns)), num_rows_(num_rows) {}

std::vector<std::string> Table::column_names() const {
  std::vector<std::string> names;
  names.reserve(schema_.size());
  for (const auto& col : schema_) {
    names.push_back(col.name);
  }
  return names;
}

std::shared_ptr<Table> Table::from_parsed_chunks(const std::vector<ColumnSchema>& schema,
                                                 ParsedChunks& chunks) {
  // Empty chunks -> 0-row table with empty column builders matching schema
  if (chunks.chunks.empty()) {
    std::vector<std::unique_ptr<ArrowColumnBuilder>> empty_columns;
    empty_columns.reserve(schema.size());
    for (const auto& col_schema : schema) {
      empty_columns.push_back(ArrowColumnBuilder::create(col_schema.type));
    }
    return std::make_shared<Table>(schema, std::move(empty_columns), 0);
  }

  // Single chunk -> use directly (no copy)
  if (chunks.chunks.size() == 1) {
    return std::make_shared<Table>(schema, std::move(chunks.chunks[0]), chunks.total_rows);
  }

  // Multiple chunks -> merge into first chunk
  auto& first = chunks.chunks[0];
  for (size_t chunk_idx = 1; chunk_idx < chunks.chunks.size(); ++chunk_idx) {
    auto& other = chunks.chunks[chunk_idx];
    if (other.size() != first.size()) {
      throw std::runtime_error("Internal error: chunk " + std::to_string(chunk_idx) + " has " +
                               std::to_string(other.size()) + " columns, expected " +
                               std::to_string(first.size()));
    }
    for (size_t col_idx = 0; col_idx < first.size() && col_idx < other.size(); ++col_idx) {
      first[col_idx]->merge_from(*other[col_idx]);
    }
  }

  return std::make_shared<Table>(schema, std::move(first), chunks.total_rows);
}

// =============================================================================
// Arrow stream export
// =============================================================================

void Table::export_to_stream(ArrowArrayStream* out) {
  auto* priv = new TableStreamPrivate();
  priv->table = shared_from_this();

  out->get_schema = table_stream_get_schema;
  out->get_next = table_stream_get_next;
  out->get_last_error = table_stream_get_last_error;
  out->release = table_stream_release;
  out->private_data = priv;
}

void Table::export_schema(ArrowSchema* out) const {
  auto* schema_priv = new StructSchemaPrivate();
  schema_priv->name_storage = "";

  // Create child schemas for each column
  for (size_t i = 0; i < num_columns(); ++i) {
    auto child = std::make_unique<ArrowSchema>();
    columns_[i]->export_schema(child.get(), schema_[i].name);
    schema_priv->child_schema_ptrs.push_back(child.get());
    schema_priv->child_schemas.push_back(std::move(child));
  }

  // Set up struct schema
  out->format = arrow_format::STRUCT;
  out->name = schema_priv->name_storage.c_str();
  out->metadata = nullptr;
  out->flags = 0;
  out->n_children = static_cast<int64_t>(num_columns());
  out->children = schema_priv->child_schema_ptrs.data();
  out->dictionary = nullptr;
  out->release = release_struct_schema;
  out->private_data = schema_priv;
}

} // namespace libvroom
