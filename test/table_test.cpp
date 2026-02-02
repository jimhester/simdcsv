/**
 * @file table_test.cpp
 * @brief Tests for the Table class and Arrow stream export
 */

#include "libvroom.h"
#include "libvroom/arrow_c_data.h"
#include "libvroom/table.h"

#include <cstdio>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <string>
#include <unistd.h>
#include <vector>

// Counter for unique temp file names
static std::atomic<uint64_t> g_temp_counter{0};

// Helper to create a temporary CSV file
class TempCsvFile {
public:
  explicit TempCsvFile(const std::string& content) {
    uint64_t id = g_temp_counter.fetch_add(1);
    path_ = "/tmp/table_test_" + std::to_string(getpid()) + "_" + std::to_string(id) + ".csv";
    std::ofstream f(path_);
    f << content;
    f.close();
  }

  ~TempCsvFile() { std::remove(path_.c_str()); }

  const std::string& path() const { return path_; }

private:
  std::string path_;
};

// Helper to release an ArrowSchema if it has a release callback
void safe_release_schema(libvroom::ArrowSchema* schema) {
  if (schema && schema->release) {
    schema->release(schema);
  }
}

// Helper to release an ArrowArray if it has a release callback
void safe_release_array(libvroom::ArrowArray* array) {
  if (array && array->release) {
    array->release(array);
  }
}

// Helper to release an ArrowArrayStream if it has a release callback
void safe_release_stream(libvroom::ArrowArrayStream* stream) {
  if (stream && stream->release) {
    stream->release(stream);
  }
}

// =============================================================================
// Table construction tests
// =============================================================================

TEST(TableTest, TableFromSingleChunk) {
  // Create column builders manually
  auto col1 = libvroom::ArrowColumnBuilder::create_int32();
  auto col2 = libvroom::ArrowColumnBuilder::create_string();

  // Add some data via context
  auto ctx1 = col1->create_context();
  auto ctx2 = col2->create_context();
  ctx1.append_fn(ctx1, "42");
  ctx1.append_fn(ctx1, "7");
  ctx2.append_fn(ctx2, "hello");
  ctx2.append_fn(ctx2, "world");

  std::vector<libvroom::ColumnSchema> schema = {
      {"id", libvroom::DataType::INT32, true, 0},
      {"name", libvroom::DataType::STRING, true, 1},
  };

  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
  columns.push_back(std::move(col1));
  columns.push_back(std::move(col2));

  auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 2);

  EXPECT_EQ(table->num_rows(), 2);
  EXPECT_EQ(table->num_columns(), 2);
  EXPECT_EQ(table->schema().size(), 2);
  EXPECT_EQ(table->schema()[0].name, "id");
  EXPECT_EQ(table->schema()[1].name, "name");

  auto names = table->column_names();
  EXPECT_EQ(names.size(), 2);
  EXPECT_EQ(names[0], "id");
  EXPECT_EQ(names[1], "name");
}

TEST(TableTest, TableFromMultipleChunks) {
  // Simulate multiple parsed chunks
  std::vector<libvroom::ColumnSchema> schema = {
      {"value", libvroom::DataType::INT32, true, 0},
  };

  // Create two chunks
  libvroom::ParsedChunks chunks;

  // Chunk 1: [1, 2, 3]
  {
    std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> chunk;
    auto col = libvroom::ArrowColumnBuilder::create_int32();
    auto ctx = col->create_context();
    ctx.append_fn(ctx, "1");
    ctx.append_fn(ctx, "2");
    ctx.append_fn(ctx, "3");
    chunk.push_back(std::move(col));
    chunks.chunks.push_back(std::move(chunk));
  }

  // Chunk 2: [4, 5]
  {
    std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> chunk;
    auto col = libvroom::ArrowColumnBuilder::create_int32();
    auto ctx = col->create_context();
    ctx.append_fn(ctx, "4");
    ctx.append_fn(ctx, "5");
    chunk.push_back(std::move(col));
    chunks.chunks.push_back(std::move(chunk));
  }

  chunks.total_rows = 5;

  auto table = libvroom::Table::from_parsed_chunks(schema, chunks);

  EXPECT_EQ(table->num_rows(), 5);
  EXPECT_EQ(table->num_columns(), 1);
  // After merge, the single column should have 5 values
  EXPECT_EQ(table->columns()[0]->size(), 5);
}

TEST(TableTest, TableFromEmpty) {
  std::vector<libvroom::ColumnSchema> schema = {
      {"value", libvroom::DataType::INT32, true, 0},
      {"name", libvroom::DataType::STRING, true, 1},
  };

  libvroom::ParsedChunks chunks;
  chunks.total_rows = 0;

  auto table = libvroom::Table::from_parsed_chunks(schema, chunks);

  EXPECT_EQ(table->num_rows(), 0);
  // Empty table should still have columns matching schema
  EXPECT_EQ(table->num_columns(), 2);
  EXPECT_EQ(table->columns()[0]->type(), libvroom::DataType::INT32);
  EXPECT_EQ(table->columns()[1]->type(), libvroom::DataType::STRING);
  EXPECT_EQ(table->columns()[0]->size(), 0);
  EXPECT_EQ(table->columns()[1]->size(), 0);

  // Schema should export correctly (0-row struct with 2 children)
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowSchema arrow_schema;
  ASSERT_EQ(stream.get_schema(&stream, &arrow_schema), 0);
  EXPECT_EQ(arrow_schema.n_children, 2);
  EXPECT_STREQ(arrow_schema.children[0]->format, "i");
  EXPECT_STREQ(arrow_schema.children[1]->format, "u");

  safe_release_schema(&arrow_schema);
  safe_release_stream(&stream);
}

// =============================================================================
// Arrow stream export tests
// =============================================================================

TEST(TableTest, ExportArrowStreamSchema) {
  auto col1 = libvroom::ArrowColumnBuilder::create_int32();
  auto col2 = libvroom::ArrowColumnBuilder::create_float64();
  auto col3 = libvroom::ArrowColumnBuilder::create_string();

  std::vector<libvroom::ColumnSchema> schema = {
      {"id", libvroom::DataType::INT32, true, 0},
      {"score", libvroom::DataType::FLOAT64, true, 1},
      {"name", libvroom::DataType::STRING, true, 2},
  };

  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
  columns.push_back(std::move(col1));
  columns.push_back(std::move(col2));
  columns.push_back(std::move(col3));

  auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 0);

  // Export stream
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);
  ASSERT_NE(stream.release, nullptr);

  // Get schema
  libvroom::ArrowSchema arrow_schema;
  int rc = stream.get_schema(&stream, &arrow_schema);
  ASSERT_EQ(rc, 0);
  ASSERT_NE(arrow_schema.release, nullptr);

  // Verify struct format
  EXPECT_STREQ(arrow_schema.format, "+s");
  EXPECT_EQ(arrow_schema.n_children, 3);

  // Verify child schemas
  EXPECT_STREQ(arrow_schema.children[0]->format, "i"); // INT32
  EXPECT_STREQ(arrow_schema.children[0]->name, "id");
  EXPECT_STREQ(arrow_schema.children[1]->format, "g"); // FLOAT64
  EXPECT_STREQ(arrow_schema.children[1]->name, "score");
  EXPECT_STREQ(arrow_schema.children[2]->format, "u"); // UTF8
  EXPECT_STREQ(arrow_schema.children[2]->name, "name");

  // Clean up
  safe_release_schema(&arrow_schema);
  safe_release_stream(&stream);
}

TEST(TableTest, ExportArrowStreamData) {
  auto col_int = libvroom::ArrowColumnBuilder::create_int32();
  auto col_dbl = libvroom::ArrowColumnBuilder::create_float64();
  auto col_str = libvroom::ArrowColumnBuilder::create_string();

  auto ctx_int = col_int->create_context();
  auto ctx_dbl = col_dbl->create_context();
  auto ctx_str = col_str->create_context();

  ctx_int.append_fn(ctx_int, "10");
  ctx_int.append_fn(ctx_int, "20");
  ctx_int.append_fn(ctx_int, "30");

  ctx_dbl.append_fn(ctx_dbl, "1.5");
  ctx_dbl.append_fn(ctx_dbl, "2.5");
  ctx_dbl.append_fn(ctx_dbl, "3.5");

  ctx_str.append_fn(ctx_str, "alpha");
  ctx_str.append_fn(ctx_str, "beta");
  ctx_str.append_fn(ctx_str, "gamma");

  std::vector<libvroom::ColumnSchema> schema = {
      {"id", libvroom::DataType::INT32, true, 0},
      {"score", libvroom::DataType::FLOAT64, true, 1},
      {"label", libvroom::DataType::STRING, true, 2},
  };

  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
  columns.push_back(std::move(col_int));
  columns.push_back(std::move(col_dbl));
  columns.push_back(std::move(col_str));

  auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 3);

  // Export stream
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // Get first (and only) batch
  libvroom::ArrowArray batch;
  int rc = stream.get_next(&stream, &batch);
  ASSERT_EQ(rc, 0);
  ASSERT_NE(batch.release, nullptr);

  // Verify batch dimensions
  EXPECT_EQ(batch.length, 3);
  EXPECT_EQ(batch.n_children, 3);

  // Verify int32 column data
  auto* int_child = batch.children[0];
  EXPECT_EQ(int_child->length, 3);
  auto* int_data = static_cast<const int32_t*>(int_child->buffers[1]);
  EXPECT_EQ(int_data[0], 10);
  EXPECT_EQ(int_data[1], 20);
  EXPECT_EQ(int_data[2], 30);

  // Verify float64 column data
  auto* dbl_child = batch.children[1];
  EXPECT_EQ(dbl_child->length, 3);
  auto* dbl_data = static_cast<const double*>(dbl_child->buffers[1]);
  EXPECT_DOUBLE_EQ(dbl_data[0], 1.5);
  EXPECT_DOUBLE_EQ(dbl_data[1], 2.5);
  EXPECT_DOUBLE_EQ(dbl_data[2], 3.5);

  // Verify string column data (buffers: [validity, offsets, data])
  auto* str_child = batch.children[2];
  EXPECT_EQ(str_child->length, 3);
  auto* offsets = static_cast<const int32_t*>(str_child->buffers[1]);
  auto* char_data = static_cast<const char*>(str_child->buffers[2]);
  // "alpha" = 5 chars, "beta" = 4 chars, "gamma" = 5 chars
  EXPECT_EQ(offsets[0], 0);
  EXPECT_EQ(offsets[1], 5);
  EXPECT_EQ(offsets[2], 9);
  EXPECT_EQ(offsets[3], 14);
  std::string s0(char_data + offsets[0], offsets[1] - offsets[0]);
  std::string s1(char_data + offsets[1], offsets[2] - offsets[1]);
  std::string s2(char_data + offsets[2], offsets[3] - offsets[2]);
  EXPECT_EQ(s0, "alpha");
  EXPECT_EQ(s1, "beta");
  EXPECT_EQ(s2, "gamma");

  // Clean up
  safe_release_array(&batch);
  safe_release_stream(&stream);
}

TEST(TableTest, ExportArrowStreamEndOfStream) {
  auto col = libvroom::ArrowColumnBuilder::create_int32();
  auto ctx = col->create_context();
  ctx.append_fn(ctx, "1");

  std::vector<libvroom::ColumnSchema> schema = {
      {"x", libvroom::DataType::INT32, true, 0},
  };

  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
  columns.push_back(std::move(col));

  auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 1);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  // First call: get the batch
  libvroom::ArrowArray batch1;
  ASSERT_EQ(stream.get_next(&stream, &batch1), 0);
  ASSERT_NE(batch1.release, nullptr);
  EXPECT_EQ(batch1.length, 1);

  // Second call: end of stream (release is nullptr)
  libvroom::ArrowArray batch2;
  ASSERT_EQ(stream.get_next(&stream, &batch2), 0);
  EXPECT_EQ(batch2.release, nullptr);

  // Clean up
  safe_release_array(&batch1);
  safe_release_stream(&stream);
}

TEST(TableTest, ExportArrowStreamLifecycle) {
  // Verify that the stream keeps the Table alive after the original shared_ptr is dropped
  libvroom::ArrowArrayStream stream;

  {
    auto col = libvroom::ArrowColumnBuilder::create_int32();
    auto ctx = col->create_context();
    ctx.append_fn(ctx, "42");

    std::vector<libvroom::ColumnSchema> schema = {
        {"val", libvroom::DataType::INT32, true, 0},
    };

    std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
    columns.push_back(std::move(col));

    auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 1);
    table->export_to_stream(&stream);
    // table shared_ptr goes out of scope here, but stream keeps it alive
  }

  // Stream should still be valid
  ASSERT_NE(stream.release, nullptr);

  // Should be able to get schema
  libvroom::ArrowSchema arrow_schema;
  ASSERT_EQ(stream.get_schema(&stream, &arrow_schema), 0);
  EXPECT_STREQ(arrow_schema.format, "+s");
  EXPECT_EQ(arrow_schema.n_children, 1);
  EXPECT_STREQ(arrow_schema.children[0]->format, "i");

  // Should be able to get data
  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_NE(batch.release, nullptr);
  EXPECT_EQ(batch.length, 1);
  auto* data = static_cast<const int32_t*>(batch.children[0]->buffers[1]);
  EXPECT_EQ(data[0], 42);

  safe_release_schema(&arrow_schema);
  safe_release_array(&batch);
  safe_release_stream(&stream);
}

TEST(TableTest, ExportSchemaDirectly) {
  auto col = libvroom::ArrowColumnBuilder::create_float64();

  std::vector<libvroom::ColumnSchema> schema = {
      {"temperature", libvroom::DataType::FLOAT64, true, 0},
  };

  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
  columns.push_back(std::move(col));

  auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 0);

  libvroom::ArrowSchema arrow_schema;
  table->export_schema(&arrow_schema);

  ASSERT_NE(arrow_schema.release, nullptr);
  EXPECT_STREQ(arrow_schema.format, "+s");
  EXPECT_EQ(arrow_schema.n_children, 1);
  EXPECT_STREQ(arrow_schema.children[0]->format, "g");
  EXPECT_STREQ(arrow_schema.children[0]->name, "temperature");

  safe_release_schema(&arrow_schema);
}

// =============================================================================
// read_csv_to_table end-to-end tests
// =============================================================================

TEST(TableTest, ReadCsvToTable) {
  TempCsvFile csv("a,b,c\n1,2.5,hello\n3,4.5,world\n");

  auto table = libvroom::read_csv_to_table(csv.path());

  EXPECT_EQ(table->num_rows(), 2);
  EXPECT_EQ(table->num_columns(), 3);

  auto names = table->column_names();
  EXPECT_EQ(names[0], "a");
  EXPECT_EQ(names[1], "b");
  EXPECT_EQ(names[2], "c");

  // Verify the table can be exported as a stream
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowSchema arrow_schema;
  ASSERT_EQ(stream.get_schema(&stream, &arrow_schema), 0);
  EXPECT_EQ(arrow_schema.n_children, 3);

  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  EXPECT_EQ(batch.length, 2);

  safe_release_schema(&arrow_schema);
  safe_release_array(&batch);
  safe_release_stream(&stream);
}

TEST(TableTest, ReadCsvToTableTypes) {
  // Test type inference produces correct Arrow types
  TempCsvFile csv("int_col,float_col,str_col,bool_col\n"
                  "42,3.14,hello,true\n"
                  "7,2.71,world,false\n");

  auto table = libvroom::read_csv_to_table(csv.path());
  ASSERT_EQ(table->num_columns(), 4);

  // Export stream and check schema formats
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowSchema arrow_schema;
  ASSERT_EQ(stream.get_schema(&stream, &arrow_schema), 0);
  ASSERT_EQ(arrow_schema.n_children, 4);

  // int_col should be int32
  EXPECT_STREQ(arrow_schema.children[0]->format, "i");
  EXPECT_STREQ(arrow_schema.children[0]->name, "int_col");

  // float_col should be float64
  EXPECT_STREQ(arrow_schema.children[1]->format, "g");
  EXPECT_STREQ(arrow_schema.children[1]->name, "float_col");

  // str_col should be utf8
  EXPECT_STREQ(arrow_schema.children[2]->format, "u");
  EXPECT_STREQ(arrow_schema.children[2]->name, "str_col");

  // bool_col stored as uint8 ("C" format)
  EXPECT_STREQ(arrow_schema.children[3]->format, "C");
  EXPECT_STREQ(arrow_schema.children[3]->name, "bool_col");

  safe_release_schema(&arrow_schema);
  safe_release_stream(&stream);
}

TEST(TableTest, ReadCsvToTableWithTestData) {
  // Use test data that gets copied by CMake
  auto table = libvroom::read_csv_to_table("test/data/basic/simple.csv");

  EXPECT_EQ(table->num_rows(), 3);
  EXPECT_EQ(table->num_columns(), 3);

  auto names = table->column_names();
  EXPECT_EQ(names[0], "A");
  EXPECT_EQ(names[1], "B");
  EXPECT_EQ(names[2], "C");

  // Verify data through Arrow stream
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_EQ(batch.length, 3);
  ASSERT_EQ(batch.n_children, 3);

  // All columns should be int32 (values are 1-9)
  auto* col_a = batch.children[0];
  auto* a_data = static_cast<const int32_t*>(col_a->buffers[1]);
  EXPECT_EQ(a_data[0], 1);
  EXPECT_EQ(a_data[1], 4);
  EXPECT_EQ(a_data[2], 7);

  safe_release_array(&batch);
  safe_release_stream(&stream);
}

TEST(TableTest, ReadCsvToTableNonExistent) {
  EXPECT_THROW(libvroom::read_csv_to_table("/nonexistent/file.csv"), std::runtime_error);
}

TEST(TableTest, ExportArrowStreamWithNulls) {
  // Verify Arrow export correctly handles null values (validity bitmap)
  auto col = libvroom::ArrowColumnBuilder::create_int32();
  auto ctx = col->create_context();
  ctx.append_fn(ctx, "10");
  ctx.append_null_fn(ctx); // null
  ctx.append_fn(ctx, "30");

  std::vector<libvroom::ColumnSchema> schema = {
      {"val", libvroom::DataType::INT32, true, 0},
  };

  std::vector<std::unique_ptr<libvroom::ArrowColumnBuilder>> columns;
  columns.push_back(std::move(col));

  auto table = std::make_shared<libvroom::Table>(schema, std::move(columns), 3);

  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowArray batch;
  ASSERT_EQ(stream.get_next(&stream, &batch), 0);
  ASSERT_EQ(batch.length, 3);
  ASSERT_EQ(batch.n_children, 1);

  auto* child = batch.children[0];
  EXPECT_EQ(child->length, 3);
  EXPECT_EQ(child->null_count, 1);

  // Validity bitmap should be non-null (we have nulls)
  ASSERT_NE(child->buffers[0], nullptr);

  // Check the validity bitmap: bit 0 = valid, bit 1 = null, bit 2 = valid
  auto* validity = static_cast<const uint8_t*>(child->buffers[0]);
  EXPECT_TRUE((validity[0] & (1 << 0)) != 0) << "Row 0 should be valid";
  EXPECT_TRUE((validity[0] & (1 << 1)) == 0) << "Row 1 should be null";
  EXPECT_TRUE((validity[0] & (1 << 2)) != 0) << "Row 2 should be valid";

  // Check data values (valid rows should have correct values)
  auto* data = static_cast<const int32_t*>(child->buffers[1]);
  EXPECT_EQ(data[0], 10);
  EXPECT_EQ(data[2], 30);

  safe_release_array(&batch);
  safe_release_stream(&stream);
}

TEST(TableTest, ExportAllColumnTypes) {
  // Test all supported column types export correctly
  // Need enough data rows for type inference to detect date/timestamp
  TempCsvFile csv("int_col,bigint_col,float_col,str_col,date_col,ts_col\n"
                  "42,9999999999,3.14,hello,2024-01-15,2024-01-15T10:30:00\n"
                  "7,8888888888,2.71,world,2024-02-20,2024-02-20T14:00:00\n"
                  "99,7777777777,1.41,foo,2024-03-25,2024-03-25T08:15:00\n");

  auto table = libvroom::read_csv_to_table(csv.path());
  ASSERT_EQ(table->num_columns(), 6);

  // Export and verify schema
  libvroom::ArrowArrayStream stream;
  table->export_to_stream(&stream);

  libvroom::ArrowSchema arrow_schema;
  ASSERT_EQ(stream.get_schema(&stream, &arrow_schema), 0);

  // Check date and timestamp formats if type inference detected them
  bool found_date = false, found_timestamp = false;
  for (int64_t i = 0; i < arrow_schema.n_children; ++i) {
    std::string fmt = arrow_schema.children[i]->format;
    if (fmt == "tdD")
      found_date = true;
    if (fmt == "tsu:")
      found_timestamp = true;
  }
  EXPECT_TRUE(found_date) << "Expected date column to be detected";
  EXPECT_TRUE(found_timestamp) << "Expected timestamp column to be detected";

  safe_release_schema(&arrow_schema);
  safe_release_stream(&stream);
}
