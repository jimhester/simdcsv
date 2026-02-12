# Type Coercion Error Reporting Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Report per-field type coercion failures (e.g., "abc" in an integer column) so R's `problems()` can show exactly which rows/columns had parsing failures.

**Architecture:** Add `ErrorCode::TYPE_COERCION` to the error enum. Extend `FastArrowContext` with optional error-reporting metadata (ErrorCollector pointer, column index, column name, expected type, and a mutable row counter pointer). When error collection is enabled and a type-specific append fails to parse, report the error through the ErrorCollector. When disabled (default), zero overhead — the pointer is null and the hot path has a single branch.

**Tech Stack:** C++20, Google Test, CMake

---

### Task 1: Add ErrorCode::TYPE_COERCION and wire up formatting

**Files:**
- Modify: `include/libvroom/error.h:37-68` (ErrorCode enum)
- Modify: `src/error.cpp:7-46` (error_code_to_string switch)

**Step 1: Add TYPE_COERCION to ErrorCode enum**

In `include/libvroom/error.h`, after the "General errors" section (line 67), add a new section before the closing `};`:

```cpp
  // Type coercion errors
  TYPE_COERCION, ///< Field value cannot be parsed as the target column type
```

Insert this between `INTERNAL_ERROR` and the closing `}` of the enum.

**Step 2: Add TYPE_COERCION to error_code_to_string**

In `src/error.cpp`, add a new case before the `default:` in `error_code_to_string()`:

```cpp
  case ErrorCode::TYPE_COERCION:
    return "TYPE_COERCION";
```

**Step 3: Build to verify compilation**

Run: `cmake --build build -j$(nproc)`
Expected: Clean compilation

**Step 4: Commit**

```bash
git add include/libvroom/error.h src/error.cpp
git commit -m "feat: add ErrorCode::TYPE_COERCION variant"
```

---

### Task 2: Extend FastArrowContext with error reporting fields

**Files:**
- Modify: `include/libvroom/fast_arrow_context.h:22-39` (FastArrowContext class)

**Step 1: Write failing test for error reporting through FastArrowContext**

Create test in `test/error_handling_test.cpp`. Add near the end of the file:

```cpp
// ============================================================================
// Type coercion error reporting tests
// ============================================================================

TEST(TypeCoercionErrorTest, Int32CoercionErrorReported) {
  // Setup: create an int32 column builder and context
  auto builder = libvroom::ArrowColumnBuilder::create_int32();
  auto ctx = builder->create_context();

  // Setup error reporting on the context
  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row_number = 5;
  ctx.error_collector = &collector;
  ctx.error_row = &row_number;
  ctx.error_col_index = 2;
  ctx.error_col_name = "age";
  ctx.error_expected_type = libvroom::DataType::INT32;

  // Append a valid value - no error
  ctx.append(std::string_view("42"));
  EXPECT_EQ(collector.error_count(), 0);

  // Append an invalid value - should report error
  row_number = 6;
  ctx.append(std::string_view("abc"));
  ASSERT_EQ(collector.error_count(), 1);

  const auto& err = collector.errors()[0];
  EXPECT_EQ(err.code, libvroom::ErrorCode::TYPE_COERCION);
  EXPECT_EQ(err.severity, libvroom::ErrorSeverity::RECOVERABLE);
  EXPECT_EQ(err.line, 6);
  EXPECT_EQ(err.column, 3); // 1-indexed
  EXPECT_NE(err.message.find("INT32"), std::string::npos);
  EXPECT_NE(err.message.find("age"), std::string::npos);
  EXPECT_NE(err.context.find("abc"), std::string::npos);
}

TEST(TypeCoercionErrorTest, NoErrorWhenCollectorNull) {
  // Default context has null error_collector - should not crash
  auto builder = libvroom::ArrowColumnBuilder::create_int32();
  auto ctx = builder->create_context();

  // This should silently produce NULL, no crash
  ctx.append(std::string_view("abc"));
  EXPECT_EQ(builder->size(), 1);
  EXPECT_EQ(builder->null_count(), 1);
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.*'`
Expected: FAIL — `error_collector` is not a member of FastArrowContext

**Step 3: Add error reporting fields to FastArrowContext**

In `include/libvroom/fast_arrow_context.h`, add these fields after `AppendNullFn append_null_fn;` (line 39) and before the static append implementations section:

```cpp
  // Error reporting (optional - null when error collection is disabled)
  ErrorCollector* error_collector = nullptr;
  size_t* error_row = nullptr;      // Pointer to current row number (caller increments)
  size_t error_col_index = 0;       // 0-indexed column index
  const char* error_col_name = "";  // Column name (points to stable storage)
  DataType error_expected_type = DataType::STRING;
```

Also add `#include "error.h"` and `#include "types.h"` to the includes at the top.

**Step 4: Add report_coercion_error helper method**

Add this inline method after the new fields:

```cpp
  // Report a type coercion error if error collection is enabled
  inline void report_coercion_error(std::string_view actual_value) {
    if (!error_collector)
      return;
    std::string msg = std::string("Cannot convert to ") + type_name(error_expected_type) +
                      " in column '" + error_col_name + "'";
    // Truncate long values in context
    std::string ctx(actual_value.substr(0, 100));
    error_collector->add_error(ErrorCode::TYPE_COERCION, ErrorSeverity::RECOVERABLE,
                               error_row ? *error_row : 0, error_col_index + 1, 0, msg, ctx);
  }
```

**Step 5: Wire up error reporting in each type-specific append**

Modify each append function's failure branch to call `report_coercion_error`. String and null appends do NOT need this (strings never fail, nulls are intentional).

For `append_int32` (lines 97-106), change the else branch:

```cpp
  VROOM_FORCE_INLINE static void append_int32(FastArrowContext& ctx, std::string_view value) {
    int32_t result;
    if (VROOM_LIKELY(simd::parse_int32_simd(value.data(), value.size(), result))) {
      ctx.int32_buffer->push_back(result);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.report_coercion_error(value);
      ctx.int32_buffer->push_back(0);
      ctx.null_bitmap->push_back_null();
    }
  }
```

Apply the same pattern to:
- `append_int64` (lines 154-163)
- `append_float64` (lines 170-180)
- `append_bool` (lines 187-202) — at the final fallthrough (line 200-201)
- `append_date` (lines 209-223) — the parse failure branch (line 220-221)
- `append_timestamp` (lines 230-244) — the parse failure branch (line 241-242)

**Step 6: Build and run tests**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.*'`
Expected: PASS

**Step 7: Commit**

```bash
git add include/libvroom/fast_arrow_context.h test/error_handling_test.cpp
git commit -m "feat: add type coercion error reporting to FastArrowContext"
```

---

### Task 3: Wire up error reporting in csv_reader.cpp serial path

**Files:**
- Modify: `src/reader/csv_reader.cpp:1179-1185` (serial read_all_serial fast_contexts setup)

**Step 1: Write failing integration test**

Add to `test/error_handling_test.cpp`:

```cpp
TEST(TypeCoercionErrorTest, CsvReaderSerialReportsCoercionErrors) {
  // Create a temp CSV file with type errors
  // Column "a" is int, "b" is float - row 3 has "abc" for int, "xyz" for float
  std::string csv = "a,b\n1,1.5\n2,2.5\nabc,xyz\n4,4.5\n";
  auto tmp = write_temp_file(csv);

  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
  opts.num_threads = 1; // Force serial path

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp);
  ASSERT_TRUE(open_result.ok) << open_result.error;

  // Override schema to force int32 and float64 types
  std::vector<libvroom::ColumnSchema> schema = reader.schema();
  schema[0].type = libvroom::DataType::INT32;
  schema[1].type = libvroom::DataType::FLOAT64;
  reader.set_schema(schema);

  auto result = reader.read_all();
  ASSERT_TRUE(result.ok()) << result.error();

  // Should have collected 2 coercion errors (abc for int, xyz for float)
  ASSERT_TRUE(reader.has_errors());
  const auto& errors = reader.errors();

  size_t coercion_count = 0;
  for (const auto& err : errors) {
    if (err.code == libvroom::ErrorCode::TYPE_COERCION) {
      coercion_count++;
      EXPECT_EQ(err.severity, libvroom::ErrorSeverity::RECOVERABLE);
      EXPECT_EQ(err.line, 4); // Row 4 (header=1, data starts at 2)
    }
  }
  EXPECT_EQ(coercion_count, 2);
}
```

Note: If `write_temp_file` doesn't exist, check the test file for the existing helper. If there's no temp file helper, write to `/tmp/test_coercion_XXXXXX` directly.

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.CsvReaderSerialReportsCoercionErrors'`
Expected: FAIL — coercion_count is 0 (contexts don't have error_collector set)

**Step 3: Wire up error metadata in serial path**

In `src/reader/csv_reader.cpp`, in the `read_all_serial()` method, after the fast_contexts are created (around line 1184) and before the parse loop, add code to populate the error fields when error collection is enabled:

```cpp
  // Wire up error reporting on FastArrowContexts when enabled
  if (check_errors) {
    for (size_t i = 0; i < fast_contexts.size(); ++i) {
      fast_contexts[i].error_collector = &impl_->error_collector;
      fast_contexts[i].error_row = &row_number;
      fast_contexts[i].error_col_index = i;
      fast_contexts[i].error_col_name = schema[i].name.c_str();
      fast_contexts[i].error_expected_type = schema[i].type;
    }
  }
```

Make sure `schema` is available — it's already captured earlier in the function as `const std::vector<ColumnSchema> schema = impl_->schema;`.

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.CsvReaderSerialReportsCoercionErrors'`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reader/csv_reader.cpp test/error_handling_test.cpp
git commit -m "feat: wire up type coercion error reporting in serial parsing path"
```

---

### Task 4: Wire up error reporting in multi-threaded path

**Files:**
- Modify: `src/reader/csv_reader.cpp:1063-1089` (multi-threaded lambda)

**Step 1: Write failing test for multi-threaded coercion errors**

Add to `test/error_handling_test.cpp`:

```cpp
TEST(TypeCoercionErrorTest, CsvReaderMultiThreadedReportsCoercionErrors) {
  // Create a CSV file large enough to trigger multi-threaded parsing
  // Need enough rows to exceed the chunk threshold
  std::string csv = "a,b\n";
  for (int i = 0; i < 10000; ++i) {
    if (i == 500 || i == 5000 || i == 9999) {
      csv += "abc,xyz\n"; // Bad rows spread across chunks
    } else {
      csv += std::to_string(i) + "," + std::to_string(i * 1.5) + "\n";
    }
  }
  auto tmp = write_temp_file(csv);

  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
  // Don't force num_threads=1 — let it use default (multi-threaded if large enough)

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp);
  ASSERT_TRUE(open_result.ok) << open_result.error;

  std::vector<libvroom::ColumnSchema> schema = reader.schema();
  schema[0].type = libvroom::DataType::INT32;
  schema[1].type = libvroom::DataType::FLOAT64;
  reader.set_schema(schema);

  auto result = reader.read_all();
  ASSERT_TRUE(result.ok()) << result.error();

  // Should have collected 6 coercion errors (3 rows x 2 columns)
  size_t coercion_count = 0;
  for (const auto& err : reader.errors()) {
    if (err.code == libvroom::ErrorCode::TYPE_COERCION) {
      coercion_count++;
    }
  }
  EXPECT_EQ(coercion_count, 6);
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.CsvReaderMultiThreadedReportsCoercionErrors'`
Expected: FAIL — coercion_count is 0

**Step 3: Wire up error metadata in multi-threaded path**

In `src/reader/csv_reader.cpp`, inside the lambda for each chunk (around line 1063-1089), after the fast_contexts are created from column builders, add error wiring. The key difference from serial is:
- Each chunk uses its own `chunk_error_collector` (from `thread_error_collectors`)
- Row numbers are 0 in multi-threaded path (existing limitation documented at line 178)
- Need a per-chunk mutable row counter variable

Inside the lambda, after the builders and fast_contexts are created, add:

```cpp
        // Wire up error reporting on FastArrowContexts when enabled
        // Note: row numbers are 0 in multi-threaded path (same as other error types)
        size_t chunk_row = 0; // No absolute row tracking in parallel path
        if (chunk_error_collector) {
          std::vector<FastArrowContext> fast_contexts;
          fast_contexts.reserve(result.columns.size());
          for (auto& col : result.columns) {
            fast_contexts.push_back(col->create_context());
          }
          for (size_t i = 0; i < fast_contexts.size(); ++i) {
            fast_contexts[i].error_collector = chunk_error_collector;
            fast_contexts[i].error_row = &chunk_row;
            fast_contexts[i].error_col_index = i;
            fast_contexts[i].error_col_name = schema[i].name.c_str();
            fast_contexts[i].error_expected_type = schema[i].type;
          }
        }
```

Wait — the fast_contexts are created inside `parse_chunk_with_state`. We need to wire them up there instead. Look at `parse_chunk_with_state` (lines 41-264):

The fast_contexts are local to `parse_chunk_with_state` at lines 50-54. The schema is NOT available inside this function. We need to pass the schema information through.

**Better approach:** Add schema info as a parameter to `parse_chunk_with_state`, or wire up the contexts after creation but before the parse loop. Since `parse_chunk_with_state` creates its own fast_contexts internally, we should add a parameter for the schema so it can wire up error reporting.

Modify `parse_chunk_with_state` signature to accept an optional schema parameter:

```cpp
std::pair<size_t, bool> parse_chunk_with_state(
    const char* data, size_t size, const CsvOptions& options, const NullChecker& null_checker,
    std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns, bool start_inside_quote,
    ErrorCollector* error_collector = nullptr, size_t base_byte_offset = 0,
    const std::vector<ColumnSchema>* schema = nullptr)
```

Then inside the function, after fast_contexts creation (line 53) add:

```cpp
  size_t parse_row = 0;
  if (error_collector && schema) {
    for (size_t i = 0; i < fast_contexts.size(); ++i) {
      fast_contexts[i].error_collector = error_collector;
      fast_contexts[i].error_row = &parse_row;
      fast_contexts[i].error_col_index = i;
      fast_contexts[i].error_col_name = (*schema)[i].name.c_str();
      fast_contexts[i].error_expected_type = (*schema)[i].type;
    }
  }
```

And at the call site in the lambda (line 1085), pass `&schema`:

```cpp
        auto [rows, ends_inside] =
            parse_chunk_with_state(chunk_data, chunk_size, options, null_checker, result.columns,
                                   start_inside, chunk_error_collector, start_offset, &schema);
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.CsvReaderMultiThreadedReportsCoercionErrors'`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reader/csv_reader.cpp test/error_handling_test.cpp
git commit -m "feat: wire up type coercion error reporting in multi-threaded parsing path"
```

---

### Task 5: Additional tests for completeness

**Files:**
- Modify: `test/error_handling_test.cpp`

**Step 1: Write tests for all type-specific coercion errors**

Add to `test/error_handling_test.cpp`:

```cpp
TEST(TypeCoercionErrorTest, Float64CoercionErrorReported) {
  auto builder = libvroom::ArrowColumnBuilder::create_float64();
  auto ctx = builder->create_context();

  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row = 3;
  ctx.error_collector = &collector;
  ctx.error_row = &row;
  ctx.error_col_index = 0;
  ctx.error_col_name = "price";
  ctx.error_expected_type = libvroom::DataType::FLOAT64;

  ctx.append(std::string_view("not_a_number"));
  ASSERT_EQ(collector.error_count(), 1);
  EXPECT_EQ(collector.errors()[0].code, libvroom::ErrorCode::TYPE_COERCION);
  EXPECT_NE(collector.errors()[0].message.find("FLOAT64"), std::string::npos);
  EXPECT_NE(collector.errors()[0].context.find("not_a_number"), std::string::npos);
}

TEST(TypeCoercionErrorTest, BoolCoercionErrorReported) {
  auto builder = libvroom::ArrowColumnBuilder::create_bool();
  auto ctx = builder->create_context();

  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row = 1;
  ctx.error_collector = &collector;
  ctx.error_row = &row;
  ctx.error_col_index = 0;
  ctx.error_col_name = "active";
  ctx.error_expected_type = libvroom::DataType::BOOL;

  ctx.append(std::string_view("maybe"));
  ASSERT_EQ(collector.error_count(), 1);
  EXPECT_EQ(collector.errors()[0].code, libvroom::ErrorCode::TYPE_COERCION);
}

TEST(TypeCoercionErrorTest, DateCoercionErrorReported) {
  auto builder = libvroom::ArrowColumnBuilder::create_date();
  auto ctx = builder->create_context();

  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row = 2;
  ctx.error_collector = &collector;
  ctx.error_row = &row;
  ctx.error_col_index = 0;
  ctx.error_col_name = "birthday";
  ctx.error_expected_type = libvroom::DataType::DATE;

  ctx.append(std::string_view("not-a-date"));
  ASSERT_EQ(collector.error_count(), 1);
  EXPECT_EQ(collector.errors()[0].code, libvroom::ErrorCode::TYPE_COERCION);
}

TEST(TypeCoercionErrorTest, TimestampCoercionErrorReported) {
  auto builder = libvroom::ArrowColumnBuilder::create_timestamp();
  auto ctx = builder->create_context();

  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row = 2;
  ctx.error_collector = &collector;
  ctx.error_row = &row;
  ctx.error_col_index = 0;
  ctx.error_col_name = "created_at";
  ctx.error_expected_type = libvroom::DataType::TIMESTAMP;

  ctx.append(std::string_view("yesterday"));
  ASSERT_EQ(collector.error_count(), 1);
  EXPECT_EQ(collector.errors()[0].code, libvroom::ErrorCode::TYPE_COERCION);
}

TEST(TypeCoercionErrorTest, Int64CoercionErrorReported) {
  auto builder = libvroom::ArrowColumnBuilder::create_int64();
  auto ctx = builder->create_context();

  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row = 7;
  ctx.error_collector = &collector;
  ctx.error_row = &row;
  ctx.error_col_index = 1;
  ctx.error_col_name = "big_num";
  ctx.error_expected_type = libvroom::DataType::INT64;

  ctx.append(std::string_view("nope"));
  ASSERT_EQ(collector.error_count(), 1);
  EXPECT_EQ(collector.errors()[0].code, libvroom::ErrorCode::TYPE_COERCION);
  EXPECT_EQ(collector.errors()[0].line, 7);
  EXPECT_EQ(collector.errors()[0].column, 2); // 1-indexed
}

TEST(TypeCoercionErrorTest, NoErrorForValidValues) {
  auto builder = libvroom::ArrowColumnBuilder::create_int32();
  auto ctx = builder->create_context();

  libvroom::ErrorCollector collector(libvroom::ErrorMode::PERMISSIVE);
  size_t row = 1;
  ctx.error_collector = &collector;
  ctx.error_row = &row;
  ctx.error_col_index = 0;
  ctx.error_col_name = "id";
  ctx.error_expected_type = libvroom::DataType::INT32;

  ctx.append(std::string_view("42"));
  ctx.append(std::string_view("-100"));
  ctx.append(std::string_view("0"));

  EXPECT_EQ(collector.error_count(), 0);
  EXPECT_EQ(builder->size(), 3);
  EXPECT_EQ(builder->null_count(), 0);
}

TEST(TypeCoercionErrorTest, DisabledModeNoCoercionErrors) {
  std::string csv = "a\nabc\n";
  auto tmp = write_temp_file(csv);

  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::DISABLED;
  opts.num_threads = 1;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp);
  ASSERT_TRUE(open_result.ok) << open_result.error;

  std::vector<libvroom::ColumnSchema> schema = reader.schema();
  schema[0].type = libvroom::DataType::INT32;
  reader.set_schema(schema);

  auto result = reader.read_all();
  ASSERT_TRUE(result.ok()) << result.error();
  EXPECT_FALSE(reader.has_errors());
}

TEST(TypeCoercionErrorTest, FailFastStopsOnFirstCoercionError) {
  std::string csv = "a\nabc\ndef\n";
  auto tmp = write_temp_file(csv);

  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::FAIL_FAST;
  opts.num_threads = 1;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp);
  ASSERT_TRUE(open_result.ok) << open_result.error;

  std::vector<libvroom::ColumnSchema> schema = reader.schema();
  schema[0].type = libvroom::DataType::INT32;
  reader.set_schema(schema);

  auto result = reader.read_all();
  // May or may not succeed depending on implementation, but should have error
  EXPECT_TRUE(reader.has_errors());
  // Should have at most 1 error (FAIL_FAST stops after first)
  EXPECT_LE(reader.errors().size(), 1);
}

TEST(TypeCoercionErrorTest, MultipleErrorsAcrossColumns) {
  std::string csv = "a,b,c\n1,2.0,true\nabc,xyz,maybe\n3,4.0,false\n";
  auto tmp = write_temp_file(csv);

  libvroom::CsvOptions opts;
  opts.error_mode = libvroom::ErrorMode::PERMISSIVE;
  opts.num_threads = 1;

  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(tmp);
  ASSERT_TRUE(open_result.ok) << open_result.error;

  std::vector<libvroom::ColumnSchema> schema = reader.schema();
  schema[0].type = libvroom::DataType::INT32;
  schema[1].type = libvroom::DataType::FLOAT64;
  schema[2].type = libvroom::DataType::BOOL;
  reader.set_schema(schema);

  auto result = reader.read_all();
  ASSERT_TRUE(result.ok()) << result.error();

  size_t coercion_count = 0;
  bool found_col_a = false, found_col_b = false, found_col_c = false;
  for (const auto& err : reader.errors()) {
    if (err.code == libvroom::ErrorCode::TYPE_COERCION) {
      coercion_count++;
      if (err.message.find("'a'") != std::string::npos) found_col_a = true;
      if (err.message.find("'b'") != std::string::npos) found_col_b = true;
      if (err.message.find("'c'") != std::string::npos) found_col_c = true;
    }
  }
  EXPECT_EQ(coercion_count, 3);
  EXPECT_TRUE(found_col_a);
  EXPECT_TRUE(found_col_b);
  EXPECT_TRUE(found_col_c);
}
```

**Step 2: Check test helpers exist**

The tests above use `write_temp_file()` — check if this exists in the test file. If not, add this helper at the top of the test file:

```cpp
#include <cstdio>
#include <fstream>

static std::string write_temp_file(const std::string& content) {
  char tmp[] = "/tmp/coercion_test_XXXXXX";
  int fd = mkstemp(tmp);
  if (fd < 0) return "";
  close(fd);
  std::ofstream f(tmp);
  f << content;
  f.close();
  return tmp;
}
```

**Step 3: Build and run all tests**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.*'`
Expected: ALL PASS

**Step 4: Run full test suite**

Run: `cd build && ctest --output-on-failure -j$(nproc)`
Expected: ALL PASS (no regressions)

**Step 5: Commit**

```bash
git add test/error_handling_test.cpp
git commit -m "test: add comprehensive type coercion error reporting tests"
```

---

### Task 6: Wire up serial path's should_stop check for FAIL_FAST

**Files:**
- Modify: `src/reader/csv_reader.cpp` (serial parse loop)

**Step 1: Add should_stop check after append in serial path**

In the serial parse loop (`read_all_serial`), after each `fast_contexts[col_idx].append(...)` call (lines 1329, 1332), the ErrorCollector may now have a new error from the type coercion. For FAIL_FAST mode, we need to check `should_stop()`.

Since the append calls are inside the field iteration loop, and the existing check_errors path already has should_stop checks, add a check after the append:

In the serial path, after line 1332 (`fast_contexts[col_idx].append(field_view);`), and after line 1329 (`fast_contexts[col_idx].append(unescaped);`), add:

```cpp
      // Check if type coercion error triggered should_stop
      if (check_errors && error_collector->should_stop()) [[unlikely]]
        goto done_serial;
```

Wait — in the serial path, the error_collector is `impl_->error_collector`, not a local `error_collector` variable. Check the existing code pattern. The existing checks use `impl_->error_collector.should_stop()`.

Actually, looking more carefully at the serial path: the contexts already have `error_collector` pointing to `impl_->error_collector`. After an append that triggers a coercion error, `impl_->error_collector` will have the new error. We just need to check should_stop.

Add after the append calls (both the escaped and non-escaped paths):

After `fast_contexts[col_idx].append(unescaped);` (the escaped path):
```cpp
        if (check_errors && impl_->error_collector.should_stop()) [[unlikely]]
          goto done_serial;
```

After `fast_contexts[col_idx].append(field_view);` (the non-escaped path):
```cpp
        if (check_errors && impl_->error_collector.should_stop()) [[unlikely]]
          goto done_serial;
```

**Step 2: Build and run FAIL_FAST test**

Run: `cmake --build build -j$(nproc) && ./build/error_handling_test --gtest_filter='TypeCoercionErrorTest.FailFastStopsOnFirstCoercionError'`
Expected: PASS

**Step 3: Run full test suite**

Run: `cd build && ctest --output-on-failure -j$(nproc)`
Expected: ALL PASS

**Step 4: Commit**

```bash
git add src/reader/csv_reader.cpp
git commit -m "feat: check should_stop after type coercion errors in serial path"
```

---

### Task 7: Final verification and cleanup

**Step 1: Run full test suite**

Run: `cmake -B build -DCMAKE_BUILD_TYPE=Release && cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`
Expected: ALL PASS

**Step 2: Verify no regressions in existing error handling tests**

Run: `./build/error_handling_test`
Expected: ALL PASS (both old and new tests)

**Step 3: Verify core CSV tests unaffected**

Run: `./build/libvroom_test`
Expected: ALL PASS

**Step 4: Verify DISABLED mode has no overhead**

The error reporting only activates when `error_collector != nullptr`, which only happens when `check_errors` is true (i.e., error mode is not DISABLED). In DISABLED mode, the FastArrowContext fields remain at their defaults (nullptr), and the `report_coercion_error` method returns immediately on the null check. The only added overhead in DISABLED mode is the extra fields in the FastArrowContext struct (40 bytes per column per chunk) which is negligible.

---

## Implementation Notes

### Design Decisions

1. **Adding fields to FastArrowContext rather than changing function pointer signatures**: This preserves the existing `AppendFn` type signature, so all column builders continue to work without changes to their `create_context()` methods. The new fields default to null/zero, so existing code that creates contexts without error reporting continues to work.

2. **Row numbers are 0 in multi-threaded path**: This is consistent with existing error reporting in the multi-threaded path (see comment at csv_reader.cpp:178-180). The byte_offset field is 0 for coercion errors since the byte offset of the field value isn't easily available inside the append function. Row numbers are accurate in the serial path.

3. **ErrorSeverity::RECOVERABLE**: Type coercion failures are recoverable — the value is replaced with NULL and parsing continues. This is consistent with R's readr behavior where parse failures produce NA with a warning.

4. **`const char*` for column name**: Points to the ColumnSchema's name storage which outlives the parsing context. Avoids per-context string allocation.

5. **No changes to streaming parser**: The streaming parser (`streaming_parser.cpp`) creates its own fast_contexts but doesn't currently have error collection wired up. That's a separate concern — the StreamingParser would need its own error reporting infrastructure first.
