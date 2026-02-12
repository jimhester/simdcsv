# R-Compatible Parsing Defaults Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Make libvroom's parsing defaults R/readr-compatible: extended booleans, leading `+` floats, decimal mark option, guess_integer, NullChecker refactor, skip lines, and type inference improvements.

**Architecture:** Each sub-change is self-contained. Options changes in `options.h` flow through `TypeInference` (which reads options directly) and to the fast contexts (which use hardcoded values that must be updated in parallel). For decimal mark, we add a field to `FastArrowContext` and set it from `csv_reader.cpp`.

**Tech Stack:** C++17, fast_float (with `from_chars_advanced` for decimal mark), Google Test

---

### Task 1: Update CsvOptions defaults and add new fields

This task changes defaults and adds new option fields. No new tests yet — we'll fix broken tests in the final task.

**Files:**
- Modify: `include/libvroom/options.h:15-47` (CsvOptions)
- Modify: `include/libvroom/options.h:50-67` (FwfOptions)

**Step 1: Update options.h**

In `CsvOptions`:
```cpp
// Line 22: change default from false to true
bool guess_integer = true;

// Line 24: already correct (trailing comma means empty is null)
// No change needed

// Line 25-26: add T/t and F/f to boolean values
std::string true_values = "true,TRUE,True,T,t,yes,YES,Yes";
std::string false_values = "false,FALSE,False,F,f,no,NO,No";

// After line 23 (trim_ws), add new fields:
char decimal_mark = '.';  // Decimal separator ('.' or ',')
size_t skip = 0;          // Lines to skip before header
```

In `FwfOptions`:
```cpp
// Line 55: change default from false to true
bool guess_integer = true;

// Line 59-60: add T/t and F/f to boolean values
std::string true_values = "true,TRUE,True,T,t,yes,YES,Yes";
std::string false_values = "false,FALSE,False,F,f,no,NO,No";
```

**Step 2: Build to verify compilation**

Run: `cmake --build build -j$(nproc) 2>&1 | tail -5`
Expected: Compiles (tests may fail, that's OK)

**Step 3: Commit**

```bash
git add include/libvroom/options.h
git commit -m "feat: update CsvOptions defaults and add decimal_mark/skip fields"
```

---

### Task 2: NullChecker refactor

**Files:**
- Modify: `include/libvroom/parse_utils.h:48-99`
- Test: `test/type_inference_test.cpp`

**Step 1: Write the failing test**

Add to `test/type_inference_test.cpp` in a new test section after the CustomOptions tests:

```cpp
// ============================================================================
// NullChecker refactor tests
// ============================================================================

class NullCheckerTest : public ::testing::Test {};

TEST_F(NullCheckerTest, EmptyNullValuesNothingIsNull) {
  // When null_values is empty, nothing should be considered null
  libvroom::NullChecker checker(std::string_view(""));
  EXPECT_FALSE(checker.is_null(""));
  EXPECT_FALSE(checker.is_null("NA"));
  EXPECT_FALSE(checker.is_null("anything"));
}

TEST_F(NullCheckerTest, EmptyStringNotNullByDefault) {
  // empty_is_null_ should default to false
  // Only explicit trailing comma enables empty-is-null
  libvroom::NullChecker checker(std::string_view("NA"));
  EXPECT_FALSE(checker.is_null(""));
  EXPECT_TRUE(checker.is_null("NA"));
}

TEST_F(NullCheckerTest, TrailingCommaEnablesEmptyIsNull) {
  // Trailing comma means empty string is null
  libvroom::NullChecker checker(std::string_view("NA,"));
  EXPECT_TRUE(checker.is_null(""));
  EXPECT_TRUE(checker.is_null("NA"));
  EXPECT_FALSE(checker.is_null("other"));
}

TEST_F(NullCheckerTest, DeferredInitialization) {
  // Test the public init() method for deferred initialization
  libvroom::NullChecker checker(std::string_view(""));
  EXPECT_FALSE(checker.is_null("NA"));

  checker.init("NA,NULL,");
  EXPECT_TRUE(checker.is_null("NA"));
  EXPECT_TRUE(checker.is_null("NULL"));
  EXPECT_TRUE(checker.is_null(""));
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: FAIL — `init()` is private, `empty_is_null_` defaults to `true`

**Step 3: Implement NullChecker changes in parse_utils.h**

Change `init()` from `private` to `public` and change `empty_is_null_` default:

```cpp
class NullChecker {
public:
  explicit NullChecker(const CsvOptions& options) { init(options.null_values); }
  explicit NullChecker(const FwfOptions& options) { init(options.null_values); }
  explicit NullChecker(std::string_view null_values_csv) { init(null_values_csv); }

  bool is_null(std::string_view value) const {
    // Fast path: empty string check
    if (value.empty()) {
      return empty_is_null_;
    }

    // Fast path: length check
    if (value.size() > max_null_length_) {
      return false;
    }

    for (const auto& nv : null_values_) {
      if (nv == value) {
        return true;
      }
    }
    return false;
  }

  // Public init for deferred initialization
  void init(std::string_view null_values) {
    null_values_.clear();
    max_null_length_ = 0;
    empty_is_null_ = false;  // Reset to default

    size_t start = 0;
    while (start < null_values.size()) {
      size_t end = null_values.find(',', start);
      if (end == std::string_view::npos) {
        end = null_values.size();
      }

      std::string_view null_val = null_values.substr(start, end - start);
      if (!null_val.empty()) {
        null_values_.emplace_back(null_val);
        max_null_length_ = std::max(max_null_length_, null_val.size());
      } else {
        empty_is_null_ = true;
      }

      start = end + 1;
    }
  }

private:
  std::vector<std::string> null_values_;
  size_t max_null_length_ = 0;
  bool empty_is_null_ = false;  // Changed from true to false
};
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: New NullChecker tests PASS

**Step 5: Commit**

```bash
git add include/libvroom/parse_utils.h test/type_inference_test.cpp
git commit -m "feat: refactor NullChecker with public init() and empty_is_null default false"
```

---

### Task 3: Leading `+` in floats

**Files:**
- Modify: `include/libvroom/fast_arrow_context.h:170-180` (append_float64)
- Modify: `include/libvroom/fast_column_context.h:175-185` (append_float64)
- Modify: `src/columns/column_builder.cpp:343-358` (Float64ColumnBuilder::append)
- Modify: `src/schema/type_inference.cpp:98-103` (float inference)
- Test: `test/type_inference_test.cpp`

**Step 1: Write the failing test**

Add to `test/type_inference_test.cpp`:

```cpp
// ============================================================================
// Leading + in floats
// ============================================================================

class LeadingPlusFloatTest : public ::testing::Test {};

TEST_F(LeadingPlusFloatTest, LeadingPlusInfersAsFloat) {
  CsvOptions opts;
  opts.separator = ',';
  TypeInference inference(opts);
  EXPECT_EQ(inference.infer_field("+1.5"), DataType::FLOAT64);
  EXPECT_EQ(inference.infer_field("+0.0"), DataType::FLOAT64);
  EXPECT_EQ(inference.infer_field("+3.14e2"), DataType::FLOAT64);
}

TEST_F(LeadingPlusFloatTest, LeadingPlusEndToEnd) {
  test_util::TempCsvFile csv("val\n+1.5\n+2.5\n+3.5\n");

  CsvOptions opts;
  CsvReader reader(opts);
  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 1u);
  EXPECT_EQ(schema[0].type, DataType::FLOAT64);
  // Verify values parsed correctly (not null)
  EXPECT_EQ(read_result.value.total_rows, 3u);
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: FAIL — `+1.5` currently infers as STRING (fast_float rejects leading `+`)

**Step 3: Implement leading `+` stripping**

In `src/schema/type_inference.cpp`, modify the float parsing section (around line 98-103):

```cpp
  // Try to parse as float
  double result;
  const char* float_start = value.data();
  size_t float_len = value.size();
  // Strip leading '+' that fast_float doesn't accept
  if (float_len > 0 && *float_start == '+') {
    float_start++;
    float_len--;
  }
  auto [ptr, ec] = fast_float::from_chars(float_start, float_start + float_len, result);
  if (ec == std::errc() && ptr == float_start + float_len) {
    return DataType::FLOAT64;
  }
```

In `include/libvroom/fast_arrow_context.h`, modify `append_float64`:

```cpp
  static void append_float64(FastArrowContext& ctx, std::string_view value) {
    double result;
    const char* start = value.data();
    size_t len = value.size();
    // Strip leading '+' that fast_float doesn't accept
    if (len > 0 && *start == '+') {
      start++;
      len--;
    }
    auto [ptr, ec] = fast_float::from_chars(start, start + len, result);
    if (ec == std::errc() && ptr == start + len) {
      ctx.float64_buffer->push_back(result);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.float64_buffer->push_back(std::numeric_limits<double>::quiet_NaN());
      ctx.null_bitmap->push_back_null();
    }
  }
```

Apply the same pattern to `include/libvroom/fast_column_context.h` `append_float64` and `src/columns/column_builder.cpp` `Float64ColumnBuilder::append`.

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: PASS

**Step 5: Commit**

```bash
git add include/libvroom/fast_arrow_context.h include/libvroom/fast_column_context.h \
        src/columns/column_builder.cpp src/schema/type_inference.cpp test/type_inference_test.cpp
git commit -m "feat: handle leading + in float parsing across all paths"
```

---

### Task 4: Extended boolean values

**Files:**
- Modify: `include/libvroom/fast_arrow_context.h:187-202` (append_bool)
- Modify: `include/libvroom/fast_column_context.h:193-211` (append_bool)
- Modify: `src/columns/column_builder.cpp:547-577` (BoolColumnBuilder::append)
- Test: `test/type_inference_test.cpp`

**Step 1: Write the failing test**

Add to `test/type_inference_test.cpp`:

```cpp
// ============================================================================
// Extended boolean values
// ============================================================================

class ExtendedBoolTest : public ::testing::Test {};

TEST_F(ExtendedBoolTest, SingleLetterBooleans) {
  CsvOptions opts;
  opts.separator = ',';
  TypeInference inference(opts);
  EXPECT_EQ(inference.infer_field("T"), DataType::BOOL);
  EXPECT_EQ(inference.infer_field("t"), DataType::BOOL);
  EXPECT_EQ(inference.infer_field("F"), DataType::BOOL);
  EXPECT_EQ(inference.infer_field("f"), DataType::BOOL);
}

TEST_F(ExtendedBoolTest, EndToEndSingleLetterBools) {
  test_util::TempCsvFile csv("flag\nT\nF\nt\nf\n");

  CsvReader reader(CsvOptions{});
  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 1u);
  EXPECT_EQ(schema[0].type, DataType::BOOL);
  EXPECT_EQ(read_result.value.total_rows, 4u);
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: FAIL — "T" infers as STRING, not BOOL (not in true_values defaults yet... actually after Task 1 it IS in the defaults, so type inference will pass. But the end-to-end test will fail because the fast context hardcoded booleans don't include T/t/F/f)

**Step 3: Add T/t/F/f to hardcoded boolean parsers**

In `include/libvroom/fast_arrow_context.h` `append_bool`:

```cpp
  static void append_bool(FastArrowContext& ctx, std::string_view value) {
    if (value == "true" || value == "TRUE" || value == "True" || value == "1" || value == "yes" ||
        value == "YES" || value == "Yes" || value == "T" || value == "t") {
      ctx.bool_buffer->push_back(1);
      ctx.null_bitmap->push_back_valid();
      return;
    }
    if (value == "false" || value == "FALSE" || value == "False" || value == "0" || value == "no" ||
        value == "NO" || value == "No" || value == "F" || value == "f") {
      ctx.bool_buffer->push_back(0);
      ctx.null_bitmap->push_back_valid();
      return;
    }
    ctx.bool_buffer->push_back(0);
    ctx.null_bitmap->push_back_null();
  }
```

Apply the same pattern to `include/libvroom/fast_column_context.h` `append_bool` and `src/columns/column_builder.cpp` `BoolColumnBuilder::append`.

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: PASS

**Step 5: Commit**

```bash
git add include/libvroom/fast_arrow_context.h include/libvroom/fast_column_context.h \
        src/columns/column_builder.cpp test/type_inference_test.cpp
git commit -m "feat: add T/t/F/f to extended boolean value parsing"
```

---

### Task 5: Decimal mark option

**Files:**
- Modify: `include/libvroom/fast_arrow_context.h` (add decimal_mark field, use in append_float64)
- Modify: `src/schema/type_inference.cpp` (use decimal_mark in float inference)
- Modify: `src/reader/csv_reader.cpp` (thread decimal_mark to contexts)
- Test: `test/type_inference_test.cpp`

**Step 1: Write the failing test**

Add to `test/type_inference_test.cpp`:

```cpp
// ============================================================================
// Decimal mark option
// ============================================================================

class DecimalMarkTest : public ::testing::Test {};

TEST_F(DecimalMarkTest, CommaDecimalMark) {
  CsvOptions opts;
  opts.separator = ';';
  opts.decimal_mark = ',';
  TypeInference inference(opts);
  EXPECT_EQ(inference.infer_field("1,5"), DataType::FLOAT64);
  EXPECT_EQ(inference.infer_field("3,14"), DataType::FLOAT64);
  EXPECT_EQ(inference.infer_field("-2,7"), DataType::FLOAT64);
}

TEST_F(DecimalMarkTest, CommaDecimalMarkEndToEnd) {
  test_util::TempCsvFile csv("val\n1,5\n2,7\n3,14\n", ".csv");

  CsvOptions opts;
  opts.separator = '\n';  // Use newline trick: one column, values on separate lines
  opts.decimal_mark = ',';
  // Actually, with comma decimal mark, separator should be semicolon
  // Let's use a proper semicolon-separated file
  test_util::TempCsvFile csv2("val\n1,5\n2,7\n3,14\n", ".csv");
  CsvOptions opts2;
  opts2.separator = ';';  // Will be auto-detected wrongly; force it
  opts2.decimal_mark = ',';
  opts2.has_header = false;
  // Single column, newline separated, comma as decimal
  std::string data = "1,5\n2,7\n3,14\n";
  // Actually let's use a simpler approach with semicolon separator
  test_util::TempCsvFile csv3("x;y\n1,5;hello\n2,7;world\n", ".csv");
  CsvOptions opts3;
  opts3.separator = ';';
  opts3.decimal_mark = ',';
  CsvReader reader(opts3);
  auto open_result = reader.open(csv3.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 2u);
  EXPECT_EQ(schema[0].type, DataType::FLOAT64);
  EXPECT_EQ(schema[1].type, DataType::STRING);
}
```

Actually, that test is messy. Let me clean it up:

```cpp
// ============================================================================
// Decimal mark option
// ============================================================================

class DecimalMarkTest : public ::testing::Test {};

TEST_F(DecimalMarkTest, CommaDecimalMarkInference) {
  CsvOptions opts;
  opts.separator = ';';
  opts.decimal_mark = ',';
  TypeInference inference(opts);
  EXPECT_EQ(inference.infer_field("1,5"), DataType::FLOAT64);
  EXPECT_EQ(inference.infer_field("3,14"), DataType::FLOAT64);
  EXPECT_EQ(inference.infer_field("-2,7"), DataType::FLOAT64);
}

TEST_F(DecimalMarkTest, CommaDecimalMarkEndToEnd) {
  test_util::TempCsvFile csv("x;y\n1,5;hello\n2,7;world\n");

  CsvOptions opts;
  opts.separator = ';';
  opts.decimal_mark = ',';
  CsvReader reader(opts);
  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 2u);
  EXPECT_EQ(schema[0].type, DataType::FLOAT64);
  EXPECT_EQ(schema[1].type, DataType::STRING);
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: FAIL — "1,5" with comma decimal mark infers as STRING

**Step 3: Implement decimal mark support**

**3a. Add decimal_mark field to FastArrowContext** (`include/libvroom/fast_arrow_context.h`):

After the function pointer members (around line 39), add:

```cpp
  char decimal_mark = '.';  // Decimal separator for float parsing
```

Update `append_float64` to use it:

```cpp
  static void append_float64(FastArrowContext& ctx, std::string_view value) {
    double result;
    const char* start = value.data();
    size_t len = value.size();
    // Strip leading '+' that fast_float doesn't accept
    if (len > 0 && *start == '+') {
      start++;
      len--;
    }
    fast_float::parse_options opts{fast_float::chars_format::general, ctx.decimal_mark};
    auto [ptr, ec] = fast_float::from_chars_advanced(start, start + len, result, opts);
    if (ec == std::errc() && ptr == start + len) {
      ctx.float64_buffer->push_back(result);
      ctx.null_bitmap->push_back_valid();
    } else {
      ctx.float64_buffer->push_back(std::numeric_limits<double>::quiet_NaN());
      ctx.null_bitmap->push_back_null();
    }
  }
```

**3b. Thread decimal_mark in csv_reader.cpp**

In `parse_chunk_with_state()` (around line 50-54), after creating fast_contexts:

```cpp
  for (auto& fc : fast_contexts) {
    fc.decimal_mark = options.decimal_mark;
  }
```

In `read_all_serial()` (around line 1181-1185), after creating fast_contexts:

```cpp
  for (auto& fc : fast_contexts) {
    fc.decimal_mark = options.decimal_mark;
  }
```

**3c. Thread decimal_mark in type_inference.cpp** (the float inference section):

```cpp
  // Try to parse as float
  double result;
  const char* float_start = value.data();
  size_t float_len = value.size();
  if (float_len > 0 && *float_start == '+') {
    float_start++;
    float_len--;
  }
  fast_float::parse_options ff_opts{fast_float::chars_format::general, options_.decimal_mark};
  auto [ptr, ec] = fast_float::from_chars_advanced(float_start, float_start + float_len, result, ff_opts);
  if (ec == std::errc() && ptr == float_start + float_len) {
    return DataType::FLOAT64;
  }
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: PASS

**Step 5: Commit**

```bash
git add include/libvroom/fast_arrow_context.h src/schema/type_inference.cpp \
        src/reader/csv_reader.cpp test/type_inference_test.cpp
git commit -m "feat: add decimal_mark option for locale-aware float parsing"
```

---

### Task 6: skip option

**Files:**
- Modify: `src/reader/csv_reader.cpp` (skip N lines in open() and open_from_buffer())
- Test: `test/csv_reader_test.cpp`

**Step 1: Write the failing test**

Add to `test/csv_reader_test.cpp`:

```cpp
// ============================================================================
// skip option
// ============================================================================

class SkipLinesTest : public ::testing::Test {};

TEST_F(SkipLinesTest, SkipZeroLines) {
  test_util::TempCsvFile csv("name,val\nalice,1\nbob,2\n");

  libvroom::CsvOptions opts;
  opts.skip = 0;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 2u);
  EXPECT_EQ(schema[0].name, "name");
}

TEST_F(SkipLinesTest, SkipTwoLines) {
  // First two lines are junk, third line is header
  test_util::TempCsvFile csv("junk line 1\njunk line 2\nname,val\nalice,1\nbob,2\n");

  libvroom::CsvOptions opts;
  opts.skip = 2;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 2u);
  EXPECT_EQ(schema[0].name, "name");
  EXPECT_EQ(schema[1].name, "val");

  auto read_result = reader.read_all();
  ASSERT_TRUE(read_result.ok) << read_result.error;
  EXPECT_EQ(read_result.value.total_rows, 2u);
}

TEST_F(SkipLinesTest, SkipWithCRLF) {
  test_util::TempCsvFile csv("skip me\r\nname,val\r\nalice,1\r\n");

  libvroom::CsvOptions opts;
  opts.skip = 1;
  libvroom::CsvReader reader(opts);
  auto open_result = reader.open(csv.path());
  ASSERT_TRUE(open_result.ok) << open_result.error;

  const auto& schema = reader.schema();
  ASSERT_EQ(schema.size(), 2u);
  EXPECT_EQ(schema[0].name, "name");
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R csv_reader_test -j$(nproc)`
Expected: FAIL — skip=2 doesn't skip lines (field doesn't exist yet in CsvOptions... actually it does after Task 1, but csv_reader.cpp doesn't use it)

**Step 3: Implement skip in csv_reader.cpp**

Add a helper function (near the existing `skip_leading_comment_lines`):

```cpp
// Skip N lines unconditionally. Returns offset past the skipped lines.
static size_t skip_n_lines(const char* data, size_t size, size_t n) {
  size_t offset = 0;
  size_t lines_skipped = 0;
  while (offset < size && lines_skipped < n) {
    // Find end of this line
    while (offset < size && data[offset] != '\n' && data[offset] != '\r') {
      offset++;
    }
    // Skip past the line ending
    if (offset < size && data[offset] == '\r') {
      offset++;
      if (offset < size && data[offset] == '\n') {
        offset++; // CRLF
      }
    } else if (offset < size && data[offset] == '\n') {
      offset++;
    }
    lines_skipped++;
  }
  return offset;
}
```

In `CsvReader::open()`, after encoding detection and before dialect detection (around line 426), add:

```cpp
  // Skip N lines before processing (before comment skip and header)
  if (impl_->options.skip > 0) {
    size_t skip_offset = skip_n_lines(impl_->data_ptr, impl_->data_size, impl_->options.skip);
    impl_->data_ptr += skip_offset;
    impl_->data_size -= skip_offset;
    if (impl_->data_size == 0) {
      return Result<bool>::failure("All data skipped");
    }
  }
```

Apply the same in `CsvReader::open_from_buffer()` at the same relative position (after encoding, before dialect detection).

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R csv_reader_test -j$(nproc)`
Expected: PASS

**Step 5: Commit**

```bash
git add src/reader/csv_reader.cpp test/csv_reader_test.cpp
git commit -m "feat: add skip option to skip N lines before header"
```

---

### Task 7: Type inference sampling improvements

**Files:**
- Modify: `src/schema/type_inference.cpp:160-167` (skip comments/empty in infer_from_sample)
- Test: `test/type_inference_test.cpp`

**Step 1: Write the failing test**

Add to `test/type_inference_test.cpp`:

```cpp
// ============================================================================
// Type inference sampling improvements
// ============================================================================

class InferenceSamplingTest : public ::testing::Test {};

TEST_F(InferenceSamplingTest, SkipCommentLinesDuringSampling) {
  CsvOptions opts;
  opts.separator = ',';
  opts.comment = '#';
  opts.guess_integer = true;
  TypeInference inference(opts);

  // Data with comment lines interspersed
  std::string data = "1,hello\n# this is a comment\n2,world\n";
  auto types = inference.infer_from_sample(data.data(), data.size(), 2);
  EXPECT_EQ(types.size(), 2u);
  EXPECT_EQ(types[0], DataType::INT32);
  EXPECT_EQ(types[1], DataType::STRING);
}

TEST_F(InferenceSamplingTest, SkipEmptyLinesDuringSampling) {
  CsvOptions opts;
  opts.separator = ',';
  opts.guess_integer = true;
  TypeInference inference(opts);

  // Data with empty lines interspersed
  std::string data = "1,hello\n\n\n2,world\n";
  auto types = inference.infer_from_sample(data.data(), data.size(), 2);
  EXPECT_EQ(types.size(), 2u);
  EXPECT_EQ(types[0], DataType::INT32);
  EXPECT_EQ(types[1], DataType::STRING);
}
```

**Step 2: Run test to verify it fails**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: FAIL — comment lines are not skipped during sampling (the `#` gets parsed as a field value)

**Step 3: Implement comment/empty line skipping in infer_from_sample**

In `src/schema/type_inference.cpp`, in the `infer_from_sample` while loop (around line 160), add comment skipping after the row_size==0 check:

```cpp
  while (offset < size && rows_sampled < max_rows) {
    size_t row_end = finder.find_row_end(data, size, offset);
    size_t row_size = row_end - offset;

    if (row_size == 0) {
      ++offset;
      continue;
    }

    // Skip empty lines (just whitespace or newlines)
    {
      bool is_empty = true;
      for (size_t i = offset; i < row_end; ++i) {
        char c = data[i];
        if (c != '\n' && c != '\r' && c != ' ' && c != '\t') {
          is_empty = false;
          break;
        }
      }
      if (is_empty) {
        offset = row_end;
        continue;
      }
    }

    // Skip comment lines
    if (options_.comment != '\0' && data[offset] == options_.comment) {
      offset = row_end;
      continue;
    }

    // ... rest of parsing logic unchanged
```

**Step 4: Run test to verify it passes**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -R type_inference_test -j$(nproc)`
Expected: PASS

**Step 5: Commit**

```bash
git add src/schema/type_inference.cpp test/type_inference_test.cpp
git commit -m "feat: skip comment and empty lines during type inference sampling"
```

---

### Task 8: Fix existing tests for new defaults

The `guess_integer` default changed from `false` to `true`, and `empty_is_null_` default changed from `true` to `false`. Several existing tests rely on the old defaults and must be updated.

**Files:**
- Modify: `test/type_inference_test.cpp` (fix tests using old defaults)
- Possibly: other test files that use default CsvOptions

**Step 1: Run all tests and identify failures**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`
Identify all failing tests.

**Step 2: Fix each failing test**

Key tests to update:

1. `GuessIntegerTest::DefaultGuessIntegerFalse_IntegersInferAsFloat` (line 574): Rename to `GuessIntegerFalse_IntegersInferAsFloat` and add `opts.guess_integer = false` explicitly.

2. `GuessIntegerTest::DefaultGuessIntegerFalse_FloatsStillFloat` (line 586): Same — add `opts.guess_integer = false` explicitly.

3. `GuessIntegerTest::DefaultGuessIntegerFalse_OtherTypesUnaffected` (line 594): Same — add `opts.guess_integer = false` explicitly.

4. `GuessIntegerTest::InferFromSample_DefaultNoIntegers` (line 617): Change to expect INT32 for the first column (since default is now `true`), or add `opts.guess_integer = false` explicitly.

5. `BooleanCSVSchemaType` (line 724): Uses `CsvOptions{}` with "true"/"false" — these are still in the default true_values/false_values, so this test should still pass.

6. Any test that relies on `NullChecker` treating empty strings as null by default — update to use explicit trailing comma in null_values.

**Step 3: Run all tests to verify**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`
Expected: ALL PASS

**Step 4: Commit**

```bash
git add test/type_inference_test.cpp
# Add any other modified test files
git commit -m "test: update tests for new R-compatible defaults"
```

---

### Task 9: Update API surfaces (C API, Python bindings, CLI)

**Files:**
- Check: `include/libvroom_c.h` and `src/libvroom_c.cpp` (C API)
- Check: `python/src/bindings.cpp` (Python bindings)
- Check: `src/cli.cpp` (CLI)

**Step 1: Check if C API needs updates**

Check if the C API exposes CsvOptions fields that need updating for new defaults. If so, update default parameter values.

**Step 2: Check if Python bindings need updates**

Check if Python bindings expose guess_integer, decimal_mark, skip, or boolean values. If so, update default parameter values.

**Step 3: Check if CLI needs updates**

Check if CLI has flags for the new options. If not, consider adding `--decimal-mark`, `--skip`, `--guess-integer` flags.

**Step 4: Run full test suite**

Run: `cmake --build build -j$(nproc) && cd build && ctest --output-on-failure -j$(nproc)`
Expected: ALL PASS

**Step 5: Commit**

```bash
git add include/libvroom_c.h src/libvroom_c.cpp python/src/bindings.cpp src/cli.cpp
git commit -m "feat: expose new R-compatible options in C API, Python, and CLI"
```

---

### Task 10: Final verification and cleanup

**Step 1: Run full build + all tests**

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release
cmake --build build -j$(nproc)
cd build && ctest --output-on-failure -j$(nproc)
```

**Step 2: Run clang-format on all modified files**

```bash
clang-format -i include/libvroom/options.h include/libvroom/parse_utils.h \
  include/libvroom/fast_arrow_context.h include/libvroom/fast_column_context.h \
  src/columns/column_builder.cpp src/schema/type_inference.cpp src/reader/csv_reader.cpp
```

**Step 3: Final commit if formatting changed**

```bash
git add -A
git commit -m "style: format code"
```
