# Backslash Escape Mode Implementation Plan

> **For Claude:** REQUIRED SUB-SKILL: Use superpowers:executing-plans to implement this plan task-by-task.

**Goal:** Add backslash escape mode (`escape_backslash = true`) so libvroom can parse CSV files that use `\"` instead of `""` for embedded quotes, plus `\\`, `\n`, `\t`, `\r` escape sequences.

**Architecture:** Thread a `bool escape_backslash` option through all parsing paths. In SIMD paths, compute an "escaped character" bitmask using simdjson's subtraction-based technique (5 operations per 64-byte block, 1 bit of cross-block state). Remove escaped quotes from quote parity computation. In post-parse, apply backslash unescaping instead of doubled-quote unescaping.

**Tech Stack:** C++17, Google Highway SIMD, Google Test

---

## Task 1: Add `escape_backslash` to CsvOptions

Replace the unused `char escape = '\\'` with `bool escape_backslash = false`.

**Files:**
- Modify: `include/libvroom/options.h:18`

**Step 1: Replace the option**

In `include/libvroom/options.h`, change line 18 from:
```cpp
  char escape = '\\';
```
to:
```cpp
  bool escape_backslash = false; // Use backslash escaping (\") instead of doubled quotes ("")
```

**Step 2: Build to check for compile errors**

```bash
cmake -B build -DCMAKE_BUILD_TYPE=Release -DBUILD_TESTING=OFF -DBUILD_BENCHMARKS=OFF 2>&1 | tail -5
cmake --build build -j$(nproc) 2>&1 | tail -20
```

Expected: Clean build (the old `escape` field was unused).

**Step 3: Commit**

```bash
git add include/libvroom/options.h
git commit -m "feat: add escape_backslash option to CsvOptions (replaces unused char escape)"
```

---

## Task 2: Implement `unescape_backslash()` utility with tests

**Files:**
- Modify: `include/libvroom/parse_utils.h` (add function after `unescape_quotes`)
- Modify: `test/csv_reader_test.cpp` (add unit tests)

**Step 1: Write the failing tests**

Add at the bottom of `test/csv_reader_test.cpp` (before the closing namespace/end):

```cpp
// ============================================================================
// BACKSLASH ESCAPE UTILITY
// ============================================================================

TEST_F(CsvReaderTest, UnescapeBackslash_EscapedQuote) {
  std::string result = libvroom::unescape_backslash(R"(he said \"hello\")", '"');
  EXPECT_EQ(result, "he said \"hello\"");
}

TEST_F(CsvReaderTest, UnescapeBackslash_EscapedBackslash) {
  std::string result = libvroom::unescape_backslash(R"(C:\\Users\\jane)", '"');
  EXPECT_EQ(result, "C:\\Users\\jane");
}

TEST_F(CsvReaderTest, UnescapeBackslash_EscapedTab) {
  std::string result = libvroom::unescape_backslash(R"(Tab:\there)", '"');
  EXPECT_EQ(result, "Tab:\there");
}

TEST_F(CsvReaderTest, UnescapeBackslash_EscapedNewline) {
  std::string result = libvroom::unescape_backslash(R"(line1\nline2)", '"');
  EXPECT_EQ(result, "line1\nline2");
}

TEST_F(CsvReaderTest, UnescapeBackslash_EscapedCR) {
  std::string result = libvroom::unescape_backslash(R"(before\rafter)", '"');
  EXPECT_EQ(result, "before\rafter");
}

TEST_F(CsvReaderTest, UnescapeBackslash_NoEscapes) {
  std::string result = libvroom::unescape_backslash("plain text", '"');
  EXPECT_EQ(result, "plain text");
}

TEST_F(CsvReaderTest, UnescapeBackslash_EmptyString) {
  std::string result = libvroom::unescape_backslash("", '"');
  EXPECT_EQ(result, "");
}

TEST_F(CsvReaderTest, UnescapeBackslash_UnknownEscape) {
  // Unknown escape sequences: backslash is dropped, character kept
  std::string result = libvroom::unescape_backslash(R"(\x)", '"');
  EXPECT_EQ(result, "x");
}

TEST_F(CsvReaderTest, UnescapeBackslash_TrailingBackslash) {
  // Trailing backslash with no following char: kept as-is
  std::string result = libvroom::unescape_backslash("trail\\", '"');
  EXPECT_EQ(result, "trail\\");
}

TEST_F(CsvReaderTest, UnescapeBackslash_MultipleEscapes) {
  std::string result = libvroom::unescape_backslash(R"(\"\\\")", '"');
  EXPECT_EQ(result, "\"\\\"");
}
```

**Step 2: Run tests to verify they fail**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
```

Expected: FAIL - `unescape_backslash` not declared.

**Step 3: Implement unescape_backslash**

Add to `include/libvroom/parse_utils.h` after the `unescape_quotes` function (after line 43):

```cpp
// Unescape backslash escape sequences in a field value.
// Handles: \" → quote, \\ → \, \n → newline, \t → tab, \r → CR
// Unknown sequences: \x → x (backslash dropped)
// Trailing backslash: kept as-is
inline std::string unescape_backslash(std::string_view value, char quote) {
  // Fast path: no backslash
  if (value.find('\\') == std::string_view::npos) {
    return std::string(value);
  }

  std::string result;
  result.reserve(value.size());

  for (size_t i = 0; i < value.size(); ++i) {
    if (value[i] == '\\' && i + 1 < value.size()) {
      char next = value[i + 1];
      switch (next) {
      case '\\': result += '\\'; break;
      case 'n':  result += '\n'; break;
      case 't':  result += '\t'; break;
      case 'r':  result += '\r'; break;
      default:
        if (next == quote) {
          result += quote;
        } else {
          // Unknown escape: drop backslash, keep character
          result += next;
        }
        break;
      }
      ++i; // Skip the escaped character
    } else {
      result += value[i]; // Regular char or trailing backslash
    }
  }

  return result;
}
```

**Step 4: Build and run tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -R CsvReaderTest -j$(nproc)
```

Expected: All UnescapeBackslash tests PASS.

**Step 5: Commit**

```bash
git add include/libvroom/parse_utils.h test/csv_reader_test.cpp
git commit -m "feat: add unescape_backslash() utility for backslash escape sequences"
```

---

## Task 3: Implement `compute_escaped_mask()` with tests

The simdjson subtraction-based technique: 5 operations per 64-byte block, 1 bit of cross-block state.

**Files:**
- Create: `include/libvroom/escape_mask.h`
- Modify: `test/simd_parsing_test.cpp` (add unit tests)

**Step 1: Write the failing tests**

Add to `test/simd_parsing_test.cpp`:

```cpp
#include "libvroom/escape_mask.h"

// ============================================================================
// BACKSLASH ESCAPE MASK COMPUTATION
// ============================================================================

TEST(EscapeMaskTest, SingleBackslashBeforeQuote) {
  // String: a\"b (positions: a=0, \=1, "=2, b=3)
  // Backslash at position 1 escapes quote at position 2
  uint64_t bs_bits = 0b0010; // position 1
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_TRUE(escaped & (1ULL << 2)) << "Position 2 should be escaped";
  EXPECT_FALSE(escaped & (1ULL << 1)) << "Position 1 (backslash itself) should not be escaped";
  EXPECT_EQ(prev_escaped, 0ULL) << "No carry-out expected";
}

TEST(EscapeMaskTest, DoubleBackslashBeforeQuote) {
  // String: a\\"b (positions: a=0, \=1, \=2, "=3, b=4)
  // \\ escapes to literal \, quote at 3 is NOT escaped
  uint64_t bs_bits = 0b0110; // positions 1, 2
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_TRUE(escaped & (1ULL << 2)) << "Position 2 (second \\) should be escaped";
  EXPECT_FALSE(escaped & (1ULL << 3)) << "Position 3 (quote) should NOT be escaped";
  EXPECT_EQ(prev_escaped, 0ULL);
}

TEST(EscapeMaskTest, TripleBackslashBeforeQuote) {
  // String: a\\\"b → positions 1,2,3 are backslashes, 4 is quote
  // \\\ = literal \ then escaped quote → quote at 4 IS escaped
  uint64_t bs_bits = 0b01110; // positions 1, 2, 3
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_TRUE(escaped & (1ULL << 4)) << "Position 4 should be escaped (odd backslashes)";
}

TEST(EscapeMaskTest, QuadBackslashBeforeQuote) {
  // \\\\" = 4 backslashes (even) → quote NOT escaped
  uint64_t bs_bits = 0b011110; // positions 1-4
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_FALSE(escaped & (1ULL << 5)) << "Position 5 should NOT be escaped (even backslashes)";
}

TEST(EscapeMaskTest, NoBackslashes) {
  uint64_t bs_bits = 0;
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_EQ(escaped, 0ULL);
  EXPECT_EQ(escape, 0ULL);
}

TEST(EscapeMaskTest, CrossBlockCarry) {
  // Block ends with a single backslash at position 63
  uint64_t bs_bits = 1ULL << 63;
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_NE(prev_escaped, 0ULL) << "Should carry: backslash at end of block escapes next block's first char";

  // Next block: the first character should be escaped
  uint64_t bs_bits2 = 0;
  auto [escaped2, escape2] = libvroom::compute_escaped_mask(bs_bits2, prev_escaped);
  EXPECT_TRUE(escaped2 & 1ULL) << "Position 0 of next block should be escaped by carry";
}

TEST(EscapeMaskTest, CrossBlockCarryDoubleBackslash) {
  // Block ends with two backslashes at positions 62, 63
  uint64_t bs_bits = (1ULL << 62) | (1ULL << 63);
  uint64_t prev_escaped = 0;
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_EQ(prev_escaped, 0ULL) << "Even backslashes: no carry-out";
}

TEST(EscapeMaskTest, CarryFromPreviousBlock) {
  // Previous block ended with odd backslash (carry-in)
  uint64_t bs_bits = 0; // no backslashes in this block
  uint64_t prev_escaped = 1; // carry from previous block
  auto [escaped, escape] = libvroom::compute_escaped_mask(bs_bits, prev_escaped);
  EXPECT_TRUE(escaped & 1ULL) << "Position 0 should be escaped by carry-in";
}
```

**Step 2: Run to verify failure**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
```

Expected: FAIL - `escape_mask.h` not found.

**Step 3: Implement compute_escaped_mask**

Create `include/libvroom/escape_mask.h`:

```cpp
#pragma once

#include <cstdint>
#include <utility>

namespace libvroom {

// Result of escape mask computation
struct EscapeMaskResult {
  uint64_t escaped; // Bitmask of escaped character positions
  uint64_t escape;  // Bitmask of escape character (backslash) positions that are "active"
};

// Compute which characters in a 64-byte block are escaped by preceding backslashes.
// Uses the simdjson subtraction-based technique (5 ops, no multiplication).
//
// Parameters:
//   bs_bits: bitmask of backslash positions in this 64-byte block
//   prev_escaped: cross-block state (0 or 1). Set to 1 if previous block ended
//                 with an active (odd) backslash. Updated on return.
//
// Returns: { escaped, escape } where:
//   escaped = positions of characters that are escaped (preceded by odd # of backslashes)
//   escape = positions of backslashes that are "active" escape characters
//
// Algorithm (from simdjson): uses subtraction to propagate through backslash
// sequences, with ODD_BITS phase correction to distinguish odd/even runs.
inline EscapeMaskResult compute_escaped_mask(uint64_t bs_bits, uint64_t& prev_escaped) {
  static constexpr uint64_t ODD_BITS = 0xAAAAAAAAAAAAAAAAULL;

  // Fast path: no backslashes and no carry-in
  if (bs_bits == 0 && prev_escaped == 0) {
    return {0, 0};
  }

  // Handle carry from previous block: if prev ended with active backslash,
  // it escapes this block's first character (making it a non-backslash)
  uint64_t potential_escape = bs_bits & ~prev_escaped;

  // Characters that *might* be escaped: those following a potential escape
  uint64_t maybe_escaped = potential_escape << 1;

  // Subtraction-based propagation with ODD_BITS phase correction:
  // - OR with ODD_BITS to set phase markers
  // - Subtract potential_escape to propagate through consecutive backslashes
  // - XOR with ODD_BITS to correct phase
  uint64_t maybe_escaped_and_odd = maybe_escaped | ODD_BITS;
  uint64_t even_series_and_odd = maybe_escaped_and_odd - potential_escape;
  uint64_t escape_and_terminal_code = even_series_and_odd ^ ODD_BITS;

  // escaped: XOR with (backslash | carry-in) to get actual escaped positions
  uint64_t escaped = escape_and_terminal_code ^ (bs_bits | prev_escaped);

  // escape: active backslash positions (those that escape the next character)
  uint64_t escape = escape_and_terminal_code & bs_bits;

  // Update cross-block state: if MSB of escape is set, this block ends
  // with an active backslash that escapes the next block's first character
  prev_escaped = escape >> 63;

  return {escaped, escape};
}

} // namespace libvroom
```

**Step 4: Build and run tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -R EscapeMaskTest -j$(nproc)
```

Expected: All EscapeMaskTest tests PASS.

**Step 5: Commit**

```bash
git add include/libvroom/escape_mask.h test/simd_parsing_test.cpp
git commit -m "feat: add compute_escaped_mask() using simdjson subtraction technique"
```

---

## Task 4: Update scalar row-finding and counting functions

Thread `escape_backslash` through `find_row_end_scalar`, `count_rows_scalar`, `find_row_end_simd`, `count_rows_simd`, and all SIMD internal functions. This task changes **signatures only** for SIMD internals (actual SIMD escape handling is Task 9).

**Files:**
- Modify: `include/libvroom/vroom.h` (public API signatures)
- Modify: `src/parser/simd_chunk_finder.cpp` (internal + public function signatures, scalar fallback implementations)

**Step 1: Update function signatures in vroom.h**

Add `bool escape_backslash = false` parameter to these functions in `include/libvroom/vroom.h`:

```cpp
// Scalar functions
std::pair<size_t, size_t> count_rows_scalar(const char* data, size_t size, char quote_char = '"',
                                            bool escape_backslash = false);
size_t find_row_end_scalar(const char* data, size_t size, size_t start = 0, char quote_char = '"',
                           bool escape_backslash = false);

// SIMD functions
std::pair<size_t, size_t> count_rows_simd(const char* data, size_t size, char quote_char = '"',
                                          bool escape_backslash = false);
std::tuple<size_t, size_t, bool> analyze_chunk_simd(const char* data, size_t size,
                                                    char quote_char = '"',
                                                    bool start_inside_quote = false,
                                                    bool escape_backslash = false);
DualStateChunkStats analyze_chunk_dual_state_simd(const char* data, size_t size,
                                                  char quote_char = '"',
                                                  bool escape_backslash = false);
size_t find_row_end_simd(const char* data, size_t size, size_t start = 0, char quote_char = '"',
                         bool escape_backslash = false);
```

Also update `ChunkFinder`:

```cpp
class ChunkFinder {
public:
  explicit ChunkFinder(char separator = ',', char quote = '"', bool escape_backslash = false);
  // ... (existing methods unchanged)
private:
  char separator_;
  char quote_;
  bool escape_backslash_;
};
```

**Step 2: Update simd_chunk_finder.cpp signatures and scalar fallbacks**

In `src/parser/simd_chunk_finder.cpp`:

1. Add `#include "libvroom/escape_mask.h"` at the top (after existing includes).

2. Update all HWY_NOINLINE function signatures to accept `bool escape_backslash`.

3. Update the scalar fallback sections in each function to handle backslash escapes. In each scalar fallback loop, before toggling quote state on a quote character, check if the quote is escaped:

The scalar pattern changes from:
```cpp
if (c == quote_char) {
  if (in_quote && offset + 1 < size && data[offset + 1] == quote_char) {
    offset += 2; // Skip escaped quote pair
    continue;
  }
  in_quote = !in_quote;
}
```

To (when `escape_backslash` is true):
```cpp
if (c == '\\') {
  offset += 2; // Skip backslash + escaped character
  continue;
}
if (c == quote_char) {
  in_quote = !in_quote;
}
```

When `escape_backslash` is false, keep the existing doubled-quote logic.

4. Update the public API functions (`count_rows_simd`, `analyze_chunk_simd`, etc.) to pass `escape_backslash` through to the HWY_DYNAMIC_DISPATCH calls.

5. Update `count_rows_scalar` and `find_row_end_scalar` with the same scalar backslash handling.

**Step 3: Update chunk_finder.cpp**

In `src/parser/chunk_finder.cpp`, update:
- Constructor to accept and store `escape_backslash_`
- `find_row_end()` to pass `escape_backslash_` to both SIMD and scalar functions
- `count_rows()` to pass `escape_backslash_` through

**Step 4: Build and run all tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -10
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: All existing tests pass (new parameter defaults to false).

**Step 5: Commit**

```bash
git add include/libvroom/vroom.h src/parser/simd_chunk_finder.cpp src/parser/chunk_finder.cpp
git commit -m "feat: thread escape_backslash through row-finding and chunk analysis functions"
```

---

## Task 5: Update SplitFields iterator

**Files:**
- Modify: `include/libvroom/split_fields.h`

**Step 1: Add escape_backslash support to SplitFields**

1. Add `escape_backslash_` member and constructor parameter:

```cpp
VROOM_FORCE_INLINE SplitFields(const char* slice, size_t size, char separator, char quote_char,
                               char eol_char, bool escape_backslash = false)
    : v_(slice), remaining_(size), separator_(separator), finished_(false),
      finished_inside_quote_(false), quote_char_(quote_char), quoting_(quote_char != 0),
      eol_char_(eol_char), escape_backslash_(escape_backslash), previous_valid_ends_(0),
      prev_escaped_(0) {}
```

2. Add member variables:
```cpp
bool escape_backslash_;
uint64_t prev_escaped_; // Cross-block state for escape mask computation
```

3. In `scan_quoted_field()` SIMD loop, when `escape_backslash_` is true:

```cpp
if (escape_backslash_) {
  // Also scan for backslash characters
  uint64_t bs_mask = detail::scan_for_char(bytes, detail::SIMD_SIZE, '\\');
  auto [escaped, escape] = compute_escaped_mask(bs_mask, prev_escaped_);
  // Remove escaped quotes from quote mask
  quote_mask &= ~escaped;
}
```

Include `escape_mask.h` at the top of split_fields.h.

4. In `scan_quoted_field()` scalar fallback, when `escape_backslash_` is true:

```cpp
if (escape_backslash_) {
  for (size_t i = 0; i < len; ++i) {
    char c = bytes[i];
    if (c == '\\' && i + 1 < len) {
      i++; // Skip escaped character
      continue;
    }
    if (c == quote_char_) {
      in_field = !in_field;
    }
    if (!in_field && eof_eol(c)) {
      return total_idx + i;
    }
  }
} else {
  // existing logic
}
```

**Step 2: Build and run tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: All existing tests pass (escape_backslash defaults to false).

**Step 3: Commit**

```bash
git add include/libvroom/split_fields.h
git commit -m "feat: add backslash escape support to SplitFields iterator"
```

---

## Task 6: Update LineParser and TypeInference

**Files:**
- Modify: `src/parser/line_parser.cpp`
- Modify: `src/schema/type_inference.cpp`

**Step 1: Update LineParser::parse_header**

In `src/parser/line_parser.cpp`, in `parse_header()`, change the quote handling:

When `options_.escape_backslash` is true, replace the doubled-quote check with backslash check:

```cpp
if (options_.escape_backslash) {
  if (c == '\\' && i + 1 < size) {
    // Backslash escape: add the escaped character
    char next = data[i + 1];
    switch (next) {
    case '\\': current_field += '\\'; break;
    case 'n':  current_field += '\n'; break;
    case 't':  current_field += '\t'; break;
    case 'r':  current_field += '\r'; break;
    default:
      if (next == options_.quote) {
        current_field += options_.quote;
      } else {
        current_field += next;
      }
      break;
    }
    ++i;
  } else if (c == options_.quote) {
    in_quote = !in_quote;
  } else if (c == options_.separator && !in_quote) {
    // ... existing separator logic
  } else {
    // ... existing else logic
  }
} else {
  // ... existing logic (doubled quote handling)
}
```

Apply the same pattern to `parse_line()`.

**Step 2: Update TypeInference::infer_from_sample**

In `src/schema/type_inference.cpp`, in `infer_from_sample()`, the field extraction loop (lines 174-211) has the same doubled-quote check. Apply the same backslash escape pattern when `options_.escape_backslash` is true.

**Step 3: Build and run tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: All existing tests pass.

**Step 4: Commit**

```bash
git add src/parser/line_parser.cpp src/schema/type_inference.cpp
git commit -m "feat: add backslash escape support to LineParser and TypeInference"
```

---

## Task 7: Update csv_reader.cpp

**Files:**
- Modify: `src/reader/csv_reader.cpp`

**Step 1: Update parse_chunk_with_state**

1. **Quote-seeking in start_inside_quote path** (lines 64-99): When `options.escape_backslash` is true, in the loop that finds the closing quote, handle backslash escapes instead of doubled quotes:

```cpp
if (options.escape_backslash) {
  while (offset < size) {
    char c = data[offset];
    if (c == '\\' && offset + 1 < size) {
      offset += 2; // Skip backslash + escaped char
      continue;
    }
    if (c == options.quote) {
      in_quote = false;
      offset++;
      break;
    }
    offset++;
  }
} else {
  // existing logic
}
```

Apply the same pattern to the "skip to end of partial row" loop.

2. **SplitFields construction** (line 150): Pass `options.escape_backslash`:

```cpp
SplitFields iter(data + offset, start_remaining, sep, quote, '\n', options.escape_backslash);
```

3. **Field unescaping** (lines 213-232): When `needs_escaping` is true, use `unescape_backslash` instead of `unescape_quotes` when in backslash mode:

```cpp
if (options.escape_backslash) {
  std::string unescaped = unescape_backslash(field_view, quote);
  fast_contexts[col_idx].append(unescaped);
} else {
  bool has_invalid_escape = false;
  std::string unescaped =
      unescape_quotes(field_view, quote, check_errors ? &has_invalid_escape : nullptr);
  // ... existing invalid escape error handling ...
  fast_contexts[col_idx].append(unescaped);
}
```

**Step 2: Update read_all_serial**

Apply the same changes:
1. SplitFields construction (line 1246): pass `options.escape_backslash`
2. Field unescaping (lines 1309-1329): use `unescape_backslash` when in backslash mode

**Step 3: Update open() paths**

1. Both `open()` and `open_from_buffer()` construct `ChunkFinder` - pass `escape_backslash`:

```cpp
ChunkFinder finder(impl_->options.separator, impl_->options.quote, impl_->options.escape_backslash);
```

2. In the no-header column counting loops, handle backslash escape:

```cpp
if (impl_->options.escape_backslash) {
  if (c == '\\' && i + 1 < first_row_end) {
    ++i; // Skip escaped character
    continue;
  }
  if (c == impl_->options.quote) {
    in_quote = !in_quote;
  }
} else {
  // existing doubled-quote logic
}
```

**Step 4: Update read_all() parallel path**

1. ChunkFinder construction in read_all() (line 902): pass `escape_backslash`
2. `analyze_chunk_dual_state_simd` call (line 990): pass `escape_backslash`
3. Start_streaming path: same updates

**Step 5: Build and run tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: All existing tests pass.

**Step 6: Commit**

```bash
git add src/reader/csv_reader.cpp
git commit -m "feat: integrate backslash escape mode into CsvReader"
```

---

## Task 8: Write and run integration tests

**Files:**
- Modify: `test/csv_reader_test.cpp`
- Modify: `test/data/escape/backslash_escape.csv` (verify/update test data)

**Step 1: Write integration tests**

Add to `test/csv_reader_test.cpp`:

```cpp
// ============================================================================
// BACKSLASH ESCAPE MODE - INTEGRATION TESTS
// ============================================================================

TEST_F(CsvReaderTest, BackslashEscape_BasicFile) {
  libvroom::CsvOptions opts;
  opts.escape_backslash = true;
  opts.separator = ',';

  auto [chunks, schema] = parseFile(testDataPath("escape/backslash_escape.csv"), opts);
  ASSERT_EQ(schema.size(), 3u);
  EXPECT_EQ(schema[0].name, "Name");
  EXPECT_EQ(schema[1].name, "Description");
  EXPECT_EQ(schema[2].name, "Value");

  ASSERT_EQ(chunks.total_rows, 3u);

  // Row 1: "John \"The Boss\" Smith" → John "The Boss" Smith
  EXPECT_EQ(getStringValue(chunks, 0, 0), "John \"The Boss\" Smith");
  // Row 1: "Says \"Hello\"" → Says "Hello"
  EXPECT_EQ(getStringValue(chunks, 1, 0), "Says \"Hello\"");
  // Row 1: 100
  EXPECT_EQ(getStringValue(chunks, 2, 0), "100");

  // Row 2: "Jane Doe" → Jane Doe
  EXPECT_EQ(getStringValue(chunks, 0, 1), "Jane Doe");
  // Row 2: "Path: C:\\Users\\jane" → Path: C:\Users\jane
  EXPECT_EQ(getStringValue(chunks, 1, 1), "Path: C:\\Users\\jane");

  // Row 3: "Bob's Place" → Bob's Place
  EXPECT_EQ(getStringValue(chunks, 0, 2), "Bob's Place");
  // Row 3: "Tab:\there" → Tab:<TAB>here
  EXPECT_EQ(getStringValue(chunks, 1, 2), "Tab:\there");
}

TEST_F(CsvReaderTest, BackslashEscape_FromString) {
  libvroom::CsvOptions opts;
  opts.escape_backslash = true;
  opts.separator = ',';

  std::string csv = "a,b\n\"he said \\\"hello\\\"\",world\n";
  auto [chunks, schema] = parseContent(csv, opts);
  ASSERT_EQ(chunks.total_rows, 1u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "he said \"hello\"");
  EXPECT_EQ(getStringValue(chunks, 1, 0), "world");
}

TEST_F(CsvReaderTest, BackslashEscape_DoubleBackslash) {
  libvroom::CsvOptions opts;
  opts.escape_backslash = true;
  opts.separator = ',';

  std::string csv = "a\n\"C:\\\\path\\\\to\\\\file\"\n";
  auto [chunks, schema] = parseContent(csv, opts);
  ASSERT_EQ(chunks.total_rows, 1u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "C:\\path\\to\\file");
}

TEST_F(CsvReaderTest, BackslashEscape_EscapedNewline) {
  libvroom::CsvOptions opts;
  opts.escape_backslash = true;
  opts.separator = ',';

  std::string csv = "a\n\"line1\\nline2\"\n";
  auto [chunks, schema] = parseContent(csv, opts);
  ASSERT_EQ(chunks.total_rows, 1u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "line1\nline2");
}

TEST_F(CsvReaderTest, BackslashEscape_DefaultFalseRegression) {
  // escape_backslash=false (default) should behave identically to before
  auto [chunks, schema] = parseFile(testDataPath("basic/simple.csv"));
  EXPECT_EQ(chunks.total_rows, 3u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "1");
}

TEST_F(CsvReaderTest, BackslashEscape_MixedQuotedUnquoted) {
  libvroom::CsvOptions opts;
  opts.escape_backslash = true;
  opts.separator = ',';

  std::string csv = "a,b,c\nunquoted,\"quoted \\\"value\\\"\",123\n";
  auto [chunks, schema] = parseContent(csv, opts);
  ASSERT_EQ(chunks.total_rows, 1u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "unquoted");
  EXPECT_EQ(getStringValue(chunks, 1, 0), "quoted \"value\"");
  EXPECT_EQ(getStringValue(chunks, 2, 0), "123");
}

TEST_F(CsvReaderTest, BackslashEscape_NoHeader) {
  libvroom::CsvOptions opts;
  opts.escape_backslash = true;
  opts.separator = ',';
  opts.has_header = false;

  std::string csv = "\"hello \\\"world\\\"\",42\n";
  auto [chunks, schema] = parseContent(csv, opts);
  ASSERT_EQ(chunks.total_rows, 1u);
  EXPECT_EQ(getStringValue(chunks, 0, 0), "hello \"world\"");
  EXPECT_EQ(getStringValue(chunks, 1, 0), "42");
}
```

**Step 2: Build and run tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -R CsvReaderTest -j$(nproc)
```

Expected: All tests PASS including new backslash escape tests.

**Step 3: Commit**

```bash
git add test/csv_reader_test.cpp
git commit -m "test: add integration tests for backslash escape mode"
```

---

## Task 9: SIMD escape mask in chunk analysis

Add actual backslash escape handling to SIMD 64-byte block processing.

**Files:**
- Modify: `src/parser/simd_chunk_finder.cpp`

**Step 1: Update AnalyzeChunkSimdImpl SIMD loop**

In the 64-byte block processing loop (the `while (offset + 64 <= size)` loop), when `escape_backslash` is true:

1. Add a backslash vector: `const auto bs_vec = hn::Set(d, static_cast<uint8_t>('\\'));`
2. Inside the block loop, also scan for backslash:
```cpp
auto bsm = hn::Eq(block, bs_vec);
uint8_t bs_bytes[HWY_MAX_BYTES / 8] = {0};
hn::StoreMaskBits(d, bsm, bs_bytes);
// ... accumulate into backslash_bits
```
3. Before computing `find_quote_mask`, compute the escaped mask:
```cpp
if (escape_backslash) {
  auto [escaped, escape] = compute_escaped_mask(backslash_bits, escape_state);
  quote_bits &= ~escaped; // Remove escaped quotes
}
```
4. Thread `escape_state` (uint64_t, initialized to 0) through the loop.

**Step 2: Update AnalyzeChunkDualStateSimdImpl similarly**

Same changes: scan for backslash, compute escaped mask, remove escaped quotes before computing quote parity.

Note: The dual-state algorithm doesn't track starting quote state for backslash escaping - the escape state is independent of quote state. Initialize `escape_state = 0` and thread through.

**Step 3: Update FindRowEndSimdImpl similarly**

Same pattern for the SIMD loop.

**Step 4: Write SIMD boundary test**

Add to `test/simd_parsing_test.cpp`:

```cpp
TEST(SimdParsingTest, BackslashEscape_SIMDBoundary) {
  // Create data where a backslash escape spans the 64-byte SIMD boundary
  std::string data(64, 'x');
  // Put backslash at position 63 (last byte of first SIMD block)
  data[0] = '"'; // opening quote
  data[63] = '\\';
  // Position 64 (first byte of second block) is the escaped character
  data += "\""; // this " at position 64 should be escaped, not close the quote
  data += ",rest";
  data += "\n";

  auto [row_count, last_end] = libvroom::count_rows_simd(data.data(), data.size(), '"', true);
  // Should find 1 row (the newline at the end)
  EXPECT_EQ(row_count, 1u);
}

TEST(SimdParsingTest, BackslashEscape_DoubleBackslashSIMDBoundary) {
  // Two backslashes spanning SIMD boundary: \\ at positions 62-63
  std::string data(64, 'x');
  data[0] = '"'; // opening quote
  data[62] = '\\';
  data[63] = '\\';
  // Position 64: quote should NOT be escaped (even backslashes)
  data += "\""; // closing quote
  data += ",rest";
  data += "\n";

  auto [row_count, last_end] = libvroom::count_rows_simd(data.data(), data.size(), '"', true);
  EXPECT_EQ(row_count, 1u);
}
```

**Step 5: Build and run all tests**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: All tests PASS.

**Step 6: Commit**

```bash
git add src/parser/simd_chunk_finder.cpp test/simd_parsing_test.cpp
git commit -m "feat: add SIMD backslash escape mask to chunk analysis functions"
```

---

## Task 10: Python bindings

**Files:**
- Modify: `python/src/bindings.cpp`
- Modify: `python/src/vroom_csv/_core.pyi` (type stubs)

**Step 1: Add escape_backslash parameter to read_csv**

In `python/src/bindings.cpp`, in the `read_csv` function:
1. Add parameter: `bool escape_backslash = false`
2. Set: `csv_opts.escape_backslash = escape_backslash;`
3. Add to the pybind11 def: `py::arg("escape_backslash") = false`
4. Update the docstring

In `python/src/vroom_csv/_core.pyi`:
1. Add `escape_backslash: bool = False` to the `read_csv` signature

**Step 2: Build Python bindings**

```bash
cd python && pip install -e . 2>&1 | tail -5
```

**Step 3: Commit**

```bash
git add python/src/bindings.cpp python/src/vroom_csv/_core.pyi
git commit -m "feat: expose escape_backslash in Python bindings"
```

---

## Task 11: CLI support

**Files:**
- Modify: `src/cli.cpp`

**Step 1: Add --escape-backslash flag**

Add a CLI flag `--escape-backslash` (boolean) that sets `opts.csv.escape_backslash = true`.

**Step 2: Build and test**

```bash
cmake --build build -j$(nproc) 2>&1 | tail -5
./build/vroom head --escape-backslash test/data/escape/backslash_escape.csv
```

Expected: Fields display with unescaped values.

**Step 3: Commit**

```bash
git add src/cli.cpp
git commit -m "feat: add --escape-backslash CLI flag"
```

---

## Task 12: Final verification and cleanup

**Step 1: Run full test suite**

```bash
cd build && ctest --output-on-failure -j$(nproc)
```

Expected: ALL tests pass.

**Step 2: Run benchmarks to verify no regression**

```bash
./build/libvroom_benchmark
```

Expected: No regression with `escape_backslash=false` (default path unchanged).

**Step 3: Format code**

```bash
# Format all modified files
clang-format -i include/libvroom/escape_mask.h include/libvroom/options.h include/libvroom/parse_utils.h include/libvroom/split_fields.h include/libvroom/vroom.h src/parser/simd_chunk_finder.cpp src/parser/chunk_finder.cpp src/parser/line_parser.cpp src/schema/type_inference.cpp src/reader/csv_reader.cpp src/cli.cpp test/csv_reader_test.cpp test/simd_parsing_test.cpp python/src/bindings.cpp
```

**Step 4: Final commit**

```bash
git add -A
git commit -m "chore: format code"
```
