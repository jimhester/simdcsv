/**
 * scsv - Command-line utility for CSV processing using simdcsv
 * Inspired by zsv (https://github.com/liquidaty/zsv)
 *
 * Note: The pretty command truncates long fields for display. This truncation
 * operates on bytes, not Unicode code points, so multi-byte UTF-8 sequences
 * may be split, resulting in invalid UTF-8 in the output. This is a display
 * limitation only; the underlying data is not modified.
 */

#include <unistd.h>
#include <algorithm>
#include <cstdlib>
#include <cstring>
#include <future>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "common_defs.h"
#include "dialect.h"
#include "io_util.h"
#include "mem_util.h"
#include "simd_highway.h"
#include "two_pass.h"

using namespace std;

// Constants
constexpr int MAX_THREADS = 256;
constexpr int MIN_THREADS = 1;
constexpr size_t MAX_COLUMN_WIDTH = 40;
constexpr size_t DEFAULT_NUM_ROWS = 10;
constexpr const char* VERSION = "0.1.0";

// Performance tuning constants
constexpr size_t QUOTE_LOOKBACK_LIMIT = 64 * 1024;  // 64KB lookback for quote state
constexpr size_t MAX_BOUNDARY_SEARCH = 8192;        // Max search for row boundary
constexpr size_t MIN_PARALLEL_SIZE = 1024 * 1024;   // Minimum size for parallel processing

/**
 * CSV Iterator - Helper class to iterate over parsed CSV data
 */
class CsvIterator {
 public:
  CsvIterator(const uint8_t* buf, const simdcsv::index& idx)
      : buf_(buf), idx_(idx) {
    // Merge indexes from all threads into sorted order
    mergeIndexes();
  }

  size_t numFields() const { return merged_indexes_.size(); }

  // Get the content of field at position i (0-indexed)
  std::string getField(size_t i) const {
    if (i >= merged_indexes_.size()) return "";

    size_t start = (i == 0) ? 0 : merged_indexes_[i - 1] + 1;
    size_t end = merged_indexes_[i];

    // Bounds check: ensure start <= end
    if (start > end) return "";

    // Handle quoted fields
    std::string field;
    bool in_quote = false;
    for (size_t j = start; j < end; ++j) {
      char c = static_cast<char>(buf_[j]);
      if (c == '"') {
        if (in_quote && j + 1 < end && buf_[j + 1] == '"') {
          field += '"';
          ++j;  // Skip escaped quote
        } else {
          in_quote = !in_quote;
        }
      } else {
        field += c;
      }
    }
    return field;
  }

  // Check if a field ends with newline (marks end of row)
  bool isRowEnd(size_t i) const {
    if (i >= merged_indexes_.size()) return true;
    size_t pos = merged_indexes_[i];
    return buf_[pos] == '\n';
  }

  // Get all rows as vector of vectors of strings
  std::vector<std::vector<std::string>> getRows(size_t max_rows = SIZE_MAX) const {
    std::vector<std::vector<std::string>> rows;
    std::vector<std::string> current_row;

    for (size_t i = 0; i < merged_indexes_.size() && rows.size() < max_rows; ++i) {
      current_row.push_back(getField(i));
      if (isRowEnd(i)) {
        rows.push_back(std::move(current_row));
        current_row.clear();
      }
    }
    if (!current_row.empty() && rows.size() < max_rows) {
      rows.push_back(std::move(current_row));
    }
    return rows;
  }

  size_t countRows() const {
    size_t count = 0;
    for (size_t i = 0; i < merged_indexes_.size(); ++i) {
      if (isRowEnd(i)) ++count;
    }
    return count;
  }

 private:
  void mergeIndexes() {
    // Calculate total size
    size_t total = 0;
    for (int i = 0; i < idx_.n_threads; ++i) {
      total += idx_.n_indexes[i];
    }
    merged_indexes_.reserve(total);

    // Collect all indexes
    for (int t = 0; t < idx_.n_threads; ++t) {
      for (uint64_t j = 0; j < idx_.n_indexes[t]; ++j) {
        auto ii = t + (j * idx_.n_threads);
        merged_indexes_.push_back(idx_.indexes[ii]);
      }
    }

    // Sort them
    std::sort(merged_indexes_.begin(), merged_indexes_.end());
  }

  const uint8_t* buf_;
  const simdcsv::index& idx_;
  std::vector<uint64_t> merged_indexes_;
};

void printVersion() {
  cout << "scsv version " << VERSION << '\n';
}

void printUsage(const char* prog) {
  cerr << "scsv - High-performance CSV processing tool\n\n";
  cerr << "Usage: " << prog << " <command> [options] [csvfile]\n\n";
  cerr << "Commands:\n";
  cerr << "  count         Count the number of rows\n";
  cerr << "  head          Display the first N rows (default: " << DEFAULT_NUM_ROWS << ")\n";
  cerr << "  select        Select specific columns by name or index\n";
  cerr << "  info          Display information about the CSV file\n";
  cerr << "  pretty        Pretty-print the CSV with aligned columns\n";
  cerr << "  dialect       Detect and output the CSV dialect\n";
  cerr << "\nArguments:\n";
  cerr << "  csvfile       Path to CSV file, or '-' to read from stdin.\n";
  cerr << "                If omitted, reads from stdin.\n";
  cerr << "\nOptions:\n";
  cerr << "  -n <num>      Number of rows (for head/pretty)\n";
  cerr << "  -c <cols>     Comma-separated column names or indices (for select)\n";
  cerr << "  -H            No header row in input\n";
  cerr << "  -t <threads>  Number of threads (default: 1, max: " << MAX_THREADS << ")\n";
  cerr << "  -d <delim>    Field delimiter (disables auto-detection)\n";
  cerr << "                Values: comma, tab, semicolon, pipe, or single character\n";
  cerr << "  -q <char>     Quote character (default: \")\n";
  cerr << "  -h            Show this help message\n";
  cerr << "  -v            Show version information\n";
  cerr << "\nDialect Detection:\n";
  cerr << "  By default, scsv auto-detects the CSV dialect (delimiter, quote character,\n";
  cerr << "  escape style). Use -d to explicitly specify a delimiter and disable\n";
  cerr << "  auto-detection.\n";
  cerr << "\nExamples:\n";
  cerr << "  " << prog << " count data.csv\n";
  cerr << "  " << prog << " head -n 5 data.csv\n";
  cerr << "  " << prog << " select -c name,age data.csv\n";
  cerr << "  " << prog << " select -c 0,2,4 data.csv\n";
  cerr << "  " << prog << " info data.csv\n";
  cerr << "  " << prog << " pretty -n 20 data.csv\n";
  cerr << "  " << prog << " count -d tab data.tsv\n";
  cerr << "  " << prog << " head -d semicolon european.csv\n";
  cerr << "  " << prog << " dialect unknown_format.csv\n";
  cerr << "  " << prog << " dialect -j data.csv       # JSON output\n";
  cerr << "  cat data.csv | " << prog << " count\n";
  cerr << "  " << prog << " head - < data.csv\n";
}

// Helper to check if reading from stdin
static bool isStdinInput(const char* filename) {
  return filename == nullptr || strcmp(filename, "-") == 0;
}

// Parse a file or stdin - returns true on success
// Caller is responsible for freeing data with aligned_free()
bool parseFile(const char* filename, int n_threads,
               std::basic_string_view<uint8_t>& data, simdcsv::index& idx,
               const simdcsv::Dialect& dialect = simdcsv::Dialect::csv(),
               bool auto_detect = false) {
  try {
    if (isStdinInput(filename)) {
      data = get_corpus_stdin(SIMDCSV_PADDING);
    } else {
      data = get_corpus(filename, SIMDCSV_PADDING);
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "'" << endl;
    }
    return false;
  }

  simdcsv::two_pass parser;
  idx = parser.init(data.size(), n_threads);

  if (auto_detect) {
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::DetectionResult detected;
    parser.parse_auto(data.data(), idx, data.size(), errors, &detected);
    if (detected.success()) {
      cerr << "Auto-detected: " << detected.dialect.to_string() << endl;
    }
  } else {
    parser.parse(data.data(), idx, data.size(), dialect);
  }
  return true;
}

// Helper function to parse delimiter string
simdcsv::Dialect parseDialect(const std::string& delimiter_str, char quote_char) {
  simdcsv::Dialect dialect;
  dialect.quote_char = quote_char;

  if (delimiter_str == "comma" || delimiter_str == ",") {
    dialect.delimiter = ',';
  } else if (delimiter_str == "tab" || delimiter_str == "\\t") {
    dialect.delimiter = '\t';
  } else if (delimiter_str == "semicolon" || delimiter_str == ";") {
    dialect.delimiter = ';';
  } else if (delimiter_str == "pipe" || delimiter_str == "|") {
    dialect.delimiter = '|';
  } else if (delimiter_str.length() == 1) {
    dialect.delimiter = delimiter_str[0];
  } else {
    cerr << "Warning: Unknown delimiter '" << delimiter_str << "', using comma\n";
    dialect.delimiter = ',';
  }

  return dialect;
}

/// ============================================================================
// Optimized Row Counting - Avoids building full index for count command
// ============================================================================

// SIMD row counter - processes 64 bytes at a time
// Note on escaped quotes (CSV ""): The SIMD path uses XOR-prefix to compute
// quote state, which toggles on every quote. For escaped quotes "", this means
// toggling twice (net effect: state unchanged). This is correct for row counting
// because: (1) "" are adjacent by definition, so no newline can appear between
// them, and (2) the final quote state after "" matches the correct semantics.
// The scalar fallback explicitly handles "" for consistency with the library.
size_t countRowsSimd(const uint8_t* buf, size_t len) {
  size_t row_count = 0;
  size_t idx = 0;
  uint64_t prev_iter_inside_quote = 0ULL;

  // Process 64 bytes at a time using SIMD
  for (; idx + 64 <= len; idx += 64) {
    simdcsv::simd_input in = simdcsv::fill_input(buf + idx);

    // Find all quotes and newlines in this 64-byte block
    uint64_t quotes = simdcsv::cmp_mask_against_input(in, '"');
    uint64_t newlines = simdcsv::cmp_mask_against_input(in, '\n');

    // Build quote mask (1 = inside quote, 0 = outside)
    uint64_t quote_mask = simdcsv::find_quote_mask2(in, quotes, prev_iter_inside_quote);

    // Newlines outside quotes
    uint64_t valid_newlines = newlines & ~quote_mask;

    // Count the newlines
    row_count += simdcsv::count_ones(valid_newlines);
  }

  // Handle remaining bytes with scalar code (properly handles escaped quotes "")
  bool in_quote = (prev_iter_inside_quote != 0);
  for (; idx < len; ++idx) {
    if (buf[idx] == '"') {
      // Check for escaped quote ("")
      if (idx + 1 < len && buf[idx + 1] == '"') {
        ++idx;  // Skip both quotes - escaped quote doesn't toggle state
      } else {
        in_quote = !in_quote;
      }
    } else if (buf[idx] == '\n' && !in_quote) {
      ++row_count;
    }
  }

  return row_count;
}

// Direct row counter - uses SIMD for large data
size_t countRowsDirect(const uint8_t* buf, size_t len) {
  if (len >= 64) {
    return countRowsSimd(buf, len);
  }

  // Scalar path for small files (properly handles escaped quotes "")
  size_t row_count = 0;
  bool in_quote = false;

  for (size_t i = 0; i < len; ++i) {
    if (buf[i] == '"') {
      // Check for escaped quote ("")
      if (i + 1 < len && buf[i + 1] == '"') {
        ++i;  // Skip both quotes - escaped quote doesn't toggle state
      } else {
        in_quote = !in_quote;
      }
    } else if (buf[i] == '\n' && !in_quote) {
      ++row_count;
    }
  }

  return row_count;
}

// Determine if position is inside or outside a quoted field
// Uses proven speculative approach from two_pass.h with 64KB lookback
enum QuoteState { OUTSIDE_QUOTE, INSIDE_QUOTE, AMBIGUOUS };

// Helper function matching two_pass.h logic
static bool isOther(uint8_t c) { return c != ',' && c != '\n' && c != '"'; }

static QuoteState getQuoteState(const uint8_t* buf, size_t pos) {
  // Uses the same proven logic as two_pass::get_quotation_state
  if (pos == 0) return OUTSIDE_QUOTE;

  size_t end = pos > QUOTE_LOOKBACK_LIMIT ? pos - QUOTE_LOOKBACK_LIMIT : 0;
  size_t i = pos;
  size_t num_quotes = 0;

  // Scan backwards looking for quote-other patterns that determine state
  while (i > end) {
    if (buf[i] == '"') {
      // q-o case: quote followed by non-delimiter means we found end of quoted field
      if (i + 1 < pos && isOther(buf[i + 1])) {
        return num_quotes % 2 == 0 ? INSIDE_QUOTE : OUTSIDE_QUOTE;
      }
      // o-q case: non-delimiter before quote means we found start of quoted field
      else if (i > end && isOther(buf[i - 1])) {
        return num_quotes % 2 == 0 ? OUTSIDE_QUOTE : INSIDE_QUOTE;
      }
      ++num_quotes;
    }
    --i;
  }

  // Check the boundary position
  if (buf[end] == '"') {
    ++num_quotes;
  }

  return AMBIGUOUS;
}

// Find a valid row boundary near target position
static size_t findRowBoundary(const uint8_t* buf, size_t len, size_t target) {
  QuoteState state = getQuoteState(buf, target);
  size_t limit = std::min(target + MAX_BOUNDARY_SEARCH, len);
  bool in_quote = (state == INSIDE_QUOTE);

  for (size_t pos = target; pos < limit; ++pos) {
    if (buf[pos] == '"') {
      // Check for escaped quote ("")
      if (pos + 1 < limit && buf[pos + 1] == '"') {
        ++pos;  // Skip both quotes - escaped quote doesn't toggle state
      } else {
        in_quote = !in_quote;
      }
    } else if (buf[pos] == '\n' && !in_quote) {
      return pos + 1;
    }
  }

  return target;
}

// Parallel direct row counter
size_t countRowsDirectParallel(const uint8_t* buf, size_t len, int n_threads) {
  if (n_threads <= 1 || len < MIN_PARALLEL_SIZE) {
    return countRowsDirect(buf, len);
  }

  size_t chunk_size = len / n_threads;
  std::vector<size_t> chunk_starts(n_threads + 1);

  chunk_starts[0] = 0;
  chunk_starts[n_threads] = len;

  // Find chunk boundaries in parallel
  std::vector<std::future<size_t>> boundary_futures;
  for (int i = 1; i < n_threads; ++i) {
    size_t target = chunk_size * i;
    boundary_futures.push_back(std::async(std::launch::async, [buf, len, target]() {
      return findRowBoundary(buf, len, target);
    }));
  }

  for (int i = 1; i < n_threads; ++i) {
    chunk_starts[i] = boundary_futures[i - 1].get();
  }

  // Count rows in each chunk in parallel
  std::vector<std::future<size_t>> count_futures;
  for (int i = 0; i < n_threads; ++i) {
    size_t start = chunk_starts[i];
    size_t end = chunk_starts[i + 1];
    count_futures.push_back(std::async(std::launch::async, [buf, start, end]() {
      return countRowsDirect(buf + start, end - start);
    }));
  }

  size_t total = 0;
  for (auto& f : count_futures) {
    total += f.get();
  }

  return total;
}

// Command: count
int cmdCount(const char* filename, int n_threads, bool has_header,
             const simdcsv::Dialect& dialect = simdcsv::Dialect::csv(),
             bool auto_detect = false) {
  std::basic_string_view<uint8_t> data;

  try {
    if (isStdinInput(filename)) {
      data = get_corpus_stdin(SIMDCSV_PADDING);
    } else {
      data = get_corpus(filename, SIMDCSV_PADDING);
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "'" << endl;
    }
    return 1;
  }

  // Use optimized direct row counting - much faster than building full index
  // Note: For non-standard dialects, this still uses standard quote char
  // TODO: Make countRowsDirectParallel dialect-aware
  size_t rows = countRowsDirectParallel(data.data(), data.size(), n_threads);

  // Subtract header if present
  if (has_header && rows > 0) {
    cout << (rows - 1) << endl;
  } else {
    cout << rows << endl;
  }

  aligned_free((void*)data.data());
  return 0;
}

// Command: head
int cmdHead(const char* filename, int n_threads, size_t num_rows, bool has_header,
            const simdcsv::Dialect& dialect = simdcsv::Dialect::csv(),
            bool auto_detect = false) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx, dialect, auto_detect)) return 1;

  CsvIterator iter(data.data(), idx);
  // Get num_rows + 1 if we have header to show header plus num_rows data rows
  auto rows = iter.getRows(has_header ? num_rows + 1 : num_rows);

  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) cout << dialect.delimiter;
      // Re-quote if field contains special characters
      bool needs_quote =
          row[i].find(dialect.delimiter) != string::npos ||
          row[i].find(dialect.quote_char) != string::npos ||
          row[i].find('\n') != string::npos;
      if (needs_quote) {
        cout << dialect.quote_char;
        for (char c : row[i]) {
          if (c == dialect.quote_char) cout << dialect.quote_char;
          cout << c;
        }
        cout << dialect.quote_char;
      } else {
        cout << row[i];
      }
    }
    cout << '\n';
  }

  aligned_free((void*)data.data());
  return 0;
}

// Command: select
int cmdSelect(const char* filename, int n_threads, const string& columns,
              bool has_header, const simdcsv::Dialect& dialect = simdcsv::Dialect::csv(),
              bool auto_detect = false) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx, dialect, auto_detect)) return 1;

  CsvIterator iter(data.data(), idx);
  auto rows = iter.getRows();

  if (rows.empty()) {
    aligned_free((void*)data.data());
    return 0;
  }

  // Parse column specification
  vector<size_t> col_indices;
  vector<string> col_specs;

  // Split columns by comma
  stringstream ss(columns);
  string spec;
  while (getline(ss, spec, ',')) {
    col_specs.push_back(spec);
  }

  // Resolve column names to indices if has_header
  const auto& header = rows[0];
  size_t num_cols = header.size();
  for (const auto& spec : col_specs) {
    // Try as numeric index first
    bool is_numeric = !spec.empty() && all_of(spec.begin(), spec.end(), ::isdigit);
    if (is_numeric) {
      size_t col_idx = stoul(spec);
      if (col_idx >= num_cols) {
        cerr << "Error: Column index " << col_idx << " is out of range (file has "
             << num_cols << " columns, indices 0-" << (num_cols - 1) << ")" << endl;
        aligned_free((void*)data.data());
        return 1;
      }
      col_indices.push_back(col_idx);
    } else if (has_header) {
      // Find by name
      auto it = find(header.begin(), header.end(), spec);
      if (it != header.end()) {
        col_indices.push_back(distance(header.begin(), it));
      } else {
        cerr << "Error: Column '" << spec << "' not found in header" << endl;
        aligned_free((void*)data.data());
        return 1;
      }
    } else {
      cerr << "Error: Cannot use column names without header (-H flag used)" << endl;
      aligned_free((void*)data.data());
      return 1;
    }
  }

  // Output selected columns
  for (const auto& row : rows) {
    bool first = true;
    for (size_t col : col_indices) {
      if (!first) cout << dialect.delimiter;
      first = false;
      // Column bounds already validated above, but handle rows with fewer columns
      if (col < row.size()) {
        const string& field = row[col];
        bool needs_quote = field.find(dialect.delimiter) != string::npos ||
                           field.find(dialect.quote_char) != string::npos ||
                           field.find('\n') != string::npos;
        if (needs_quote) {
          cout << dialect.quote_char;
          for (char c : field) {
            if (c == dialect.quote_char) cout << dialect.quote_char;
            cout << c;
          }
          cout << dialect.quote_char;
        } else {
          cout << field;
        }
      }
      // Empty field for rows with fewer columns (ragged CSV)
    }
    cout << '\n';
  }

  aligned_free((void*)data.data());
  return 0;
}

// Command: info
int cmdInfo(const char* filename, int n_threads, bool has_header,
            const simdcsv::Dialect& dialect = simdcsv::Dialect::csv(),
            bool auto_detect = false) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx, dialect, auto_detect)) return 1;

  CsvIterator iter(data.data(), idx);
  auto rows = iter.getRows();

  cout << "Source: " << (isStdinInput(filename) ? "<stdin>" : filename) << '\n';
  cout << "Size: " << data.size() << " bytes\n";
  cout << "Dialect: " << dialect.to_string() << '\n';

  size_t num_rows = rows.size();
  size_t num_cols = rows.empty() ? 0 : rows[0].size();

  if (has_header) {
    cout << "Rows: " << (num_rows > 0 ? num_rows - 1 : 0) << " (excluding header)\n";
  } else {
    cout << "Rows: " << num_rows << "\n";
  }
  cout << "Columns: " << num_cols << '\n';

  if (has_header && !rows.empty()) {
    cout << "\nColumn names:\n";
    for (size_t i = 0; i < rows[0].size(); ++i) {
      cout << "  " << i << ": " << rows[0][i] << '\n';
    }
  }

  aligned_free((void*)data.data());
  return 0;
}

// Command: pretty
int cmdPretty(const char* filename, int n_threads, size_t num_rows, bool has_header,
              const simdcsv::Dialect& dialect = simdcsv::Dialect::csv(),
              bool auto_detect = false) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx, dialect, auto_detect)) return 1;

  CsvIterator iter(data.data(), idx);
  auto rows = iter.getRows(has_header ? num_rows + 1 : num_rows);

  if (rows.empty()) {
    aligned_free((void*)data.data());
    return 0;
  }

  // Calculate column widths
  size_t num_cols = 0;
  for (const auto& row : rows) {
    num_cols = max(num_cols, row.size());
  }

  vector<size_t> widths(num_cols, 0);
  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      widths[i] = max(widths[i], row[i].size());
    }
  }

  // Limit width to reasonable size
  for (auto& w : widths) {
    w = min(w, MAX_COLUMN_WIDTH);
  }

  // Print separator line
  auto printSep = [&]() {
    cout << '+';
    for (size_t i = 0; i < num_cols; ++i) {
      cout << string(widths[i] + 2, '-') << '+';
    }
    cout << '\n';
  };

  // Print rows
  printSep();
  for (size_t r = 0; r < rows.size(); ++r) {
    const auto& row = rows[r];
    cout << '|';
    for (size_t i = 0; i < num_cols; ++i) {
      string val = (i < row.size()) ? row[i] : "";
      // Truncate if too long (note: may split multi-byte UTF-8 sequences)
      if (val.size() > widths[i] && widths[i] >= 3) {
        val = val.substr(0, widths[i] - 3) + "...";
      } else if (val.size() > widths[i]) {
        val = val.substr(0, widths[i]);
      }
      cout << ' ' << left << setw(widths[i]) << val << " |";
    }
    cout << '\n';

    // Print separator after header
    if (has_header && r == 0) {
      printSep();
    }
  }
  printSep();

  aligned_free((void*)data.data());
  return 0;
}

// Helper: format delimiter for display
static std::string formatDelimiter(char delim) {
  switch (delim) {
    case ',': return "comma";
    case '\t': return "tab";
    case ';': return "semicolon";
    case '|': return "pipe";
    case ':': return "colon";
    default: return std::string(1, delim);
  }
}

// Helper: format quote char for display
static std::string formatQuoteChar(char quote) {
  if (quote == '"') return "double-quote";
  if (quote == '\'') return "single-quote";
  if (quote == '\0') return "none";
  return std::string(1, quote);
}

// Helper: format line ending for display
static std::string formatLineEnding(simdcsv::Dialect::LineEnding le) {
  switch (le) {
    case simdcsv::Dialect::LineEnding::LF: return "LF";
    case simdcsv::Dialect::LineEnding::CRLF: return "CRLF";
    case simdcsv::Dialect::LineEnding::CR: return "CR";
    case simdcsv::Dialect::LineEnding::MIXED: return "mixed";
    default: return "unknown";
  }
}

// Command: dialect - detect and output CSV dialect
int cmdDialect(const char* filename, bool json_output) {
  std::basic_string_view<uint8_t> data;

  try {
    if (isStdinInput(filename)) {
      data = get_corpus_stdin(SIMDCSV_PADDING);
    } else {
      data = get_corpus(filename, SIMDCSV_PADDING);
    }
  } catch (const std::exception& e) {
    if (isStdinInput(filename)) {
      cerr << "Error: Could not read from stdin: " << e.what() << endl;
    } else {
      cerr << "Error: Could not load file '" << filename << "'" << endl;
    }
    return 1;
  }

  simdcsv::DialectDetector detector;
  auto result = detector.detect(data.data(), data.size());

  if (!result.success()) {
    cerr << "Error: Could not detect CSV dialect";
    if (!result.warning.empty()) {
      cerr << ": " << result.warning;
    }
    cerr << endl;
    aligned_free((void*)data.data());
    return 1;
  }

  const auto& d = result.dialect;

  if (json_output) {
    // JSON output for programmatic use
    cout << "{\n";
    cout << "  \"delimiter\": \"";
    if (d.delimiter == '\t') {
      cout << "\\t";
    } else if (d.delimiter == '"') {
      cout << "\\\"";
    } else if (d.delimiter == '\\') {
      cout << "\\\\";
    } else {
      cout << d.delimiter;
    }
    cout << "\",\n";
    cout << "  \"quote\": \"";
    if (d.quote_char == '\0') {
      cout << "";
    } else if (d.quote_char == '"') {
      cout << "\\\"";
    } else if (d.quote_char == '\\') {
      cout << "\\\\";
    } else {
      cout << d.quote_char;
    }
    cout << "\",\n";
    cout << "  \"escape\": \"" << (d.double_quote ? "double" : "backslash") << "\",\n";
    cout << "  \"line_ending\": \"" << formatLineEnding(d.line_ending) << "\",\n";
    cout << "  \"has_header\": " << (result.has_header ? "true" : "false") << ",\n";
    cout << "  \"columns\": " << result.detected_columns << ",\n";
    cout << "  \"confidence\": " << result.confidence << "\n";
    cout << "}\n";
  } else {
    // Human-readable output with CLI flags
    cout << "Detected dialect:\n";
    cout << "  Delimiter:    " << formatDelimiter(d.delimiter) << "\n";
    cout << "  Quote:        " << formatQuoteChar(d.quote_char) << "\n";
    cout << "  Escape:       " << (d.double_quote ? "double-quote (\"\")" : "backslash (\\)") << "\n";
    cout << "  Line ending:  " << formatLineEnding(d.line_ending) << "\n";
    cout << "  Has header:   " << (result.has_header ? "yes" : "no") << "\n";
    cout << "  Columns:      " << result.detected_columns << "\n";
    cout << "  Confidence:   " << static_cast<int>(result.confidence * 100) << "%\n";
    cout << "\n";

    // Output CLI flags that can be reused
    cout << "CLI flags: -d " << formatDelimiter(d.delimiter);
    if (d.quote_char != '"') {
      cout << " -q " << d.quote_char;
    }
    if (!result.has_header) {
      cout << " -H";
    }
    cout << "\n";
  }

  aligned_free((void*)data.data());
  return 0;
}

int main(int argc, char* argv[]) {
  if (argc < 2) {
    printUsage(argv[0]);
    return 1;
  }

  // Check for help or version
  if (strcmp(argv[1], "-h") == 0 || strcmp(argv[1], "--help") == 0) {
    printUsage(argv[0]);
    return 0;
  }
  if (strcmp(argv[1], "-v") == 0 || strcmp(argv[1], "--version") == 0) {
    printVersion();
    return 0;
  }

  // Parse command
  string command = argv[1];

  // Skip command for option parsing
  optind = 2;

  int n_threads = 1;
  size_t num_rows = DEFAULT_NUM_ROWS;
  bool has_header = true;
  bool auto_detect = true;  // Auto-detect by default
  bool delimiter_specified = false;  // Track if user specified delimiter
  bool json_output = false;  // JSON output for dialect command
  string columns;
  string delimiter_str = "comma";
  char quote_char = '"';

  int c;
  while ((c = getopt(argc, argv, "n:c:Ht:d:q:jhv")) != -1) {
    switch (c) {
      case 'n': {
        char* endptr;
        long val = strtol(optarg, &endptr, 10);
        if (*endptr != '\0' || val < 0) {
          cerr << "Error: Invalid row count '" << optarg << "'\n";
          return 1;
        }
        num_rows = static_cast<size_t>(val);
        break;
      }
      case 'c':
        columns = optarg;
        break;
      case 'H':
        has_header = false;
        break;
      case 't': {
        char* endptr;
        long val = strtol(optarg, &endptr, 10);
        if (*endptr != '\0' || val < MIN_THREADS || val > MAX_THREADS) {
          cerr << "Error: Thread count must be between " << MIN_THREADS
               << " and " << MAX_THREADS << "\n";
          return 1;
        }
        n_threads = static_cast<int>(val);
        break;
      }
      case 'd':
        delimiter_str = optarg;
        delimiter_specified = true;
        auto_detect = false;  // Disable auto-detect when delimiter is specified
        break;
      case 'q':
        if (strlen(optarg) == 1) {
          quote_char = optarg[0];
        } else {
          cerr << "Error: Quote character must be a single character\n";
          return 1;
        }
        break;
      case 'j':
        json_output = true;
        break;
      case 'h':
        printUsage(argv[0]);
        return 0;
      case 'v':
        printVersion();
        return 0;
      default:
        printUsage(argv[0]);
        return 1;
    }
  }

  // Allow reading from stdin if no filename is specified
  const char* filename = nullptr;
  if (optind < argc) {
    filename = argv[optind];
  }
  simdcsv::Dialect dialect = parseDialect(delimiter_str, quote_char);

  // Dispatch to command handlers
  if (command == "count") {
    return cmdCount(filename, n_threads, has_header, dialect, auto_detect);
  } else if (command == "head") {
    return cmdHead(filename, n_threads, num_rows, has_header, dialect, auto_detect);
  } else if (command == "select") {
    if (columns.empty()) {
      cerr << "Error: -c option required for select command\n";
      return 1;
    }
    return cmdSelect(filename, n_threads, columns, has_header, dialect, auto_detect);
  } else if (command == "info") {
    return cmdInfo(filename, n_threads, has_header, dialect, auto_detect);
  } else if (command == "pretty") {
    return cmdPretty(filename, n_threads, num_rows, has_header, dialect, auto_detect);
  } else if (command == "dialect") {
    // Note: dialect command ignores -d flag since it's for detection
    (void)delimiter_specified;  // Suppress unused warning
    return cmdDialect(filename, json_output);
  } else {
    cerr << "Error: Unknown command '" << command << "'\n";
    printUsage(argv[0]);
    return 1;
  }

  return 0;
}
