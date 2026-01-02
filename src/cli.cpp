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
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"

using namespace std;

// Constants
constexpr int MAX_THREADS = 256;
constexpr int MIN_THREADS = 1;
constexpr size_t MAX_COLUMN_WIDTH = 40;
constexpr size_t DEFAULT_NUM_ROWS = 10;
constexpr const char* VERSION = "0.1.0";

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
  cerr << "Usage: " << prog << " <command> [options] <csvfile>\n\n";
  cerr << "Commands:\n";
  cerr << "  count         Count the number of rows\n";
  cerr << "  head          Display the first N rows (default: " << DEFAULT_NUM_ROWS << ")\n";
  cerr << "  select        Select specific columns by name or index\n";
  cerr << "  info          Display information about the CSV file\n";
  cerr << "  pretty        Pretty-print the CSV with aligned columns\n";
  cerr << "\nOptions:\n";
  cerr << "  -n <num>      Number of rows (for head/pretty)\n";
  cerr << "  -c <cols>     Comma-separated column names or indices (for select)\n";
  cerr << "  -H            No header row in input\n";
  cerr << "  -t <threads>  Number of threads (default: 1, max: " << MAX_THREADS << ")\n";
  cerr << "  -h            Show this help message\n";
  cerr << "  -v            Show version information\n";
  cerr << "\nExamples:\n";
  cerr << "  " << prog << " count data.csv\n";
  cerr << "  " << prog << " head -n 5 data.csv\n";
  cerr << "  " << prog << " select -c name,age data.csv\n";
  cerr << "  " << prog << " select -c 0,2,4 data.csv\n";
  cerr << "  " << prog << " info data.csv\n";
  cerr << "  " << prog << " pretty -n 20 data.csv\n";
}

// Parse a file - returns true on success
// Caller is responsible for freeing data with aligned_free()
bool parseFile(const char* filename, int n_threads,
               std::basic_string_view<uint8_t>& data, simdcsv::index& idx) {
  try {
    data = get_corpus(filename, SIMDCSV_PADDING);
  } catch (const std::exception& e) {
    cerr << "Error: Could not load file '" << filename << "'" << endl;
    return false;
  }

  simdcsv::two_pass parser;
  idx = parser.init(data.size(), n_threads);
  parser.parse(data.data(), idx, data.size());
  return true;
}

// Command: count
int cmdCount(const char* filename, int n_threads, bool has_header) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx)) return 1;

  CsvIterator iter(data.data(), idx);
  size_t rows = iter.countRows();

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
int cmdHead(const char* filename, int n_threads, size_t num_rows, bool has_header) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx)) return 1;

  CsvIterator iter(data.data(), idx);
  // Get num_rows + 1 if we have header to show header plus num_rows data rows
  auto rows = iter.getRows(has_header ? num_rows + 1 : num_rows);

  for (const auto& row : rows) {
    for (size_t i = 0; i < row.size(); ++i) {
      if (i > 0) cout << ",";
      // Re-quote if field contains special characters
      bool needs_quote =
          row[i].find(',') != string::npos || row[i].find('"') != string::npos ||
          row[i].find('\n') != string::npos;
      if (needs_quote) {
        cout << '"';
        for (char c : row[i]) {
          if (c == '"') cout << '"';
          cout << c;
        }
        cout << '"';
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
              bool has_header) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx)) return 1;

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
      if (!first) cout << ",";
      first = false;
      // Column bounds already validated above, but handle rows with fewer columns
      if (col < row.size()) {
        const string& field = row[col];
        bool needs_quote = field.find(',') != string::npos ||
                           field.find('"') != string::npos ||
                           field.find('\n') != string::npos;
        if (needs_quote) {
          cout << '"';
          for (char c : field) {
            if (c == '"') cout << '"';
            cout << c;
          }
          cout << '"';
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
int cmdInfo(const char* filename, int n_threads, bool has_header) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx)) return 1;

  CsvIterator iter(data.data(), idx);
  auto rows = iter.getRows();

  cout << "File: " << filename << '\n';
  cout << "Size: " << data.size() << " bytes\n";

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
int cmdPretty(const char* filename, int n_threads, size_t num_rows, bool has_header) {
  std::basic_string_view<uint8_t> data;
  simdcsv::index idx;

  if (!parseFile(filename, n_threads, data, idx)) return 1;

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
  string columns;

  int c;
  while ((c = getopt(argc, argv, "n:c:Ht:hv")) != -1) {
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

  if (optind >= argc) {
    cerr << "Error: No input file specified\n";
    printUsage(argv[0]);
    return 1;
  }

  const char* filename = argv[optind];

  // Dispatch to command handlers
  if (command == "count") {
    return cmdCount(filename, n_threads, has_header);
  } else if (command == "head") {
    return cmdHead(filename, n_threads, num_rows, has_header);
  } else if (command == "select") {
    if (columns.empty()) {
      cerr << "Error: -c option required for select command\n";
      return 1;
    }
    return cmdSelect(filename, n_threads, columns, has_header);
  } else if (command == "info") {
    return cmdInfo(filename, n_threads, has_header);
  } else if (command == "pretty") {
    return cmdPretty(filename, n_threads, num_rows, has_header);
  } else {
    cerr << "Error: Unknown command '" << command << "'\n";
    printUsage(argv[0]);
    return 1;
  }

  return 0;
}
