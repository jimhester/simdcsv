/**
 * vroom - Command-line utility for CSV to Parquet conversion
 *
 * Simplified CLI for the libvroom2 architecture.
 */

#include "libvroom.h"

#include <cstdlib>
#include <cstring>
#include <iomanip>
#include <iostream>
#include <string>
#include <unistd.h>

using namespace std;

// Constants
constexpr const char* VERSION = "2.0.0";

// =============================================================================
// Progress Bar Support
// =============================================================================

class ProgressBar {
public:
  explicit ProgressBar(bool enabled, size_t width = 40) : enabled_(enabled), width_(width) {}

  bool update(size_t bytes_processed, size_t total_bytes) {
    if (!enabled_ || total_bytes == 0)
      return true;

    int percent = static_cast<int>((bytes_processed * 100) / total_bytes);

    if (percent == last_percent_)
      return true;
    last_percent_ = percent;

    size_t filled = (percent * width_) / 100;

    std::string bar(width_, ' ');
    for (size_t i = 0; i < filled && i < width_; ++i) {
      bar[i] = '=';
    }
    if (filled < width_) {
      bar[filled] = '>';
    }

    std::cerr << "\r[" << bar << "] " << std::setw(3) << percent << "%" << std::flush;

    return true;
  }

  void finish() {
    if (enabled_) {
      std::string bar(width_, '=');
      std::cerr << "\r[" << bar << "] 100%" << std::endl;
    }
  }

  void clear() {
    if (enabled_) {
      std::cerr << "\r" << std::string(width_ + 7, ' ') << "\r" << std::flush;
    }
  }

  libvroom::ProgressCallback callback() {
    return [this](size_t processed, size_t total) { return this->update(processed, total); };
  }

private:
  bool enabled_;
  size_t width_;
  int last_percent_ = -1;
};

// =============================================================================
// Help and Usage
// =============================================================================

void print_version() {
  cout << "vroom " << VERSION << endl;
}

void print_usage() {
  cout << R"(vroom - High-performance CSV to Parquet converter

USAGE:
    vroom <COMMAND> [OPTIONS] <INPUT>

COMMANDS:
    convert     Convert CSV to Parquet format (default)
    head        Show first N rows of CSV file (stub)
    tail        Show last N rows of CSV file (stub)
    sample      Show random sample of rows (stub)
    stats       Show statistics about CSV file (stub)
    help        Show this help message
    version     Show version information

CONVERT OPTIONS:
    -o, --output <FILE>      Output Parquet file path (required)
    -c, --compression <TYPE> Compression: zstd, snappy, lz4, gzip, none (default: zstd)
    -r, --row-group <SIZE>   Rows per row group (default: 1000000)
    -j, --threads <N>        Number of threads (default: auto)
    -p, --progress           Show progress bar
    -v, --verbose            Verbose output
    -h, --help               Show this help message

CSV OPTIONS:
    -d, --delimiter <CHAR>   Field delimiter (default: ,)
    -q, --quote <CHAR>       Quote character (default: ")
    --no-header              CSV has no header row

EXAMPLES:
    vroom convert data.csv -o data.parquet
    vroom convert data.csv -o data.parquet -c snappy -j 4
    vroom data.csv -o data.parquet -p
    vroom head data.csv -n 20
    vroom stats data.csv

For more information, visit: https://github.com/jimhester/libvroom
)";
}

// =============================================================================
// Command: convert
// =============================================================================

int cmd_convert(int argc, char* argv[]) {
  // Options
  string input_path;
  string output_path;
  string compression = "zstd";
  size_t row_group_size = 1'000'000;
  size_t num_threads = 0;
  bool show_progress = false;
  bool verbose = false;
  char delimiter = ',';
  char quote = '"';
  bool has_header = true;

  // Parse arguments
  for (int i = 1; i < argc; ++i) {
    string arg = argv[i];

    if (arg == "-o" || arg == "--output") {
      if (++i >= argc) {
        cerr << "Error: --output requires a file path" << endl;
        return 1;
      }
      output_path = argv[i];
    } else if (arg == "-c" || arg == "--compression") {
      if (++i >= argc) {
        cerr << "Error: --compression requires a type" << endl;
        return 1;
      }
      compression = argv[i];
    } else if (arg == "-r" || arg == "--row-group") {
      if (++i >= argc) {
        cerr << "Error: --row-group requires a size" << endl;
        return 1;
      }
      row_group_size = stoul(argv[i]);
    } else if (arg == "-j" || arg == "--threads") {
      if (++i >= argc) {
        cerr << "Error: --threads requires a number" << endl;
        return 1;
      }
      num_threads = stoul(argv[i]);
    } else if (arg == "-d" || arg == "--delimiter") {
      if (++i >= argc) {
        cerr << "Error: --delimiter requires a character" << endl;
        return 1;
      }
      delimiter = argv[i][0];
    } else if (arg == "-q" || arg == "--quote") {
      if (++i >= argc) {
        cerr << "Error: --quote requires a character" << endl;
        return 1;
      }
      quote = argv[i][0];
    } else if (arg == "--no-header") {
      has_header = false;
    } else if (arg == "-p" || arg == "--progress") {
      show_progress = true;
    } else if (arg == "-v" || arg == "--verbose") {
      verbose = true;
    } else if (arg == "-h" || arg == "--help") {
      print_usage();
      return 0;
    } else if (arg[0] != '-' && input_path.empty()) {
      input_path = arg;
    } else if (arg == "convert") {
      // Skip command name
    } else {
      cerr << "Error: Unknown option: " << arg << endl;
      return 1;
    }
  }

  // Validate arguments
  if (input_path.empty()) {
    cerr << "Error: Input file required" << endl;
    print_usage();
    return 1;
  }

  if (output_path.empty()) {
    cerr << "Error: Output file required (use -o or --output)" << endl;
    return 1;
  }

  // Set up options
  libvroom::VroomOptions opts;
  opts.input_path = input_path;
  opts.output_path = output_path;
  opts.verbose = verbose;
  opts.progress = show_progress;

  // CSV options
  opts.csv.separator = delimiter;
  opts.csv.quote = quote;
  opts.csv.has_header = has_header;
  if (num_threads > 0) {
    opts.csv.num_threads = num_threads;
    opts.threads.num_threads = num_threads;
  }

  // Parquet options
  opts.parquet.row_group_size = row_group_size;

  // Set compression
  if (compression == "zstd") {
    opts.parquet.compression = libvroom::Compression::ZSTD;
  } else if (compression == "snappy") {
    opts.parquet.compression = libvroom::Compression::SNAPPY;
  } else if (compression == "lz4") {
    opts.parquet.compression = libvroom::Compression::LZ4;
  } else if (compression == "gzip") {
    opts.parquet.compression = libvroom::Compression::GZIP;
  } else if (compression == "none") {
    opts.parquet.compression = libvroom::Compression::NONE;
  } else {
    cerr << "Error: Unknown compression type: " << compression << endl;
    return 1;
  }

  // Set up progress callback
  ProgressBar progress(show_progress && isatty(STDERR_FILENO));
  libvroom::ProgressCallback progress_cb = nullptr;
  if (show_progress) {
    progress_cb = progress.callback();
  }

  // Run conversion
  if (verbose) {
    cerr << "Converting " << input_path << " to " << output_path << endl;
    cerr << "Compression: " << compression << endl;
    cerr << "Row group size: " << row_group_size << endl;
  }

  auto result = libvroom::convert_csv_to_parquet(opts, progress_cb);

  if (show_progress) {
    progress.finish();
  }

  if (!result.ok()) {
    cerr << "Error: " << result.error << endl;
    return 1;
  }

  if (verbose) {
    cerr << "Converted " << result.rows << " rows, " << result.cols << " columns" << endl;
  }

  return 0;
}

// =============================================================================
// Stub Commands
// =============================================================================

int cmd_head([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  cerr << "Error: 'head' command not yet implemented in libvroom2" << endl;
  cerr << "This command will be available in a future release." << endl;
  return 1;
}

int cmd_tail([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  cerr << "Error: 'tail' command not yet implemented in libvroom2" << endl;
  cerr << "This command will be available in a future release." << endl;
  return 1;
}

int cmd_sample([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  cerr << "Error: 'sample' command not yet implemented in libvroom2" << endl;
  cerr << "This command will be available in a future release." << endl;
  return 1;
}

int cmd_stats([[maybe_unused]] int argc, [[maybe_unused]] char* argv[]) {
  cerr << "Error: 'stats' command not yet implemented in libvroom2" << endl;
  cerr << "This command will be available in a future release." << endl;
  return 1;
}

// =============================================================================
// Main
// =============================================================================

int main(int argc, char* argv[]) {
  if (argc < 2) {
    print_usage();
    return 1;
  }

  string cmd = argv[1];

  if (cmd == "help" || cmd == "--help" || cmd == "-h") {
    print_usage();
    return 0;
  }

  if (cmd == "version" || cmd == "--version" || cmd == "-V") {
    print_version();
    return 0;
  }

  if (cmd == "convert") {
    return cmd_convert(argc, argv);
  }

  if (cmd == "head") {
    return cmd_head(argc, argv);
  }

  if (cmd == "tail") {
    return cmd_tail(argc, argv);
  }

  if (cmd == "sample") {
    return cmd_sample(argc, argv);
  }

  if (cmd == "stats") {
    return cmd_stats(argc, argv);
  }

  // Default: treat as convert command with file argument
  if (cmd[0] != '-' && cmd.find('.') != string::npos) {
    // Looks like a filename, run convert
    return cmd_convert(argc, argv);
  }

  cerr << "Error: Unknown command: " << cmd << endl;
  print_usage();
  return 1;
}
