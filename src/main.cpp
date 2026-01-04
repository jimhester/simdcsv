#include <unistd.h>  // for getopt
#include <getopt.h>  // for getopt_long
#include <cstring>   // for strcmp
#include <iostream>
#include <limits>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "timing.h"
#include "debug.h"
#include "debug_parser.h"  // includes two_pass.h

using namespace std;

void print_usage(const char* program_name) {
  cerr << "Usage: " << program_name << " [options] [csvfile]" << endl;
  cerr << endl;
  cerr << "Parse and benchmark CSV file processing using SIMD operations." << endl;
  cerr << endl;
  cerr << "Arguments:" << endl;
  cerr << "  csvfile            Path to CSV file, or '-' to read from stdin." << endl;
  cerr << "                     If omitted, reads from stdin." << endl;
  cerr << endl;
  cerr << "Options:" << endl;
  cerr << "  -v, --verbose      Enable verbose output" << endl;
  cerr << "  -d, --dump         Dump index data" << endl;
  cerr << "  -t, --threads N    Number of threads (default: 1)" << endl;
  cerr << "  -i, --iterations N Number of iterations (default: 10)" << endl;
  cerr << "  --debug            Enable debug mode (verbose + timing + masks)" << endl;
  cerr << "  --debug-verbose    Enable verbose debug output" << endl;
  cerr << "  --debug-timing     Enable timing output" << endl;
  cerr << "  --debug-masks      Enable mask/buffer dumps" << endl;
  cerr << "  -h, --help         Show this help message" << endl;
  cerr << endl;
  cerr << "Examples:" << endl;
  cerr << "  " << program_name << " data.csv" << endl;
  cerr << "  cat data.csv | " << program_name << endl;
  cerr << "  " << program_name << " - < data.csv" << endl;
}

int main(int argc, char* argv[]) {
  int c;
  int n_threads = 1;
  bool verbose = false;
  bool dump = false;
  size_t iterations = 10;
  simdcsv::DebugConfig debug_config;

  static struct option long_options[] = {
    {"verbose",       no_argument,       0, 'v'},
    {"dump",          no_argument,       0, 'd'},
    {"threads",       required_argument, 0, 't'},
    {"iterations",    required_argument, 0, 'i'},
    {"debug",         no_argument,       0, 'D'},
    {"debug-verbose", no_argument,       0, 'V'},
    {"debug-timing",  no_argument,       0, 'T'},
    {"debug-masks",   no_argument,       0, 'M'},
    {"help",          no_argument,       0, 'h'},
    {0, 0, 0, 0}
  };

  int option_index = 0;
  while ((c = getopt_long(argc, argv, "vdt:i:h", long_options, &option_index)) != -1) {
    switch (c) {
      case 'v':
        verbose = true;
        break;
      case 'd':
        dump = true;
        break;
      case 't':
        n_threads = atoi(optarg);
        break;
      case 'i':
        iterations = atoi(optarg);
        break;
      case 'D':
        debug_config = simdcsv::DebugConfig::all();
        break;
      case 'V':
        debug_config.verbose = true;
        break;
      case 'T':
        debug_config.timing = true;
        break;
      case 'M':
        debug_config.dump_masks = true;
        break;
      case 'h':
        print_usage(argv[0]);
        return 0;
      default:
        print_usage(argv[0]);
        return 1;
    }
  }
  // Determine if reading from stdin or file
  bool read_from_stdin = false;
  const char* filename = nullptr;

  if (optind >= argc) {
    // No filename provided - read from stdin
    read_from_stdin = true;
  } else {
    filename = argv[optind];
    // Check if filename is "-" (stdin convention)
    if (std::strcmp(filename, "-") == 0) {
      read_from_stdin = true;
    }
  }

  std::basic_string_view<uint8_t> p;
  try {
    if (read_from_stdin) {
      p = get_corpus_stdin(SIMDCSV_PADDING);
    } else {
      p = get_corpus(filename, SIMDCSV_PADDING);
    }
  } catch (const std::exception& e) {  // caught by reference to base
    if (read_from_stdin) {
      std::cout << "Could not read from stdin: " << e.what() << std::endl;
    } else {
      std::cout << "Could not load the file " << filename << std::endl;
    }
    return EXIT_FAILURE;
  }

  simdcsv::debug_parser parser;
  simdcsv::DebugTrace trace(debug_config);

  // Print debug info if enabled
  if (debug_config.enabled()) {
    cout << "[simdcsv] Debug mode enabled" << endl;
    cout << "[simdcsv] SIMD: " << simdcsv::get_simd_info() << endl;
    cout << "[simdcsv] Input: " << (read_from_stdin ? "<stdin>" : filename) << endl;
    cout << "[simdcsv] Data size: " << p.size() << " bytes" << endl;
    cout << "[simdcsv] Threads: " << n_threads << endl;
    cout << "[simdcsv] Iterations: " << iterations << endl;
    cout << endl;
  }

#ifdef __linux__
  vector<int> evts;
  evts.push_back(PERF_COUNT_HW_CPU_CYCLES);
  evts.push_back(PERF_COUNT_HW_INSTRUCTIONS);
  evts.push_back(PERF_COUNT_HW_BRANCH_MISSES);
  evts.push_back(PERF_COUNT_HW_CACHE_REFERENCES);
  evts.push_back(PERF_COUNT_HW_CACHE_MISSES);
  evts.push_back(PERF_COUNT_HW_REF_CPU_CYCLES);
  TimingAccumulator ta(2, evts);
#endif  //__linux__

  simdcsv::index res = parser.init(p.size(), n_threads);

  double total = 0;  // naive accumulator
  for (size_t i = 0; i < iterations; i++) {
    struct timespec start, finish;
    clock_gettime(CLOCK_MONOTONIC, &start);
#ifdef __linux__
    {
      TimingPhase p1(ta, 0);
#endif  // __linux__
      if (debug_config.enabled() && i == 0) {
        // Use debug parsing on first iteration
        parser.parse_debug(p.data(), res, p.size(), trace);
      } else {
        parser.parse(p.data(), res, p.size());
      }
#ifdef __linux__
    }
#endif  // __linux__
    clock_gettime(CLOCK_MONOTONIC, &finish);

    double time_in_s = (finish.tv_sec - start.tv_sec) +
                       ((finish.tv_nsec - start.tv_nsec) / 1000000000.0);

    total += time_in_s;
  }
  double volume = p.size() * iterations;

  printf("Total time in (s) = %f\n", total);
  printf("GB/s: %f\n", volume / total / (1024 * 1024 * 1024));
#ifdef __linux__
  printf("Cycles per byte: %f\n", (1.0 * ta.results[0]) / volume);
  if (verbose) {
    cout << "Number of cycles                   = " << ta.results[0] << endl;
    cout << "Number of cycles per byte          = " << ta.results[0] / volume << endl;
    cout << "Number of cycles (ref)             = " << ta.results[5] << endl;
    cout << "Number of cycles (ref) per byte    = " << ta.results[5] / volume << endl;
    cout << "Number of instructions             = " << ta.results[1] << endl;
    cout << "Number of instructions per byte    = " << ta.results[1] / volume << endl;
    cout << "Number of instructions per cycle   = "
         << double(ta.results[1]) / ta.results[0] << endl;
    cout << "Number of branch misses            = " << ta.results[2] << endl;
    cout << "Number of branch misses per byte   = " << ta.results[2] / volume << endl;
    cout << "Number of cache references         = " << ta.results[3] << endl;
    cout << "Number of cache references per b.  = " << ta.results[3] / volume << endl;
    cout << "Number of cache misses             = " << ta.results[4] << endl;
    cout << "Number of cache misses per byte    = " << ta.results[4] / volume << endl;
    cout << "CPU freq (effective)               = "
         << ta.results[0] / total / (1000 * 1000 * 1000) << endl;
    cout << "CPU freq (base)                    = "
         << ta.results[5] / total / (1000 * 1000 * 1000) << endl;
  } else {
    ta.dump();
  }
#endif
  if (dump) {
    // res.write("out.idx");
    // res.read("out.idx");
    for (size_t iii = 0; iii < iterations; iii++) {
      struct timespec start, finish;
      clock_gettime(CLOCK_MONOTONIC, &start);
      uint64_t total = 0;
      for (auto i = 0; i < res.n_threads; ++i) {
        for (uint64_t j = 0; j < res.n_indexes[i]; ++j) {
          auto idx = i + (j * res.n_threads);
          total += res.indexes[idx];
          // printf("index[%" PRIu64 "] = %" PRIu64 "\n", idx, res.indexes[idx]);
        }
      }
      clock_gettime(CLOCK_MONOTONIC, &finish);
      double time_in_s1 = (finish.tv_sec - start.tv_sec) +
                          ((finish.tv_nsec - start.tv_nsec) / 1000000000.0);
      uint64_t num_indexes = 0;
      for (auto i = 0; i < res.n_threads; ++i) {
        num_indexes += res.n_indexes[i];
      }
      clock_gettime(CLOCK_MONOTONIC, &start);
      uint64_t* idx = new uint64_t[num_indexes];
      uint64_t k = 0;
      for (auto i = 0; i < res.n_threads; ++i) {
        for (uint64_t j = 0; j < res.n_indexes[i]; ++j) {
          auto ii = i + (j * res.n_threads);
          idx[k++] = res.indexes[ii];
        }
      }
      clock_gettime(CLOCK_MONOTONIC, &finish);
      double time_in_s15 = (finish.tv_sec - start.tv_sec) +
                           ((finish.tv_nsec - start.tv_nsec) / 1000000000.0);
      uint64_t total2 = 0;
      clock_gettime(CLOCK_MONOTONIC, &start);
      for (auto i = 0; i < num_indexes; ++i) {
        total2 += idx[i];
      }
      clock_gettime(CLOCK_MONOTONIC, &finish);
      double time_in_s2 = (finish.tv_sec - start.tv_sec) +
                          ((finish.tv_nsec - start.tv_nsec) / 1000000000.0);

      printf("total: %" PRIu64 "\ttotal2: %" PRIu64
             "\ntime: %f\ttime1.5: %f\ttime2: %f\n",
             total, total2, time_in_s1, time_in_s15, time_in_s2);
    }
  }

  aligned_free((void*)p.data());

  return 0;
}
