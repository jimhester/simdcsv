#include <unistd.h>  // for getopt
#include <iostream>
#include <limits>
#include "common_defs.h"
#include "io_util.h"
#include "mem_util.h"
#include "timing.h"
#include "two_pass.h"

using namespace std;

int main(int argc, char* argv[]) {
  int c;
  int n_threads = 1;
  bool verbose = false;
  bool dump = false;
  size_t iterations = 10;

  while ((c = getopt(argc, argv, "vdt:si:s")) != -1) {
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
    }
  }
  if (optind >= argc) {
    cerr << "Usage: " << argv[0] << " <csvfile>" << endl;
    exit(1);
  }

  const char* filename = argv[optind];
  std::basic_string_view<uint8_t> p;
  try {
    p = get_corpus(filename, SIMDCSV_PADDING);
  } catch (const std::exception& e) {  // caught by reference to base
    std::cout << "Could not load the file " << filename << std::endl;
    return EXIT_FAILURE;
  }

  simdcsv::two_pass parser;

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
      parser.parse(p.data(), res, p.size());
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
