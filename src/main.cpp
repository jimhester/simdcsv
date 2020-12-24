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

  while ((c = getopt(argc, argv, "")) != -1) {
    switch (c) {}
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

  double total = 0;         // naive accumulator
  clock_t start = clock();  // brutally portable

  simdcsv::index res;
#ifdef __linux__
  {
    TimingPhase p1(ta, 0);
#endif  // __linux__
    res = parser.parse(p.data(), p.size(), 16);
#ifdef __linux__
  }
  {
    TimingPhase p2(ta, 1);
  }     // the scoping business is an instance of C++ extreme programming
#endif  // __linux__
  total += clock() - start;  // brutally portable

  double time_in_s = total / CLOCKS_PER_SEC;

  printf("Total time in (s) = %f\n", time_in_s);
  printf("GB/s: %f\n", p.size() / time_in_s / (1024 * 1024 * 1024));
#ifdef __linux__
  printf("Cycles per byte: %f\n", (1.0 * ta.results[0]) / p.size());
  ta.dump();
#endif
  for (auto i = 0; i < res.n_threads; ++i) {
    for (uint64_t j = 0; j < res.n_indexes[i]; ++j) {
      auto idx = i + (j * res.n_threads);
      printf("index[%lu] = %lu\n", idx, res.indexes[idx]);
    }
  }
  delete[] res.indexes;
  delete[] res.n_indexes;
  aligned_free((void*)p.data());

  return 0;
}
