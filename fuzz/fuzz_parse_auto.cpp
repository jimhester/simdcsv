/**
 * @file fuzz_parse_auto.cpp
 * @brief LibFuzzer target for fuzz testing parse_auto.
 */

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <memory>

#include "two_pass.h"
#include "error.h"
#include "dialect.h"
#include "mem_util.h"

struct AlignedDeleter {
    void operator()(uint8_t* ptr) const { if (ptr) aligned_free(ptr); }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) return 0;
    // 64KB limit: Matches fuzz_csv_parser since parse_auto exercises both
    // dialect detection and full parsing paths
    constexpr size_t MAX_INPUT_SIZE = 64 * 1024;
    if (size > MAX_INPUT_SIZE) size = MAX_INPUT_SIZE;

    // Use immediate RAII to prevent leaks if an exception occurs
    std::unique_ptr<uint8_t, AlignedDeleter> guard(
        static_cast<uint8_t*>(aligned_malloc(64, size + 64)));
    if (!guard) return 0;
    uint8_t* buf = guard.get();
    std::memcpy(buf, data, size);
    std::memset(buf + size, 0, 64);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(size, 1);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::DetectionResult detected;
    bool success = parser.parse_auto(buf, idx, size, errors, &detected);

    // Use the results to exercise different code paths
    if (success && detected.success()) {
        (void)idx.n_indexes;
        (void)detected.detected_columns;
    }

    return 0;
}
