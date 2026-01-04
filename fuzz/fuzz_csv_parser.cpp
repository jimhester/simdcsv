/**
 * @file fuzz_csv_parser.cpp
 * @brief LibFuzzer target for fuzz testing the CSV parser.
 */

#include <cstdint>
#include <cstddef>
#include <cstring>
#include <memory>

#include "two_pass.h"
#include "error.h"
#include "mem_util.h"

struct AlignedDeleter {
    void operator()(uint8_t* ptr) const { if (ptr) aligned_free(ptr); }
};

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size == 0) return 0;
    constexpr size_t MAX_INPUT_SIZE = 64 * 1024;
    if (size > MAX_INPUT_SIZE) size = MAX_INPUT_SIZE;

    uint8_t* buf = static_cast<uint8_t*>(aligned_malloc(64, size + 64));
    if (!buf) return 0;
    std::unique_ptr<uint8_t, AlignedDeleter> guard(buf);
    std::memcpy(buf, data, size);
    std::memset(buf + size, 0, 64);

    simdcsv::two_pass parser;

    { // Single-threaded parsing
        simdcsv::index idx = parser.init(size, 1);
        try { parser.parse(buf, idx, size); } catch (...) {}
    }

    { // Error collection mode
        simdcsv::index idx = parser.init(size, 1);
        simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
        parser.parse_with_errors(buf, idx, size, errors);
    }

    return 0;
}
