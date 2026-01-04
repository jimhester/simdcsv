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
    constexpr size_t MAX_INPUT_SIZE = 64 * 1024;
    if (size > MAX_INPUT_SIZE) size = MAX_INPUT_SIZE;

    uint8_t* buf = static_cast<uint8_t*>(aligned_malloc(64, size + 64));
    if (!buf) return 0;
    std::unique_ptr<uint8_t, AlignedDeleter> guard(buf);
    std::memcpy(buf, data, size);
    std::memset(buf + size, 0, 64);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(size, 1);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::DetectionResult detected;
    parser.parse_auto(buf, idx, size, errors, &detected);
    (void)detected.success();

    return 0;
}
