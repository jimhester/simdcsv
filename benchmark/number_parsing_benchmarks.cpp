/**
 * @file number_parsing_benchmarks.cpp
 * @brief Benchmarks comparing scalar vs SIMD number parsing performance.
 *
 * This benchmark suite measures the performance improvement from SIMD-accelerated
 * number parsing compared to scalar implementations.
 */

#include <benchmark/benchmark.h>
#include <random>
#include <string>
#include <vector>
#include <cstring>
#include <memory>

#include "simd_number_parsing.h"
#include "value_extraction.h"
#include "type_detector.h"

using namespace simdcsv;

namespace {

// Generate random integer strings
std::vector<std::string> generate_integer_strings(size_t count, int min_val, int max_val) {
    std::vector<std::string> result;
    result.reserve(count);

    std::mt19937 gen(42);  // Fixed seed for reproducibility
    std::uniform_int_distribution<int64_t> dist(min_val, max_val);

    for (size_t i = 0; i < count; ++i) {
        result.push_back(std::to_string(dist(gen)));
    }

    return result;
}

// Generate random float strings
std::vector<std::string> generate_float_strings(size_t count, double min_val, double max_val) {
    std::vector<std::string> result;
    result.reserve(count);

    std::mt19937 gen(42);
    std::uniform_real_distribution<double> dist(min_val, max_val);

    char buffer[32];
    for (size_t i = 0; i < count; ++i) {
        double val = dist(gen);
        snprintf(buffer, sizeof(buffer), "%.6f", val);
        result.push_back(buffer);
    }

    return result;
}

// Generate scientific notation strings
std::vector<std::string> generate_scientific_strings(size_t count) {
    std::vector<std::string> result;
    result.reserve(count);

    std::mt19937 gen(42);
    std::uniform_real_distribution<double> mantissa_dist(1.0, 10.0);
    std::uniform_int_distribution<int> exp_dist(-10, 10);

    char buffer[32];
    for (size_t i = 0; i < count; ++i) {
        double mantissa = mantissa_dist(gen);
        int exp = exp_dist(gen);
        snprintf(buffer, sizeof(buffer), "%.3fe%d", mantissa, exp);
        result.push_back(buffer);
    }

    return result;
}

// Global test data
constexpr size_t NUM_VALUES = 10000;

std::vector<std::string> g_small_integers;
std::vector<std::string> g_large_integers;
std::vector<std::string> g_floats;
std::vector<std::string> g_scientific;

void init_test_data() {
    static bool initialized = false;
    if (!initialized) {
        g_small_integers = generate_integer_strings(NUM_VALUES, -1000, 1000);
        g_large_integers = generate_integer_strings(NUM_VALUES, -1000000000LL, 1000000000LL);
        g_floats = generate_float_strings(NUM_VALUES, -1000.0, 1000.0);
        g_scientific = generate_scientific_strings(NUM_VALUES);
        initialized = true;
    }
}

}  // namespace

// =============================================================================
// Integer Parsing Benchmarks
// =============================================================================

static void BM_ScalarParseSmallIntegers(benchmark::State& state) {
    init_test_data();
    ExtractionConfig config;

    for (auto _ : state) {
        int64_t sum = 0;
        for (const auto& s : g_small_integers) {
            auto result = parse_integer<int64_t>(s.c_str(), s.size(), config);
            if (result.ok()) sum += result.get();
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_ScalarParseSmallIntegers);

static void BM_SIMDParseSmallIntegers(benchmark::State& state) {
    init_test_data();

    for (auto _ : state) {
        int64_t sum = 0;
        for (const auto& s : g_small_integers) {
            auto result = SIMDIntegerParser::parse_int64(s.c_str(), s.size());
            if (result.ok()) sum += result.value;
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_SIMDParseSmallIntegers);

static void BM_ScalarParseLargeIntegers(benchmark::State& state) {
    init_test_data();
    ExtractionConfig config;

    for (auto _ : state) {
        int64_t sum = 0;
        for (const auto& s : g_large_integers) {
            auto result = parse_integer<int64_t>(s.c_str(), s.size(), config);
            if (result.ok()) sum += result.get();
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_ScalarParseLargeIntegers);

static void BM_SIMDParseLargeIntegers(benchmark::State& state) {
    init_test_data();

    for (auto _ : state) {
        int64_t sum = 0;
        for (const auto& s : g_large_integers) {
            auto result = SIMDIntegerParser::parse_int64(s.c_str(), s.size());
            if (result.ok()) sum += result.value;
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_SIMDParseLargeIntegers);

// =============================================================================
// Float Parsing Benchmarks
// =============================================================================

static void BM_ScalarParseFloats(benchmark::State& state) {
    init_test_data();
    ExtractionConfig config;

    for (auto _ : state) {
        double sum = 0.0;
        for (const auto& s : g_floats) {
            auto result = parse_double(s.c_str(), s.size(), config);
            if (result.ok()) sum += result.get();
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_ScalarParseFloats);

static void BM_SIMDParseFloats(benchmark::State& state) {
    init_test_data();

    for (auto _ : state) {
        double sum = 0.0;
        for (const auto& s : g_floats) {
            auto result = SIMDDoubleParser::parse_double(s.c_str(), s.size());
            if (result.ok()) sum += result.value;
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_SIMDParseFloats);

static void BM_ScalarParseScientific(benchmark::State& state) {
    init_test_data();
    ExtractionConfig config;

    for (auto _ : state) {
        double sum = 0.0;
        for (const auto& s : g_scientific) {
            auto result = parse_double(s.c_str(), s.size(), config);
            if (result.ok()) sum += result.get();
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_ScalarParseScientific);

static void BM_SIMDParseScientific(benchmark::State& state) {
    init_test_data();

    for (auto _ : state) {
        double sum = 0.0;
        for (const auto& s : g_scientific) {
            auto result = SIMDDoubleParser::parse_double(s.c_str(), s.size());
            if (result.ok()) sum += result.value;
        }
        benchmark::DoNotOptimize(sum);
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_SIMDParseScientific);

// =============================================================================
// Type Validation Benchmarks
// =============================================================================

static void BM_ScalarTypeValidation(benchmark::State& state) {
    init_test_data();

    // Mix of integers and floats
    std::vector<std::string> mixed;
    mixed.reserve(NUM_VALUES * 2);
    for (size_t i = 0; i < NUM_VALUES / 2; ++i) {
        mixed.push_back(g_small_integers[i]);
        mixed.push_back(g_floats[i]);
    }

    std::vector<const uint8_t*> ptrs;
    std::vector<size_t> lengths;
    for (const auto& s : mixed) {
        ptrs.push_back(reinterpret_cast<const uint8_t*>(s.c_str()));
        lengths.push_back(s.size());
    }

    TypeDetectionOptions options;

    for (auto _ : state) {
        size_t int_count = 0, float_count = 0, other_count = 0;
        for (size_t i = 0; i < ptrs.size(); ++i) {
            auto type = TypeDetector::detect_field(ptrs[i], lengths[i], options);
            if (type == FieldType::INTEGER) ++int_count;
            else if (type == FieldType::FLOAT) ++float_count;
            else ++other_count;
        }
        benchmark::DoNotOptimize(int_count);
        benchmark::DoNotOptimize(float_count);
    }

    state.SetItemsProcessed(state.iterations() * mixed.size());
}
BENCHMARK(BM_ScalarTypeValidation);

static void BM_SIMDTypeValidation(benchmark::State& state) {
    init_test_data();

    // Mix of integers and floats
    std::vector<std::string> mixed;
    mixed.reserve(NUM_VALUES * 2);
    for (size_t i = 0; i < NUM_VALUES / 2; ++i) {
        mixed.push_back(g_small_integers[i]);
        mixed.push_back(g_floats[i]);
    }

    std::vector<const uint8_t*> ptrs;
    std::vector<size_t> lengths;
    for (const auto& s : mixed) {
        ptrs.push_back(reinterpret_cast<const uint8_t*>(s.c_str()));
        lengths.push_back(s.size());
    }

    for (auto _ : state) {
        size_t int_count, float_count, other_count;
        SIMDTypeValidator::validate_batch(ptrs.data(), lengths.data(), ptrs.size(),
                                          int_count, float_count, other_count);
        benchmark::DoNotOptimize(int_count);
        benchmark::DoNotOptimize(float_count);
    }

    state.SetItemsProcessed(state.iterations() * mixed.size());
}
BENCHMARK(BM_SIMDTypeValidation);

// =============================================================================
// Digit Validation Benchmarks
// =============================================================================

static void BM_ScalarDigitValidation(benchmark::State& state) {
    // Create strings of different lengths with all digits
    std::vector<std::string> digit_strings;
    for (size_t len = 1; len <= 100; ++len) {
        digit_strings.push_back(std::string(len, '5'));
    }

    for (auto _ : state) {
        size_t valid_count = 0;
        for (const auto& s : digit_strings) {
            bool valid = true;
            for (char c : s) {
                if (c < '0' || c > '9') {
                    valid = false;
                    break;
                }
            }
            if (valid) ++valid_count;
        }
        benchmark::DoNotOptimize(valid_count);
    }

    state.SetItemsProcessed(state.iterations() * digit_strings.size());
}
BENCHMARK(BM_ScalarDigitValidation);

static void BM_SIMDDigitValidation(benchmark::State& state) {
    // Create strings of different lengths with all digits
    std::vector<std::string> digit_strings;
    for (size_t len = 1; len <= 100; ++len) {
        digit_strings.push_back(std::string(len, '5'));
    }

    for (auto _ : state) {
        size_t valid_count = 0;
        for (const auto& s : digit_strings) {
            if (SIMDIntegerParser::validate_digits_simd(
                    reinterpret_cast<const uint8_t*>(s.c_str()), s.size())) {
                ++valid_count;
            }
        }
        benchmark::DoNotOptimize(valid_count);
    }

    state.SetItemsProcessed(state.iterations() * digit_strings.size());
}
BENCHMARK(BM_SIMDDigitValidation);

// =============================================================================
// SIMDTypeDetector Benchmarks
// =============================================================================

// Benchmark SIMDTypeDetector::all_digits (SIMD implementation)
static void BM_SIMDTypeDetector_AllDigits(benchmark::State& state) {
    size_t len = static_cast<size_t>(state.range(0));

    // Create a buffer of digits with proper alignment and padding
    std::vector<uint8_t> buffer(len + 64, '5');  // Extra padding for safety

    for (auto _ : state) {
        bool result = SIMDTypeDetector::all_digits(buffer.data(), len);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * len);
}
BENCHMARK(BM_SIMDTypeDetector_AllDigits)
    ->Arg(8)     // Small (scalar path)
    ->Arg(16)    // One SIMD vector (128-bit)
    ->Arg(32)    // Two vectors or one 256-bit
    ->Arg(64)    // Four vectors or one 512-bit
    ->Arg(128)   // Multiple SIMD iterations
    ->Arg(256)
    ->Arg(1024);

// Benchmark SIMDTypeDetector::classify_digits (SIMD implementation)
static void BM_SIMDTypeDetector_ClassifyDigits(benchmark::State& state) {
    size_t len = static_cast<size_t>(state.range(0));
    len = std::min(len, size_t(64));  // classify_digits only processes up to 64 bytes

    // Create a buffer of mixed digits and non-digits
    std::vector<uint8_t> buffer(64, 0);
    for (size_t i = 0; i < len; ++i) {
        buffer[i] = (i % 2 == 0) ? '5' : 'a';  // Alternating pattern
    }

    for (auto _ : state) {
        uint64_t result = SIMDTypeDetector::classify_digits(buffer.data(), len);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * len);
}
BENCHMARK(BM_SIMDTypeDetector_ClassifyDigits)
    ->Arg(8)
    ->Arg(16)
    ->Arg(32)
    ->Arg(64);

// Comparison: all_digits with varying input patterns
static void BM_SIMDTypeDetector_AllDigits_FailFast(benchmark::State& state) {
    size_t len = static_cast<size_t>(state.range(0));

    // Create a buffer where the first character is not a digit
    std::vector<uint8_t> buffer(len + 64, '5');
    buffer[0] = 'x';  // Non-digit at start - should fail fast

    for (auto _ : state) {
        bool result = SIMDTypeDetector::all_digits(buffer.data(), len);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * len);
}
BENCHMARK(BM_SIMDTypeDetector_AllDigits_FailFast)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024);

static void BM_SIMDTypeDetector_AllDigits_FailLate(benchmark::State& state) {
    size_t len = static_cast<size_t>(state.range(0));

    // Create a buffer where the last character is not a digit
    std::vector<uint8_t> buffer(len + 64, '5');
    buffer[len - 1] = 'x';  // Non-digit at end - must check all data

    for (auto _ : state) {
        bool result = SIMDTypeDetector::all_digits(buffer.data(), len);
        benchmark::DoNotOptimize(result);
    }

    state.SetBytesProcessed(state.iterations() * len);
}
BENCHMARK(BM_SIMDTypeDetector_AllDigits_FailLate)
    ->Arg(16)
    ->Arg(64)
    ->Arg(256)
    ->Arg(1024);

// =============================================================================
// Column Parsing Benchmarks
// =============================================================================

static void BM_ScalarParseIntColumn(benchmark::State& state) {
    init_test_data();
    ExtractionConfig config;

    std::vector<const char*> ptrs;
    std::vector<size_t> lengths;
    for (const auto& s : g_small_integers) {
        ptrs.push_back(s.c_str());
        lengths.push_back(s.size());
    }

    std::vector<std::optional<int64_t>> results(NUM_VALUES);

    for (auto _ : state) {
        for (size_t i = 0; i < NUM_VALUES; ++i) {
            auto result = parse_integer<int64_t>(ptrs[i], lengths[i], config);
            results[i] = result.value;
        }
        benchmark::DoNotOptimize(results.data());
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_ScalarParseIntColumn);

static void BM_SIMDParseIntColumn(benchmark::State& state) {
    init_test_data();

    std::vector<const char*> ptrs;
    std::vector<size_t> lengths;
    for (const auto& s : g_small_integers) {
        ptrs.push_back(s.c_str());
        lengths.push_back(s.size());
    }

    std::vector<int64_t> results(NUM_VALUES);
    // Allocate a proper bool array to avoid type confusion with std::vector<bool>
    std::unique_ptr<bool[]> valid(new bool[NUM_VALUES]);

    for (auto _ : state) {
        SIMDIntegerParser::parse_int64_column(ptrs.data(), lengths.data(), NUM_VALUES,
                                               results.data(), valid.get());
        benchmark::DoNotOptimize(results.data());
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_SIMDParseIntColumn);

static void BM_ScalarParseDoubleColumn(benchmark::State& state) {
    init_test_data();
    ExtractionConfig config;

    std::vector<const char*> ptrs;
    std::vector<size_t> lengths;
    for (const auto& s : g_floats) {
        ptrs.push_back(s.c_str());
        lengths.push_back(s.size());
    }

    std::vector<std::optional<double>> results(NUM_VALUES);

    for (auto _ : state) {
        for (size_t i = 0; i < NUM_VALUES; ++i) {
            auto result = parse_double(ptrs[i], lengths[i], config);
            results[i] = result.value;
        }
        benchmark::DoNotOptimize(results.data());
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_ScalarParseDoubleColumn);

static void BM_SIMDParseDoubleColumn(benchmark::State& state) {
    init_test_data();

    std::vector<const char*> ptrs;
    std::vector<size_t> lengths;
    for (const auto& s : g_floats) {
        ptrs.push_back(s.c_str());
        lengths.push_back(s.size());
    }

    std::vector<double> results(NUM_VALUES);
    // Allocate a proper bool array to avoid type confusion with std::vector<bool>
    std::unique_ptr<bool[]> valid(new bool[NUM_VALUES]);

    for (auto _ : state) {
        SIMDDoubleParser::parse_double_column(ptrs.data(), lengths.data(), NUM_VALUES,
                                               results.data(), valid.get());
        benchmark::DoNotOptimize(results.data());
    }

    state.SetItemsProcessed(state.iterations() * NUM_VALUES);
}
BENCHMARK(BM_SIMDParseDoubleColumn);

// Main function is provided by benchmark_main in the main benchmark executable
