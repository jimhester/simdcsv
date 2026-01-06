#ifndef LIBVROOM_EXTRACTION_CONFIG_H
#define LIBVROOM_EXTRACTION_CONFIG_H

#include <cstddef>
#include <optional>
#include <stdexcept>
#include <string_view>
#include <vector>

#include "common_defs.h"

namespace libvroom {

/**
 * Result structure for value extraction operations.
 * Contains either a successfully parsed value or an error indicator.
 */
template <typename T>
struct ExtractResult {
    std::optional<T> value;
    const char* error = nullptr;

    bool ok() const { return value.has_value(); }
    bool is_na() const { return !value.has_value() && error == nullptr; }

    T get() const {
        if (!value.has_value()) {
            throw std::runtime_error(error ? error : "Value is NA");
        }
        return *value;
    }

    T get_or(T default_value) const {
        return value.value_or(default_value);
    }
};

/**
 * Configuration for value extraction behavior.
 * Controls NA detection, boolean parsing, and whitespace handling.
 */
struct ExtractionConfig {
    std::vector<std::string_view> na_values = {"", "NA", "N/A", "NaN", "null", "NULL", "None"};
    std::vector<std::string_view> true_values = {"true", "True", "TRUE", "1", "yes", "Yes", "YES", "T"};
    std::vector<std::string_view> false_values = {"false", "False", "FALSE", "0", "no", "No", "NO", "F"};
    bool trim_whitespace = true;
    bool allow_leading_zeros = true;
    size_t max_integer_digits = 20;

    static ExtractionConfig defaults() { return ExtractionConfig{}; }
};

/**
 * Parse a boolean value from a string.
 * Checks against configurable true/false/NA values.
 */
really_inline ExtractResult<bool> parse_bool(const char* str, size_t len,
                                              const ExtractionConfig& config = ExtractionConfig::defaults()) {
    if (len == 0) return {std::nullopt, nullptr};

    const char* ptr = str;
    const char* end = str + len;

    if (config.trim_whitespace) {
        while (ptr < end && (*ptr == ' ' || *ptr == '\t')) ++ptr;
        while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t')) --end;
        if (ptr == end) return {std::nullopt, nullptr};
    }

    std::string_view sv(ptr, end - ptr);
    for (const auto& tv : config.true_values) if (sv == tv) return {true, nullptr};
    for (const auto& fv : config.false_values) if (sv == fv) return {false, nullptr};
    for (const auto& na : config.na_values) if (sv == na) return {std::nullopt, nullptr};
    return {std::nullopt, "Invalid boolean value"};
}

/**
 * Check if a string value represents NA/missing.
 */
really_inline bool is_na(const char* str, size_t len, const ExtractionConfig& config = ExtractionConfig::defaults()) {
    if (len == 0) return true;
    const char* ptr = str;
    const char* end = str + len;
    if (config.trim_whitespace) {
        while (ptr < end && (*ptr == ' ' || *ptr == '\t')) ++ptr;
        while (end > ptr && (*(end - 1) == ' ' || *(end - 1) == '\t')) --end;
        if (ptr == end) return true;
    }
    std::string_view sv(ptr, end - ptr);
    for (const auto& na : config.na_values) if (sv == na) return true;
    return false;
}

}  // namespace libvroom

#endif  // LIBVROOM_EXTRACTION_CONFIG_H
