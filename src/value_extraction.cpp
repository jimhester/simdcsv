#include "value_extraction.h"
#include "two_pass.h"
#include <algorithm>
#include <cassert>
#include <stdexcept>

namespace simdcsv {

ValueExtractor::ValueExtractor(const uint8_t* buf, size_t len, const index& idx,
                               const Dialect& dialect, const ExtractionConfig& config)
    : buf_(buf), len_(len), idx_(idx), dialect_(dialect), config_(config) {
    uint64_t total_indexes = 0;
    for (uint8_t i = 0; i < idx_.n_threads; ++i) total_indexes += idx_.n_indexes[i];
    linear_indexes_.reserve(total_indexes);
    for (uint8_t t = 0; t < idx_.n_threads; ++t)
        for (uint64_t j = 0; j < idx_.n_indexes[t]; ++j)
            linear_indexes_.push_back(idx_.indexes[t + (j * idx_.n_threads)]);
    std::sort(linear_indexes_.begin(), linear_indexes_.end());
    size_t first_nl = 0;
    for (size_t i = 0; i < linear_indexes_.size(); ++i) {
        if (linear_indexes_[i] >= len_) continue;  // Bounds check
        if (buf_[linear_indexes_[i]] == '\n') { first_nl = i; break; }
    }
    num_columns_ = first_nl + 1;
    recalculate_num_rows();
}

std::string_view ValueExtractor::get_string_view(size_t row, size_t col) const {
    if (row >= num_rows_) throw std::out_of_range("Row index out of range");
    if (col >= num_columns_) throw std::out_of_range("Column index out of range");
    return get_string_view_internal(row, col);
}

std::string_view ValueExtractor::get_string_view_internal(size_t row, size_t col) const {
    size_t field_idx = compute_field_index(row, col);
    if (field_idx >= linear_indexes_.size()) return std::string_view();  // Bounds check
    size_t start = (field_idx == 0) ? 0 : linear_indexes_[field_idx - 1] + 1;
    size_t end = linear_indexes_[field_idx];
    if (end > len_) end = len_;  // Bounds check
    if (start > len_) start = len_;  // Bounds check
    if (end > start && buf_[end - 1] == '\r') --end;
    if (end > start && buf_[start] == static_cast<uint8_t>(dialect_.quote_char))
        if (buf_[end - 1] == static_cast<uint8_t>(dialect_.quote_char)) { ++start; --end; }
    if (end < start) end = start;
    assert(end >= start && "Invalid range: end must be >= start");
    return std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start);
}

std::string ValueExtractor::get_string(size_t row, size_t col) const {
    size_t field_idx = compute_field_index(row, col);
    if (field_idx >= linear_indexes_.size()) return std::string();  // Bounds check
    size_t start = (field_idx == 0) ? 0 : linear_indexes_[field_idx - 1] + 1;
    size_t end = linear_indexes_[field_idx];
    if (end > len_) end = len_;  // Bounds check
    if (start > len_) start = len_;  // Bounds check
    if (end > start && buf_[end - 1] == '\r') --end;
    if (end < start) end = start;  // Normalize range
    assert(end >= start && "Invalid range: end must be >= start");
    return unescape_field(std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start));
}

size_t ValueExtractor::compute_field_index(size_t row, size_t col) const {
    return (has_header_ ? row + 1 : row) * num_columns_ + col;
}

std::string ValueExtractor::unescape_field(std::string_view field) const {
    if (field.empty() || field.front() != dialect_.quote_char) return std::string(field);
    if (field.size() < 2 || field.back() != dialect_.quote_char) return std::string(field);
    std::string_view inner = field.substr(1, field.size() - 2);
    std::string result; result.reserve(inner.size());
    for (size_t i = 0; i < inner.size(); ++i) {
        char c = inner[i];
        if (c == dialect_.escape_char && i + 1 < inner.size() && inner[i + 1] == dialect_.quote_char) {
            result += dialect_.quote_char; ++i;
        } else result += c;
    }
    return result;
}

std::vector<std::string_view> ValueExtractor::extract_column_string_view(size_t col) const {
    if (col >= num_columns_) throw std::out_of_range("Column index out of range");
    std::vector<std::string_view> result; result.reserve(num_rows_);
    for (size_t row = 0; row < num_rows_; ++row) result.push_back(get_string_view_internal(row, col));
    return result;
}

std::vector<std::string> ValueExtractor::extract_column_string(size_t col) const {
    if (col >= num_columns_) throw std::out_of_range("Column index out of range");
    std::vector<std::string> result; result.reserve(num_rows_);
    for (size_t row = 0; row < num_rows_; ++row) result.push_back(get_string(row, col));
    return result;
}

std::vector<std::string> ValueExtractor::get_header() const {
    if (!has_header_) throw std::runtime_error("CSV has no header row");
    std::vector<std::string> headers; headers.reserve(num_columns_);
    for (size_t col = 0; col < num_columns_; ++col) {
        if (col >= linear_indexes_.size()) break;  // Bounds check
        size_t start = (col == 0) ? 0 : linear_indexes_[col - 1] + 1;
        size_t end = linear_indexes_[col];
        if (end > len_) end = len_;  // Bounds check
        if (start > len_) start = len_;  // Bounds check
        if (end > start && buf_[end - 1] == '\r') --end;
        if (end < start) end = start;  // Normalize range
        assert(end >= start && "Invalid range: end must be >= start");
        headers.push_back(unescape_field(std::string_view(reinterpret_cast<const char*>(buf_ + start), end - start)));
    }
    return headers;
}

bool ValueExtractor::get_field_bounds(size_t row, size_t col, size_t& start, size_t& end) const {
    if (row >= num_rows_ || col >= num_columns_) return false;
    size_t field_idx = compute_field_index(row, col);
    start = (field_idx == 0) ? 0 : linear_indexes_[field_idx - 1] + 1;
    end = linear_indexes_[field_idx];
    return true;
}

}  // namespace simdcsv
