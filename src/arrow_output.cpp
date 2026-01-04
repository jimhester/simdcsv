#ifdef SIMDCSV_ENABLE_ARROW

#include "arrow_output.h"
#include "io_util.h"
#include "mem_util.h"
#include <arrow/builder.h>
#include <arrow/table.h>
#include <algorithm>
#include <charconv>
#include <cctype>
#include <cerrno>
#include <cmath>
#include <cstring>
#include <memory>

namespace simdcsv {

// RAII wrapper for aligned memory to ensure proper cleanup on all code paths
// This prevents memory leaks when exceptions are thrown during parsing or conversion
struct AlignedDeleter {
    void operator()(uint8_t* ptr) const noexcept {
        if (ptr) aligned_free(ptr);
    }
};
using AlignedBufferPtr = std::unique_ptr<uint8_t, AlignedDeleter>;

std::shared_ptr<arrow::DataType> column_type_to_arrow(ColumnType type) {
    switch (type) {
        case ColumnType::STRING: return arrow::utf8();
        case ColumnType::INT64: return arrow::int64();
        case ColumnType::DOUBLE: return arrow::float64();
        case ColumnType::BOOLEAN: return arrow::boolean();
        case ColumnType::DATE: return arrow::date32();
        case ColumnType::TIMESTAMP: return arrow::timestamp(arrow::TimeUnit::MICRO);
        case ColumnType::NULL_TYPE: return arrow::null();
        default: return arrow::utf8();
    }
}

const char* column_type_to_string(ColumnType type) {
    switch (type) {
        case ColumnType::STRING: return "STRING";
        case ColumnType::INT64: return "INT64";
        case ColumnType::DOUBLE: return "DOUBLE";
        case ColumnType::BOOLEAN: return "BOOLEAN";
        case ColumnType::DATE: return "DATE";
        case ColumnType::TIMESTAMP: return "TIMESTAMP";
        case ColumnType::NULL_TYPE: return "NULL";
        case ColumnType::AUTO: return "AUTO";
        default: return "UNKNOWN";
    }
}

namespace {
// Case-insensitive string comparison for ASCII
bool iequals(std::string_view a, std::string_view b) {
    if (a.size() != b.size()) return false;
    for (size_t i = 0; i < a.size(); ++i) {
        if (std::tolower(static_cast<unsigned char>(a[i])) !=
            std::tolower(static_cast<unsigned char>(b[i]))) return false;
    }
    return true;
}
}  // anonymous namespace

ArrowConverter::ArrowConverter() : options_(), has_user_schema_(false) {}
ArrowConverter::ArrowConverter(const ArrowConvertOptions& options) : options_(options), has_user_schema_(false) {}
ArrowConverter::ArrowConverter(const std::vector<ColumnSpec>& columns, const ArrowConvertOptions& options)
    : options_(options), columns_(columns), has_user_schema_(true) {}

bool ArrowConverter::is_null_value(std::string_view value) {
    for (const auto& null_str : options_.null_values) {
        if (value == null_str) return true;
    }
    return false;
}

std::optional<bool> ArrowConverter::parse_boolean(std::string_view value) {
    // Use case-insensitive comparison without allocating temporary strings
    for (const auto& v : options_.true_values) {
        if (iequals(value, v)) return true;
    }
    for (const auto& v : options_.false_values) {
        if (iequals(value, v)) return false;
    }
    return std::nullopt;
}

std::optional<int64_t> ArrowConverter::parse_int64(std::string_view value) {
    if (value.empty()) return std::nullopt;
    size_t start = 0, end = value.size();
    while (start < end && std::isspace(static_cast<unsigned char>(value[start]))) ++start;
    while (end > start && std::isspace(static_cast<unsigned char>(value[end-1]))) --end;
    if (start >= end) return std::nullopt;
    int64_t result;
    auto [ptr, ec] = std::from_chars(value.data()+start, value.data()+end, result);
    if (ec == std::errc() && ptr == value.data()+end) return result;
    return std::nullopt;
}

std::optional<double> ArrowConverter::parse_double(std::string_view value) {
    if (value.empty()) return std::nullopt;
    size_t start = 0, end = value.size();
    while (start < end && std::isspace(static_cast<unsigned char>(value[start]))) ++start;
    while (end > start && std::isspace(static_cast<unsigned char>(value[end-1]))) --end;
    if (start >= end) return std::nullopt;
    std::string_view trimmed = value.substr(start, end-start);
    if (trimmed == "inf" || trimmed == "Inf") return std::numeric_limits<double>::infinity();
    if (trimmed == "-inf" || trimmed == "-Inf") return -std::numeric_limits<double>::infinity();
    if (trimmed == "nan" || trimmed == "NaN") return std::numeric_limits<double>::quiet_NaN();
    char* endptr;
    std::string temp(trimmed);
    errno = 0;  // Reset errno before call
    double result = std::strtod(temp.c_str(), &endptr);
    // Check for parsing success and overflow/underflow (ERANGE)
    if (endptr == temp.c_str() + temp.size() && errno != ERANGE) return result;
    return std::nullopt;
}

ColumnType ArrowConverter::infer_cell_type(std::string_view cell) {
    if (cell.empty() || is_null_value(cell)) return ColumnType::NULL_TYPE;
    if (parse_boolean(cell).has_value()) return ColumnType::BOOLEAN;
    if (parse_int64(cell).has_value()) return ColumnType::INT64;
    if (parse_double(cell).has_value()) return ColumnType::DOUBLE;
    return ColumnType::STRING;
}

std::string_view ArrowConverter::extract_field(const uint8_t* buf, size_t start, size_t end, const Dialect& dialect) {
    if (start >= end) return std::string_view();
    const char* field_start = reinterpret_cast<const char*>(buf + start);
    size_t len = end - start;
    if (len >= 2 && field_start[0] == dialect.quote_char && field_start[len-1] == dialect.quote_char) {
        field_start++; len -= 2;
    }
    return std::string_view(field_start, len);
}

std::vector<std::vector<ArrowConverter::FieldRange>> ArrowConverter::extract_field_ranges(
    const uint8_t* buf, size_t len, const index& idx, const Dialect& dialect) {
    std::vector<std::vector<FieldRange>> columns;
    if (idx.n_threads == 0) return columns;
    size_t total_seps = 0;
    for (uint8_t t = 0; t < idx.n_threads; ++t) total_seps += idx.n_indexes[t];
    if (total_seps == 0) return columns;
    std::vector<uint64_t> all_positions;
    all_positions.reserve(total_seps);
    for (uint8_t t = 0; t < idx.n_threads; ++t)
        for (size_t i = 0; i < idx.n_indexes[t]; ++i) {
            uint64_t pos = idx.indexes[t + i * idx.n_threads];
            if (pos < len) all_positions.push_back(pos);  // Bounds check to prevent buffer overrun
        }
    std::sort(all_positions.begin(), all_positions.end());
    if (all_positions.empty()) return columns;  // No valid positions after bounds filtering
    size_t num_columns = 0;
    for (size_t i = 0; i < all_positions.size(); ++i) {
        num_columns++;
        if (buf[all_positions[i]] == '\n') break;
    }
    if (num_columns == 0) return columns;
    columns.resize(num_columns);
    size_t field_start = 0, current_col = 0;
    bool skip_header = true;
    for (size_t i = 0; i < all_positions.size(); ++i) {
        size_t field_end = all_positions[i];
        char sep_char = static_cast<char>(buf[field_end]);
        if (!skip_header && current_col < num_columns)
            columns[current_col].push_back({field_start, field_end});
        if (sep_char == '\n') { if (skip_header) skip_header = false; current_col = 0; }
        else current_col++;
        field_start = field_end + 1;
    }
    return columns;
}

std::vector<ColumnType> ArrowConverter::infer_types(const uint8_t* buf, size_t len, const index& idx, const Dialect& dialect) {
    auto field_ranges = extract_field_ranges(buf, len, idx, dialect);
    std::vector<ColumnType> types(field_ranges.size(), ColumnType::NULL_TYPE);
    for (size_t col = 0; col < field_ranges.size(); ++col) {
        const auto& ranges = field_ranges[col];
        size_t samples = options_.type_inference_rows > 0 ? std::min(options_.type_inference_rows, ranges.size()) : ranges.size();
        ColumnType strongest = ColumnType::NULL_TYPE;
        for (size_t row = 0; row < samples; ++row) {
            ColumnType ct = infer_cell_type(extract_field(buf, ranges[row].start, ranges[row].end, dialect));
            if (ct == ColumnType::NULL_TYPE) continue;
            if (strongest == ColumnType::NULL_TYPE) strongest = ct;
            else if (strongest != ct) {
                if ((strongest == ColumnType::INT64 && ct == ColumnType::DOUBLE) ||
                    (strongest == ColumnType::DOUBLE && ct == ColumnType::INT64)) strongest = ColumnType::DOUBLE;
                else { strongest = ColumnType::STRING; break; }
            }
        }
        types[col] = (strongest == ColumnType::NULL_TYPE) ? ColumnType::STRING : strongest;
    }
    return types;
}

std::shared_ptr<arrow::Schema> ArrowConverter::build_schema(const std::vector<std::string>& names, const std::vector<ColumnType>& types) {
    std::vector<std::shared_ptr<arrow::Field>> fields;
    for (size_t i = 0; i < names.size(); ++i) {
        auto arrow_type = (has_user_schema_ && i < columns_.size() && columns_[i].arrow_type) ? columns_[i].arrow_type : (i < types.size() ? column_type_to_arrow(types[i]) : arrow::utf8());
        bool nullable = has_user_schema_ && i < columns_.size() ? columns_[i].nullable : true;
        fields.push_back(arrow::field(names[i], arrow_type, nullable));
    }
    return arrow::schema(fields);
}

arrow::Result<std::shared_ptr<arrow::Array>> ArrowConverter::build_string_column(const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect) {
    arrow::StringBuilder builder(options_.memory_pool);
    ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
    for (const auto& range : ranges) {
        auto cell = extract_field(buf, range.start, range.end, dialect);
        if (is_null_value(cell)) ARROW_RETURN_NOT_OK(builder.AppendNull());
        else ARROW_RETURN_NOT_OK(builder.Append(std::string(cell)));
    }
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>> ArrowConverter::build_int64_column(const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect) {
    arrow::Int64Builder builder(options_.memory_pool);
    ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
    for (const auto& range : ranges) {
        auto cell = extract_field(buf, range.start, range.end, dialect);
        if (is_null_value(cell)) ARROW_RETURN_NOT_OK(builder.AppendNull());
        else if (auto v = parse_int64(cell)) ARROW_RETURN_NOT_OK(builder.Append(*v));
        else ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>> ArrowConverter::build_double_column(const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect) {
    arrow::DoubleBuilder builder(options_.memory_pool);
    ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
    for (const auto& range : ranges) {
        auto cell = extract_field(buf, range.start, range.end, dialect);
        if (is_null_value(cell)) ARROW_RETURN_NOT_OK(builder.AppendNull());
        else if (auto v = parse_double(cell)) ARROW_RETURN_NOT_OK(builder.Append(*v));
        else ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>> ArrowConverter::build_boolean_column(const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect) {
    arrow::BooleanBuilder builder(options_.memory_pool);
    ARROW_RETURN_NOT_OK(builder.Reserve(static_cast<int64_t>(ranges.size())));
    for (const auto& range : ranges) {
        auto cell = extract_field(buf, range.start, range.end, dialect);
        if (is_null_value(cell)) ARROW_RETURN_NOT_OK(builder.AppendNull());
        else if (auto v = parse_boolean(cell)) ARROW_RETURN_NOT_OK(builder.Append(*v));
        else ARROW_RETURN_NOT_OK(builder.AppendNull());
    }
    return builder.Finish();
}

arrow::Result<std::shared_ptr<arrow::Array>> ArrowConverter::build_column(const uint8_t* buf, const std::vector<FieldRange>& ranges, ColumnType type, const Dialect& dialect) {
    switch (type) {
        case ColumnType::INT64: return build_int64_column(buf, ranges, dialect);
        case ColumnType::DOUBLE: return build_double_column(buf, ranges, dialect);
        case ColumnType::BOOLEAN: return build_boolean_column(buf, ranges, dialect);
        default: return build_string_column(buf, ranges, dialect);
    }
}

ArrowConvertResult ArrowConverter::convert(const uint8_t* buf, size_t len, const index& idx, const Dialect& dialect) {
    ArrowConvertResult result;
    auto field_ranges = extract_field_ranges(buf, len, idx, dialect);
    if (field_ranges.empty()) { result.error_message = "No data"; return result; }
    size_t num_columns = field_ranges.size(), num_rows = field_ranges[0].size();

    // Extract column names from header
    std::vector<std::string> column_names;
    size_t total_seps = 0;
    for (uint8_t t = 0; t < idx.n_threads; ++t) total_seps += idx.n_indexes[t];
    if (total_seps > 0) {
        std::vector<uint64_t> all_positions;
        for (uint8_t t = 0; t < idx.n_threads; ++t)
            for (size_t i = 0; i < idx.n_indexes[t]; ++i) {
                uint64_t pos = idx.indexes[t + i * idx.n_threads];
                if (pos < len) all_positions.push_back(pos);  // Bounds check
            }
        std::sort(all_positions.begin(), all_positions.end());
        size_t fs = 0;
        for (size_t i = 0; i < all_positions.size() && column_names.size() < num_columns; ++i) {
            column_names.push_back(std::string(extract_field(buf, fs, all_positions[i], dialect)));
            fs = all_positions[i] + 1;
            if (buf[all_positions[i]] == '\n') break;
        }
    }
    while (column_names.size() < num_columns) column_names.push_back("column_" + std::to_string(column_names.size()));

    // Get column types
    auto column_types = options_.infer_types ? infer_types(buf, len, idx, dialect) : std::vector<ColumnType>(num_columns, ColumnType::STRING);
    result.schema = build_schema(column_names, column_types);

    // Build columns
    std::vector<std::shared_ptr<arrow::Array>> arrays;
    for (size_t col = 0; col < num_columns; ++col) {
        auto arr = build_column(buf, field_ranges[col], column_types[col], dialect);
        if (!arr.ok()) { result.error_message = arr.status().ToString(); return result; }
        arrays.push_back(*arr);
    }
    result.table = arrow::Table::Make(result.schema, arrays);
    result.num_rows = static_cast<int64_t>(num_rows);
    result.num_columns = static_cast<int64_t>(num_columns);
    return result;
}

ArrowConvertResult csv_to_arrow(const std::string& filename, const ArrowConvertOptions& options, const Dialect& dialect) {
    ArrowConvertResult result;
    try {
        auto corpus = get_corpus(filename, 64);
        // Use RAII to ensure memory is freed even if an exception is thrown
        // during parsing or conversion. The buffer will be automatically freed
        // when buffer_guard goes out of scope (either normally or via exception).
        AlignedBufferPtr buffer_guard(const_cast<uint8_t*>(corpus.data()));

        two_pass parser;
        index idx = parser.init(corpus.size(), 1);
        parser.parse(corpus.data(), idx, corpus.size(), dialect);
        ArrowConverter converter(options);
        result = converter.convert(corpus.data(), corpus.size(), idx, dialect);
        // buffer_guard automatically frees memory when it goes out of scope
    } catch (const std::exception& e) {
        result.error_message = e.what();
    }
    return result;
}

ArrowConvertResult csv_to_arrow_from_memory(const uint8_t* data, size_t len, const ArrowConvertOptions& options, const Dialect& dialect) {
    ArrowConvertResult result;
    uint8_t* buf = allocate_padded_buffer(len, 64);
    if (!buf) {
        result.error_message = "Allocation failed";
        return result;
    }
    // Use RAII to ensure memory is freed even if an exception is thrown
    AlignedBufferPtr buffer_guard(buf);

    try {
        std::memcpy(buf, data, len);
        two_pass parser;
        index idx = parser.init(len, 1);
        parser.parse(buf, idx, len, dialect);
        ArrowConverter converter(options);
        result = converter.convert(buf, len, idx, dialect);
        // buffer_guard automatically frees memory when it goes out of scope
    } catch (const std::exception& e) {
        result.error_message = e.what();
    }
    return result;
}

}  // namespace simdcsv

#endif  // SIMDCSV_ENABLE_ARROW
