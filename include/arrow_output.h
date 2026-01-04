/**
 * @file arrow_output.h
 * @brief Apache Arrow output integration for simdcsv.
 *
 * This header provides functionality to convert parsed CSV data into Apache Arrow
 * format (Arrays and Tables). Arrow integration is optional and requires building
 * with -DSIMDCSV_ENABLE_ARROW=ON.
 *
 * @note This header is only available when compiled with SIMDCSV_ENABLE_ARROW=ON
 */

#ifndef SIMDCSV_ARROW_OUTPUT_H
#define SIMDCSV_ARROW_OUTPUT_H

#ifdef SIMDCSV_ENABLE_ARROW

#include <arrow/api.h>
#include <arrow/builder.h>
#include <memory>
#include <string>
#include <string_view>
#include <vector>
#include <optional>

#include "two_pass.h"
#include "dialect.h"

namespace simdcsv {

enum class ColumnType {
    STRING, INT64, DOUBLE, BOOLEAN, DATE, TIMESTAMP, NULL_TYPE, AUTO
};

std::shared_ptr<arrow::DataType> column_type_to_arrow(ColumnType type);
const char* column_type_to_string(ColumnType type);

struct ColumnSpec {
    std::string name;
    ColumnType type = ColumnType::AUTO;
    std::shared_ptr<arrow::DataType> arrow_type = nullptr;
    bool nullable = true;

    ColumnSpec() = default;
    ColumnSpec(std::string name_, ColumnType type_ = ColumnType::AUTO)
        : name(std::move(name_)), type(type_) {}
};

struct ArrowConvertOptions {
    bool infer_types = true;
    size_t type_inference_rows = 1000;
    bool empty_is_null = false;
    std::vector<std::string> null_values = {"", "NA", "N/A", "null", "NULL", "None", "NaN"};
    std::vector<std::string> true_values = {"true", "True", "TRUE", "1", "yes", "Yes", "YES"};
    std::vector<std::string> false_values = {"false", "False", "FALSE", "0", "no", "No", "NO"};
    arrow::MemoryPool* memory_pool = nullptr;
};

struct ArrowConvertResult {
    std::shared_ptr<arrow::Table> table;
    std::string error_message;
    int64_t num_rows = 0;
    int64_t num_columns = 0;
    std::shared_ptr<arrow::Schema> schema;
    bool ok() const { return table != nullptr; }
};

class ArrowConverter {
public:
    ArrowConverter();
    explicit ArrowConverter(const ArrowConvertOptions& options);
    ArrowConverter(const std::vector<ColumnSpec>& columns,
                   const ArrowConvertOptions& options = ArrowConvertOptions());

    ArrowConvertResult convert(const uint8_t* buf, size_t len, const index& idx,
                               const Dialect& dialect = Dialect::csv());

    std::vector<ColumnType> infer_types(const uint8_t* buf, size_t len, const index& idx,
                                        const Dialect& dialect = Dialect::csv());

    std::shared_ptr<arrow::Schema> build_schema(const std::vector<std::string>& column_names,
                                                const std::vector<ColumnType>& column_types);

private:
    ArrowConvertOptions options_;
    std::vector<ColumnSpec> columns_;
    bool has_user_schema_ = false;

    struct FieldRange { size_t start; size_t end; };

    std::vector<std::vector<FieldRange>> extract_field_ranges(
        const uint8_t* buf, size_t len, const index& idx, const Dialect& dialect);

    /**
     * @brief Extract a field from the buffer as a string_view.
     * @param buf Pointer to the CSV buffer
     * @param start Starting byte offset of the field (inclusive)
     * @param end Ending byte offset of the field (exclusive)
     * @param dialect CSV dialect settings
     * @return A string_view of the field contents, with quotes stripped if present.
     *         Returns empty string_view if start >= end.
     * @pre end >= start (asserted in debug builds to catch corrupted index data)
     */
    std::string_view extract_field(const uint8_t* buf, size_t start, size_t end, const Dialect& dialect);
    ColumnType infer_cell_type(std::string_view cell);
    bool is_null_value(std::string_view value);
    std::optional<bool> parse_boolean(std::string_view value);
    std::optional<int64_t> parse_int64(std::string_view value);
    std::optional<double> parse_double(std::string_view value);

    arrow::Result<std::shared_ptr<arrow::Array>> build_column(
        const uint8_t* buf, const std::vector<FieldRange>& ranges, ColumnType type, const Dialect& dialect);
    arrow::Result<std::shared_ptr<arrow::Array>> build_string_column(
        const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect);
    arrow::Result<std::shared_ptr<arrow::Array>> build_int64_column(
        const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect);
    arrow::Result<std::shared_ptr<arrow::Array>> build_double_column(
        const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect);
    arrow::Result<std::shared_ptr<arrow::Array>> build_boolean_column(
        const uint8_t* buf, const std::vector<FieldRange>& ranges, const Dialect& dialect);
};

ArrowConvertResult csv_to_arrow(const std::string& filename,
                                const ArrowConvertOptions& options = ArrowConvertOptions(),
                                const Dialect& dialect = Dialect::csv());

ArrowConvertResult csv_to_arrow_from_memory(const uint8_t* data, size_t len,
                                            const ArrowConvertOptions& options = ArrowConvertOptions(),
                                            const Dialect& dialect = Dialect::csv());

}  // namespace simdcsv

#endif  // SIMDCSV_ENABLE_ARROW
#endif  // SIMDCSV_ARROW_OUTPUT_H
