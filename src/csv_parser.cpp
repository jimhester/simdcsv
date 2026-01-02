#include "csv_parser.h"
#include <algorithm>
#include <sstream>

namespace simdcsv {

std::string CSVParser::getContext(const uint8_t* buf, size_t len, size_t pos, size_t context_size) {
    size_t start = pos > context_size ? pos - context_size : 0;
    size_t end = std::min(pos + context_size, len);

    std::string ctx;
    for (size_t i = start; i < end; ++i) {
        char c = static_cast<char>(buf[i]);
        if (c == '\n') {
            ctx += "\\n";
        } else if (c == '\r') {
            ctx += "\\r";
        } else if (c == '\0') {
            ctx += "\\0";
        } else if (c >= 32 && c < 127) {
            ctx += c;
        } else {
            ctx += "?";
        }
    }
    return ctx;
}

void CSVParser::getLineColumn(const uint8_t* buf, size_t offset, size_t& line, size_t& column) {
    line = 1;
    column = 1;
    for (size_t i = 0; i < offset && i < offset; ++i) {
        if (buf[i] == '\n') {
            ++line;
            column = 1;
        } else if (buf[i] != '\r') {  // Don't count CR as column
            ++column;
        }
    }
}

LineEnding CSVParser::detectLineEnding(const uint8_t* buf, size_t len, size_t pos) {
    if (pos < len && buf[pos] == '\r') {
        if (pos + 1 < len && buf[pos + 1] == '\n') {
            return LineEnding::CRLF;
        }
        return LineEnding::CR;
    }
    if (pos < len && buf[pos] == '\n') {
        return LineEnding::LF;
    }
    return LineEnding::UNKNOWN;
}

void CSVParser::checkDuplicateColumns() {
    std::unordered_set<std::string> seen;
    for (size_t i = 0; i < header_fields_.size(); ++i) {
        const auto& name = header_fields_[i];
        if (seen.count(name) > 0) {
            errors_.add_error(
                ErrorCode::DUPLICATE_COLUMN_NAMES,
                ErrorSeverity::WARNING,
                1, i + 1, 0,
                "Duplicate column name: '" + name + "'",
                name
            );
        }
        seen.insert(name);
    }
}

bool CSVParser::parseHeader(const uint8_t* buf, size_t len, size_t& pos) {
    // Check for empty header
    if (len == 0 || buf[0] == '\n' || buf[0] == '\r') {
        errors_.add_error(
            ErrorCode::EMPTY_HEADER,
            ErrorSeverity::ERROR,
            1, 1, 0,
            "Header row is empty",
            len > 0 ? getContext(buf, len, 0, 10) : ""
        );
        // Skip the empty line
        if (pos < len && buf[pos] == '\r') ++pos;
        if (pos < len && buf[pos] == '\n') ++pos;
        return false;
    }

    // Parse header fields
    std::string current_field;
    State state = State::FIELD_START;
    size_t field_start = 0;

    while (pos < len) {
        uint8_t c = buf[pos];

        if (c == '\0') {
            size_t line, col;
            getLineColumn(buf, pos, line, col);
            errors_.add_error(
                ErrorCode::NULL_BYTE,
                ErrorSeverity::ERROR,
                line, col, pos,
                "Null byte in header",
                getContext(buf, len, pos)
            );
            if (errors_.should_stop()) return false;
            ++pos;
            continue;
        }

        switch (state) {
            case State::RECORD_START:
            case State::FIELD_START:
                if (c == config_.quote) {
                    state = State::QUOTED_FIELD;
                    field_start = pos + 1;
                } else if (c == config_.delimiter) {
                    header_fields_.push_back("");
                    state = State::FIELD_START;
                } else if (c == '\n' || c == '\r') {
                    header_fields_.push_back(current_field);
                    current_field.clear();
                    // Handle CRLF
                    if (c == '\r' && pos + 1 < len && buf[pos + 1] == '\n') {
                        ++pos;
                    }
                    ++pos;
                    expected_field_count_ = header_fields_.size();
                    checkDuplicateColumns();
                    return true;
                } else {
                    state = State::UNQUOTED_FIELD;
                    current_field += static_cast<char>(c);
                }
                break;

            case State::UNQUOTED_FIELD:
                if (c == config_.quote) {
                    size_t line, col;
                    getLineColumn(buf, pos, line, col);
                    errors_.add_error(
                        ErrorCode::QUOTE_IN_UNQUOTED_FIELD,
                        ErrorSeverity::ERROR,
                        line, col, pos,
                        "Quote character in unquoted field",
                        getContext(buf, len, pos)
                    );
                    if (errors_.should_stop()) return false;
                    current_field += static_cast<char>(c);
                } else if (c == config_.delimiter) {
                    header_fields_.push_back(current_field);
                    current_field.clear();
                    state = State::FIELD_START;
                } else if (c == '\n' || c == '\r') {
                    header_fields_.push_back(current_field);
                    current_field.clear();
                    if (c == '\r' && pos + 1 < len && buf[pos + 1] == '\n') {
                        ++pos;
                    }
                    ++pos;
                    expected_field_count_ = header_fields_.size();
                    checkDuplicateColumns();
                    return true;
                } else {
                    current_field += static_cast<char>(c);
                }
                break;

            case State::QUOTED_FIELD:
                if (c == config_.quote) {
                    state = State::QUOTED_END;
                } else {
                    current_field += static_cast<char>(c);
                }
                break;

            case State::QUOTED_END:
                if (c == config_.quote) {
                    // Escaped quote
                    current_field += config_.quote;
                    state = State::QUOTED_FIELD;
                } else if (c == config_.delimiter) {
                    header_fields_.push_back(current_field);
                    current_field.clear();
                    state = State::FIELD_START;
                } else if (c == '\n' || c == '\r') {
                    header_fields_.push_back(current_field);
                    current_field.clear();
                    if (c == '\r' && pos + 1 < len && buf[pos + 1] == '\n') {
                        ++pos;
                    }
                    ++pos;
                    expected_field_count_ = header_fields_.size();
                    checkDuplicateColumns();
                    return true;
                } else {
                    size_t line, col;
                    getLineColumn(buf, pos, line, col);
                    errors_.add_error(
                        ErrorCode::INVALID_QUOTE_ESCAPE,
                        ErrorSeverity::ERROR,
                        line, col, pos,
                        "Invalid character after closing quote",
                        getContext(buf, len, pos)
                    );
                    if (errors_.should_stop()) return false;
                    current_field += static_cast<char>(c);
                    state = State::UNQUOTED_FIELD;
                }
                break;
        }
        ++pos;
    }

    // EOF reached
    if (state == State::QUOTED_FIELD) {
        size_t line, col;
        getLineColumn(buf, pos > 0 ? pos - 1 : 0, line, col);
        errors_.add_error(
            ErrorCode::UNCLOSED_QUOTE,
            ErrorSeverity::FATAL,
            line, col, pos,
            "Unclosed quote at end of file",
            getContext(buf, len, pos > 20 ? pos - 20 : 0)
        );
        return false;
    }

    // Handle last field if no trailing newline
    if (!current_field.empty() || state == State::FIELD_START) {
        header_fields_.push_back(current_field);
    }
    expected_field_count_ = header_fields_.size();
    checkDuplicateColumns();
    return true;
}

ParseResult CSVParser::parse(const uint8_t* buf, size_t len) {
    ParseResult result;
    result.errors.set_mode(config_.error_mode);

    if (len == 0) {
        return result;
    }

    size_t pos = 0;
    size_t current_line = 1;
    LineEnding first_line_ending = LineEnding::UNKNOWN;
    bool line_ending_warning_issued = false;

    // Parse header if configured
    if (config_.has_header) {
        if (!parseHeader(buf, len, pos)) {
            if (errors_.has_fatal_errors()) {
                result.success = false;
                result.errors = errors_;
                return result;
            }
        }
        current_line = 2;
    }

    // Parse data rows
    State state = State::FIELD_START;
    std::string current_field;
    std::vector<std::string> current_row;
    size_t row_start_pos = pos;
    size_t field_start_col = 1;

    while (pos < len) {
        uint8_t c = buf[pos];

        // Check for null bytes
        if (c == '\0') {
            size_t line, col;
            getLineColumn(buf, pos, line, col);
            errors_.add_error(
                ErrorCode::NULL_BYTE,
                ErrorSeverity::ERROR,
                line, col, pos,
                "Null byte in data",
                getContext(buf, len, pos)
            );
            if (errors_.should_stop()) {
                result.success = false;
                result.errors = errors_;
                return result;
            }
            ++pos;
            continue;
        }

        switch (state) {
            case State::RECORD_START:
            case State::FIELD_START:
                field_start_col = pos - row_start_pos + 1;
                if (c == config_.quote) {
                    state = State::QUOTED_FIELD;
                } else if (c == config_.delimiter) {
                    current_row.push_back("");
                    state = State::FIELD_START;
                } else if (c == '\n' || c == '\r') {
                    // End of row
                    current_row.push_back(current_field);
                    current_field.clear();

                    // Check line ending consistency
                    LineEnding this_ending = detectLineEnding(buf, len, pos);
                    if (first_line_ending == LineEnding::UNKNOWN) {
                        first_line_ending = this_ending;
                    } else if (this_ending != first_line_ending && !line_ending_warning_issued) {
                        errors_.add_error(
                            ErrorCode::MIXED_LINE_ENDINGS,
                            ErrorSeverity::WARNING,
                            current_line, pos - row_start_pos + 1, pos,
                            "Mixed line endings detected",
                            ""
                        );
                        line_ending_warning_issued = true;
                    }

                    // Check field count
                    if (expected_field_count_ > 0 && current_row.size() != expected_field_count_) {
                        std::ostringstream msg;
                        msg << "Expected " << expected_field_count_ << " fields but found " << current_row.size();
                        errors_.add_error(
                            ErrorCode::INCONSISTENT_FIELD_COUNT,
                            ErrorSeverity::ERROR,
                            current_line, 1, row_start_pos,
                            msg.str(),
                            getContext(buf, len, row_start_pos, 40)
                        );
                        if (errors_.should_stop()) {
                            result.success = false;
                            result.errors = errors_;
                            return result;
                        }
                    }

                    result.data.push_back(std::move(current_row));
                    current_row.clear();
                    ++result.rows_parsed;

                    // Handle CRLF
                    if (c == '\r' && pos + 1 < len && buf[pos + 1] == '\n') {
                        ++pos;
                    }
                    ++current_line;
                    row_start_pos = pos + 1;
                    state = State::RECORD_START;
                } else {
                    state = State::UNQUOTED_FIELD;
                    current_field += static_cast<char>(c);
                }
                break;

            case State::UNQUOTED_FIELD:
                if (c == config_.quote) {
                    size_t line, col;
                    getLineColumn(buf, pos, line, col);
                    errors_.add_error(
                        ErrorCode::QUOTE_IN_UNQUOTED_FIELD,
                        ErrorSeverity::ERROR,
                        line, col, pos,
                        "Quote character in unquoted field",
                        getContext(buf, len, pos)
                    );
                    if (errors_.should_stop()) {
                        result.success = false;
                        result.errors = errors_;
                        return result;
                    }
                    // Continue parsing, treating quote as regular character
                    current_field += static_cast<char>(c);
                } else if (c == config_.delimiter) {
                    current_row.push_back(current_field);
                    current_field.clear();
                    state = State::FIELD_START;
                } else if (c == '\n' || c == '\r') {
                    current_row.push_back(current_field);
                    current_field.clear();

                    // Check line ending consistency
                    LineEnding this_ending = detectLineEnding(buf, len, pos);
                    if (first_line_ending == LineEnding::UNKNOWN) {
                        first_line_ending = this_ending;
                    } else if (this_ending != first_line_ending && !line_ending_warning_issued) {
                        errors_.add_error(
                            ErrorCode::MIXED_LINE_ENDINGS,
                            ErrorSeverity::WARNING,
                            current_line, pos - row_start_pos + 1, pos,
                            "Mixed line endings detected",
                            ""
                        );
                        line_ending_warning_issued = true;
                    }

                    // Check field count
                    if (expected_field_count_ > 0 && current_row.size() != expected_field_count_) {
                        std::ostringstream msg;
                        msg << "Expected " << expected_field_count_ << " fields but found " << current_row.size();
                        errors_.add_error(
                            ErrorCode::INCONSISTENT_FIELD_COUNT,
                            ErrorSeverity::ERROR,
                            current_line, 1, row_start_pos,
                            msg.str(),
                            getContext(buf, len, row_start_pos, 40)
                        );
                        if (errors_.should_stop()) {
                            result.success = false;
                            result.errors = errors_;
                            return result;
                        }
                    }

                    result.data.push_back(std::move(current_row));
                    current_row.clear();
                    ++result.rows_parsed;

                    if (c == '\r' && pos + 1 < len && buf[pos + 1] == '\n') {
                        ++pos;
                    }
                    ++current_line;
                    row_start_pos = pos + 1;
                    state = State::RECORD_START;
                } else {
                    current_field += static_cast<char>(c);
                }
                break;

            case State::QUOTED_FIELD:
                if (c == config_.quote) {
                    state = State::QUOTED_END;
                } else {
                    current_field += static_cast<char>(c);
                }
                break;

            case State::QUOTED_END:
                if (c == config_.quote) {
                    // Escaped quote ""
                    current_field += config_.quote;
                    state = State::QUOTED_FIELD;
                } else if (c == config_.delimiter) {
                    current_row.push_back(current_field);
                    current_field.clear();
                    state = State::FIELD_START;
                } else if (c == '\n' || c == '\r') {
                    current_row.push_back(current_field);
                    current_field.clear();

                    // Check line ending consistency
                    LineEnding this_ending = detectLineEnding(buf, len, pos);
                    if (first_line_ending == LineEnding::UNKNOWN) {
                        first_line_ending = this_ending;
                    } else if (this_ending != first_line_ending && !line_ending_warning_issued) {
                        errors_.add_error(
                            ErrorCode::MIXED_LINE_ENDINGS,
                            ErrorSeverity::WARNING,
                            current_line, pos - row_start_pos + 1, pos,
                            "Mixed line endings detected",
                            ""
                        );
                        line_ending_warning_issued = true;
                    }

                    // Check field count
                    if (expected_field_count_ > 0 && current_row.size() != expected_field_count_) {
                        std::ostringstream msg;
                        msg << "Expected " << expected_field_count_ << " fields but found " << current_row.size();
                        errors_.add_error(
                            ErrorCode::INCONSISTENT_FIELD_COUNT,
                            ErrorSeverity::ERROR,
                            current_line, 1, row_start_pos,
                            msg.str(),
                            getContext(buf, len, row_start_pos, 40)
                        );
                        if (errors_.should_stop()) {
                            result.success = false;
                            result.errors = errors_;
                            return result;
                        }
                    }

                    result.data.push_back(std::move(current_row));
                    current_row.clear();
                    ++result.rows_parsed;

                    if (c == '\r' && pos + 1 < len && buf[pos + 1] == '\n') {
                        ++pos;
                    }
                    ++current_line;
                    row_start_pos = pos + 1;
                    state = State::RECORD_START;
                } else {
                    // Invalid: character after closing quote that isn't delimiter or newline
                    size_t line, col;
                    getLineColumn(buf, pos, line, col);
                    errors_.add_error(
                        ErrorCode::INVALID_QUOTE_ESCAPE,
                        ErrorSeverity::ERROR,
                        line, col, pos,
                        "Invalid character after closing quote",
                        getContext(buf, len, pos)
                    );
                    if (errors_.should_stop()) {
                        result.success = false;
                        result.errors = errors_;
                        return result;
                    }
                    // Recovery: treat as continuing the field
                    current_field += static_cast<char>(c);
                    state = State::UNQUOTED_FIELD;
                }
                break;
        }

        // Check field size limit
        if (current_field.size() > config_.max_field_size) {
            size_t line, col;
            getLineColumn(buf, pos, line, col);
            errors_.add_error(
                ErrorCode::FIELD_TOO_LARGE,
                ErrorSeverity::ERROR,
                line, field_start_col, pos,
                "Field exceeds maximum size limit",
                ""
            );
            if (errors_.should_stop()) {
                result.success = false;
                result.errors = errors_;
                return result;
            }
        }

        ++pos;
    }

    // Handle EOF
    if (state == State::QUOTED_FIELD) {
        size_t line, col;
        getLineColumn(buf, pos > 0 ? pos - 1 : 0, line, col);
        errors_.add_error(
            ErrorCode::UNCLOSED_QUOTE,
            ErrorSeverity::FATAL,
            line, col, pos,
            "Unclosed quote at end of file",
            getContext(buf, len, pos > 20 ? pos - 20 : 0)
        );
        result.success = false;
    }

    // Handle last row if no trailing newline
    // Only process if we actually have data (not just header with no data rows)
    bool has_data = !current_field.empty() || !current_row.empty() ||
                    (state != State::RECORD_START && state != State::FIELD_START);
    if (has_data) {
        if (state != State::RECORD_START || !current_field.empty()) {
            current_row.push_back(current_field);
        }

        if (!current_row.empty()) {
            // Check field count for last row
            if (expected_field_count_ > 0 && current_row.size() != expected_field_count_) {
                std::ostringstream msg;
                msg << "Expected " << expected_field_count_ << " fields but found " << current_row.size();
                errors_.add_error(
                    ErrorCode::INCONSISTENT_FIELD_COUNT,
                    ErrorSeverity::ERROR,
                    current_line, 1, row_start_pos,
                    msg.str(),
                    getContext(buf, len, row_start_pos, 40)
                );
            }
            result.data.push_back(std::move(current_row));
            ++result.rows_parsed;
        }
    }

    result.errors = errors_;
    result.success = !errors_.has_fatal_errors();
    return result;
}

} // namespace simdcsv
