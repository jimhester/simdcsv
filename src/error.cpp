#include "error.h"
#include <sstream>

namespace simdcsv {

const char* error_code_to_string(ErrorCode code) {
    switch (code) {
        case ErrorCode::NONE: return "NONE";
        case ErrorCode::UNCLOSED_QUOTE: return "UNCLOSED_QUOTE";
        case ErrorCode::INVALID_QUOTE_ESCAPE: return "INVALID_QUOTE_ESCAPE";
        case ErrorCode::QUOTE_IN_UNQUOTED_FIELD: return "QUOTE_IN_UNQUOTED_FIELD";
        case ErrorCode::INCONSISTENT_FIELD_COUNT: return "INCONSISTENT_FIELD_COUNT";
        case ErrorCode::FIELD_TOO_LARGE: return "FIELD_TOO_LARGE";
        case ErrorCode::MIXED_LINE_ENDINGS: return "MIXED_LINE_ENDINGS";
        case ErrorCode::INVALID_LINE_ENDING: return "INVALID_LINE_ENDING";
        case ErrorCode::INVALID_UTF8: return "INVALID_UTF8";
        case ErrorCode::NULL_BYTE: return "NULL_BYTE";
        case ErrorCode::EMPTY_HEADER: return "EMPTY_HEADER";
        case ErrorCode::DUPLICATE_COLUMN_NAMES: return "DUPLICATE_COLUMN_NAMES";
        case ErrorCode::AMBIGUOUS_SEPARATOR: return "AMBIGUOUS_SEPARATOR";
        case ErrorCode::FILE_TOO_LARGE: return "FILE_TOO_LARGE";
        case ErrorCode::IO_ERROR: return "IO_ERROR";
        case ErrorCode::INTERNAL_ERROR: return "INTERNAL_ERROR";
        default: return "UNKNOWN";
    }
}

const char* error_severity_to_string(ErrorSeverity severity) {
    switch (severity) {
        case ErrorSeverity::WARNING: return "WARNING";
        case ErrorSeverity::ERROR: return "ERROR";
        case ErrorSeverity::FATAL: return "FATAL";
        default: return "UNKNOWN";
    }
}

std::string ParseError::to_string() const {
    std::ostringstream ss;
    ss << "[" << error_severity_to_string(severity) << "] "
       << error_code_to_string(code) << " at line " << line
       << ", column " << column << " (byte " << byte_offset << "): "
       << message;

    if (!context.empty()) {
        ss << "\n  Context: " << context;
    }

    return ss.str();
}

std::string ErrorCollector::summary() const {
    if (errors_.empty()) {
        return "No errors";
    }

    std::ostringstream ss;
    size_t warnings = 0, errors = 0, fatal = 0;

    for (const auto& err : errors_) {
        switch (err.severity) {
            case ErrorSeverity::WARNING: warnings++; break;
            case ErrorSeverity::ERROR: errors++; break;
            case ErrorSeverity::FATAL: fatal++; break;
        }
    }

    ss << "Total errors: " << errors_.size();
    if (warnings > 0) ss << " (Warnings: " << warnings;
    if (errors > 0) ss << ", Errors: " << errors;
    if (fatal > 0) ss << ", Fatal: " << fatal;
    if (warnings > 0 || errors > 0 || fatal > 0) ss << ")";

    ss << "\n\nDetails:\n";
    for (const auto& err : errors_) {
        ss << err.to_string() << "\n";
    }

    return ss.str();
}

std::string ParseException::format_errors(const std::vector<ParseError>& errors) {
    if (errors.empty()) return "Parse error";
    if (errors.size() == 1) return errors[0].message;

    std::ostringstream ss;
    ss << "Multiple parse errors (" << errors.size() << "):\n";
    for (const auto& err : errors) {
        ss << "  - " << err.to_string() << "\n";
    }
    return ss.str();
}

} // namespace simdcsv
