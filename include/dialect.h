/**
 * @file dialect.h
 * @brief CSV dialect detection and configuration.
 *
 * This header provides structures and algorithms for CSV dialect detection,
 * including automatic detection of delimiters, quote characters, and escape
 * mechanisms. The detection algorithm is inspired by CleverCSV and uses a
 * consistency-based scoring approach.
 *
 * @see DialectDetector for automatic dialect detection
 * @see Dialect for dialect configuration
 */

#ifndef SIMDCSV_DIALECT_H
#define SIMDCSV_DIALECT_H

#include <cstdint>
#include <cstddef>
#include <string>
#include <string_view>
#include <vector>

namespace simdcsv {

/**
 * @brief CSV dialect configuration.
 *
 * Holds the parameters that define how a CSV file is formatted:
 * - delimiter: field separator character (default: comma)
 * - quote_char: character used to quote fields (default: double-quote)
 * - escape_char: character used to escape quotes
 * - double_quote: whether quotes are escaped by doubling (RFC 4180 style)
 */
struct Dialect {
    char delimiter = ',';
    char quote_char = '"';
    char escape_char = '"';
    bool double_quote = true;  ///< If true, "" escapes to " (RFC 4180)

    /// Line ending style detected (informational)
    enum class LineEnding { LF, CRLF, CR, MIXED, UNKNOWN };
    LineEnding line_ending = LineEnding::UNKNOWN;

    /// Factory for standard CSV (comma-separated, double-quoted)
    static Dialect csv() {
        return Dialect{',', '"', '"', true, LineEnding::UNKNOWN};
    }

    /// Factory for TSV (tab-separated)
    static Dialect tsv() {
        return Dialect{'\t', '"', '"', true, LineEnding::UNKNOWN};
    }

    /// Factory for semicolon-separated (European style)
    static Dialect semicolon() {
        return Dialect{';', '"', '"', true, LineEnding::UNKNOWN};
    }

    /// Factory for pipe-separated
    static Dialect pipe() {
        return Dialect{'|', '"', '"', true, LineEnding::UNKNOWN};
    }

    bool operator==(const Dialect& other) const {
        return delimiter == other.delimiter &&
               quote_char == other.quote_char &&
               escape_char == other.escape_char &&
               double_quote == other.double_quote;
    }

    bool operator!=(const Dialect& other) const {
        return !(*this == other);
    }

    /// Returns a human-readable description of the dialect
    std::string to_string() const;
};

/**
 * @brief Configuration options for dialect detection.
 */
struct DetectionOptions {
    size_t sample_size = 10240;   ///< Bytes to sample (default 10KB)
    size_t min_rows = 3;          ///< Minimum rows needed for detection
    size_t max_rows = 100;        ///< Maximum rows to analyze

    /// Candidate delimiter characters to test
    std::vector<char> delimiters = {',', ';', '\t', '|', ':'};

    /// Candidate quote characters to test
    std::vector<char> quote_chars = {'"', '\''};

    /// Minimum confidence threshold for successful detection
    double min_confidence = 0.5;
};

/**
 * @brief Candidate dialect with detection scores.
 */
struct DialectCandidate {
    Dialect dialect;
    double pattern_score = 0.0;      ///< Row length consistency [0, 1]
    double type_score = 0.0;         ///< Cell type inference score [0, 1]
    double consistency_score = 0.0;  ///< Combined: pattern_score * type_score
    size_t num_columns = 0;          ///< Detected column count

    bool operator<(const DialectCandidate& other) const {
        // Higher consistency score is better
        if (consistency_score != other.consistency_score) {
            return consistency_score > other.consistency_score;
        }
        // Tie-break: prefer more columns
        return num_columns > other.num_columns;
    }
};

/**
 * @brief Result of dialect detection.
 */
struct DetectionResult {
    Dialect dialect;                  ///< Detected dialect
    double confidence = 0.0;          ///< Overall confidence [0, 1]
    bool has_header = false;          ///< Whether first row appears to be header
    size_t detected_columns = 0;      ///< Number of columns detected
    size_t rows_analyzed = 0;         ///< Number of rows analyzed
    std::string warning;              ///< Any warnings during detection

    /// All tested candidates, sorted by score (best first)
    std::vector<DialectCandidate> candidates;

    /// Returns true if detection was successful (confidence > threshold)
    bool success() const { return confidence > 0.5; }
};

/**
 * @brief CSV dialect auto-detector.
 *
 * Implements a CleverCSV-inspired detection algorithm:
 * 1. Generate candidate dialects from delimiter/quote combinations
 * 2. For each candidate, compute pattern score (row consistency)
 * 3. For each candidate, compute type score (cell type inference)
 * 4. Rank by consistency_score = pattern_score * type_score
 *
 * @example
 * @code
 * #include "dialect.h"
 * #include "io_util.h"
 *
 * auto corpus = get_corpus("data.csv", 64);
 * simdcsv::DialectDetector detector;
 * auto result = detector.detect(corpus.data(), corpus.size());
 *
 * if (result.success()) {
 *     std::cout << "Detected: " << result.dialect.to_string() << std::endl;
 *     std::cout << "Columns: " << result.detected_columns << std::endl;
 * }
 * @endcode
 */
class DialectDetector {
public:
    /// Cell type categories for type inference
    enum class CellType {
        EMPTY,
        INTEGER,
        FLOAT,
        DATE,
        DATETIME,
        TIME,
        BOOLEAN,
        STRING
    };

    /**
     * @brief Construct a detector with given options.
     * @param options Detection configuration
     */
    explicit DialectDetector(const DetectionOptions& options = DetectionOptions());

    /**
     * @brief Detect dialect from a memory buffer.
     * @param buf Pointer to CSV data
     * @param len Length of data in bytes
     * @return DetectionResult with detected dialect and confidence
     */
    DetectionResult detect(const uint8_t* buf, size_t len) const;

    /**
     * @brief Detect dialect from a file.
     * @param filename Path to CSV file
     * @return DetectionResult with detected dialect and confidence
     */
    DetectionResult detect_file(const std::string& filename) const;

    /**
     * @brief Infer the type of a cell value.
     * @param cell The cell content as a string view
     * @return The inferred CellType
     */
    static CellType infer_cell_type(std::string_view cell);

    /**
     * @brief Convert CellType to string for debugging.
     */
    static const char* cell_type_to_string(CellType type);

private:
    DetectionOptions options_;

    /// Generate all candidate dialects to test
    std::vector<Dialect> generate_candidates() const;

    /// Score a single dialect candidate
    DialectCandidate score_dialect(const Dialect& dialect,
                                   const uint8_t* buf, size_t len) const;

    /// Compute pattern score (row length consistency)
    double compute_pattern_score(const Dialect& dialect,
                                 const uint8_t* buf, size_t len,
                                 std::vector<size_t>& row_field_counts) const;

    /// Compute type score (ratio of typed cells)
    double compute_type_score(const Dialect& dialect,
                              const uint8_t* buf, size_t len) const;

    /// Detect line ending style
    static Dialect::LineEnding detect_line_ending(const uint8_t* buf, size_t len);

    /// Detect if first row is likely a header
    bool detect_header(const Dialect& dialect,
                       const uint8_t* buf, size_t len) const;

    /// Find row boundaries respecting quote state
    std::vector<std::pair<size_t, size_t>> find_rows(
        const Dialect& dialect,
        const uint8_t* buf, size_t len) const;

    /// Extract fields from a single row
    std::vector<std::string_view> extract_fields(
        const Dialect& dialect,
        const uint8_t* row_start, size_t row_len) const;
};

}  // namespace simdcsv

#endif  // SIMDCSV_DIALECT_H
