#include <gtest/gtest.h>
#include <string>

#include "dialect.h"
#include "two_pass.h"
#include "io_util.h"

// ============================================================================
// DIALECT DETECTION TESTS
// ============================================================================

class DialectDetectionTest : public ::testing::Test {
protected:
    std::string getTestDataPath(const std::string& category, const std::string& filename) {
        return "test/data/" + category + "/" + filename;
    }

    simdcsv::DialectDetector detector;
};

// ============================================================================
// Delimiter Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectCommaDelimiter) {
    std::string path = getTestDataPath("basic", "simple.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for simple.csv";
    EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma delimiter";
    EXPECT_EQ(result.detected_columns, 3) << "simple.csv has 3 columns";
}

TEST_F(DialectDetectionTest, DetectSemicolonDelimiter) {
    std::string path = getTestDataPath("separators", "semicolon.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for semicolon.csv";
    EXPECT_EQ(result.dialect.delimiter, ';') << "Should detect semicolon delimiter";
    EXPECT_EQ(result.detected_columns, 3) << "semicolon.csv has 3 columns";
}

TEST_F(DialectDetectionTest, DetectTabDelimiter) {
    std::string path = getTestDataPath("separators", "tab.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for tab.csv";
    EXPECT_EQ(result.dialect.delimiter, '\t') << "Should detect tab delimiter";
    EXPECT_EQ(result.detected_columns, 3) << "tab.csv has 3 columns";
}

TEST_F(DialectDetectionTest, DetectPipeDelimiter) {
    std::string path = getTestDataPath("separators", "pipe.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for pipe.csv";
    EXPECT_EQ(result.dialect.delimiter, '|') << "Should detect pipe delimiter";
    EXPECT_EQ(result.detected_columns, 3) << "pipe.csv has 3 columns";
}

// ============================================================================
// Embedded Separator Tests (should not be fooled by quoted delimiters)
// ============================================================================

TEST_F(DialectDetectionTest, NotFooledByQuotedCommas) {
    std::string path = getTestDataPath("quoted", "embedded_separators.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for embedded_separators.csv";
    EXPECT_EQ(result.dialect.delimiter, ',') << "Should detect comma, not be fooled by quoted commas";
    EXPECT_EQ(result.detected_columns, 3) << "embedded_separators.csv has 3 columns";
}

// ============================================================================
// Quote Character Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectDoubleQuote) {
    std::string path = getTestDataPath("quoted", "quoted_fields.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for quoted_fields.csv";
    EXPECT_EQ(result.dialect.quote_char, '"') << "Should detect double-quote character";
}

// ============================================================================
// Header Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectsHeaderInSimpleCSV) {
    std::string path = getTestDataPath("basic", "simple.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed";
    EXPECT_TRUE(result.has_header) << "simple.csv has a header row (A,B,C)";
}

TEST_F(DialectDetectionTest, DetectsNoHeaderWhenExplicitlyNone) {
    std::string path = getTestDataPath("basic", "simple_no_header.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed";
    // File contains only numeric data rows, so should not detect header
    EXPECT_FALSE(result.has_header) << "simple_no_header.csv has no header";
}

// ============================================================================
// Line Ending Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectLFLineEnding) {
    std::string path = getTestDataPath("line_endings", "lf.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for lf.csv";
    EXPECT_EQ(result.dialect.line_ending, simdcsv::Dialect::LineEnding::LF);
}

TEST_F(DialectDetectionTest, DetectCRLFLineEnding) {
    std::string path = getTestDataPath("line_endings", "crlf.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for crlf.csv";
    EXPECT_EQ(result.dialect.line_ending, simdcsv::Dialect::LineEnding::CRLF);
}

TEST_F(DialectDetectionTest, DetectCRLineEnding) {
    std::string path = getTestDataPath("line_endings", "cr.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for cr.csv";
    EXPECT_EQ(result.dialect.line_ending, simdcsv::Dialect::LineEnding::CR);
}

// ============================================================================
// Cell Type Inference Tests
// ============================================================================

TEST_F(DialectDetectionTest, InferIntegerType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("123"), CellType::INTEGER);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("-456"), CellType::INTEGER);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("+789"), CellType::INTEGER);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("0"), CellType::INTEGER);
}

TEST_F(DialectDetectionTest, InferFloatType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("3.14"), CellType::FLOAT);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("-2.718"), CellType::FLOAT);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("1e10"), CellType::FLOAT);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("1.5E-3"), CellType::FLOAT);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type(".5"), CellType::FLOAT);
}

TEST_F(DialectDetectionTest, InferBooleanType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("true"), CellType::BOOLEAN);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("false"), CellType::BOOLEAN);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("TRUE"), CellType::BOOLEAN);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("FALSE"), CellType::BOOLEAN);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("True"), CellType::BOOLEAN);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("False"), CellType::BOOLEAN);
}

TEST_F(DialectDetectionTest, InferDateType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("2024-01-15"), CellType::DATE);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("2024/01/15"), CellType::DATE);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("15-01-2024"), CellType::DATE);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("15/01/2024"), CellType::DATE);
}

TEST_F(DialectDetectionTest, InferTimeType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("14:30"), CellType::TIME);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("14:30:59"), CellType::TIME);
}

TEST_F(DialectDetectionTest, InferDateTimeType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("2024-01-15T14:30:00"), CellType::DATETIME);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("2024-01-15 14:30:00"), CellType::DATETIME);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("2024-01-15T14:30:00Z"), CellType::DATETIME);
}

TEST_F(DialectDetectionTest, InferEmptyType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type(""), CellType::EMPTY);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("   "), CellType::EMPTY);
}

TEST_F(DialectDetectionTest, InferStringType) {
    using CellType = simdcsv::DialectDetector::CellType;

    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("hello"), CellType::STRING);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("John Doe"), CellType::STRING);
    EXPECT_EQ(simdcsv::DialectDetector::infer_cell_type("123abc"), CellType::STRING);
}

// ============================================================================
// Dialect Factory Tests
// ============================================================================

TEST_F(DialectDetectionTest, DialectFactories) {
    auto csv = simdcsv::Dialect::csv();
    EXPECT_EQ(csv.delimiter, ',');
    EXPECT_EQ(csv.quote_char, '"');
    EXPECT_TRUE(csv.double_quote);

    auto tsv = simdcsv::Dialect::tsv();
    EXPECT_EQ(tsv.delimiter, '\t');
    EXPECT_EQ(tsv.quote_char, '"');

    auto semicolon = simdcsv::Dialect::semicolon();
    EXPECT_EQ(semicolon.delimiter, ';');

    auto pipe = simdcsv::Dialect::pipe();
    EXPECT_EQ(pipe.delimiter, '|');
}

TEST_F(DialectDetectionTest, DialectEquality) {
    auto d1 = simdcsv::Dialect::csv();
    auto d2 = simdcsv::Dialect::csv();
    auto d3 = simdcsv::Dialect::tsv();

    EXPECT_EQ(d1, d2);
    EXPECT_NE(d1, d3);
}

TEST_F(DialectDetectionTest, DialectToString) {
    auto csv = simdcsv::Dialect::csv();
    std::string str = csv.to_string();

    EXPECT_NE(str.find("','"), std::string::npos) << "Should contain comma repr";
    EXPECT_NE(str.find("Dialect"), std::string::npos) << "Should contain 'Dialect'";
}

// ============================================================================
// Edge Cases
// ============================================================================

TEST_F(DialectDetectionTest, EmptyFile) {
    std::string path = getTestDataPath("edge_cases", "empty_file.csv");
    auto result = detector.detect_file(path);

    EXPECT_FALSE(result.success()) << "Detection should fail for empty file";
    EXPECT_FALSE(result.warning.empty()) << "Should have a warning";
}

TEST_F(DialectDetectionTest, SingleCell) {
    std::string path = getTestDataPath("edge_cases", "single_cell.csv");
    auto result = detector.detect_file(path);

    // Single cell doesn't meet min_rows requirement by default
    // Detection may or may not succeed depending on content
    // Just verify it doesn't crash
    EXPECT_NO_FATAL_FAILURE(detector.detect_file(path));
}

TEST_F(DialectDetectionTest, NonExistentFile) {
    auto result = detector.detect_file("nonexistent.csv");

    EXPECT_FALSE(result.success()) << "Detection should fail for non-existent file";
    EXPECT_NE(result.warning.find("Could not open"), std::string::npos)
        << "Should warn about file not found";
}

// ============================================================================
// Detection from Memory Buffer
// ============================================================================

TEST_F(DialectDetectionTest, DetectFromBuffer) {
    const std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success()) << "Detection should succeed for in-memory CSV";
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, DetectSemicolonFromBuffer) {
    const std::string csv_data = "a;b;c\n1;2;3\n4;5;6\n7;8;9\n";
    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success()) << "Detection should succeed for semicolon-separated data";
    EXPECT_EQ(result.dialect.delimiter, ';');
    EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, NullBuffer) {
    auto result = detector.detect(nullptr, 100);

    EXPECT_FALSE(result.success()) << "Detection should fail for null buffer";
    EXPECT_FALSE(result.warning.empty());
}

TEST_F(DialectDetectionTest, ZeroLength) {
    uint8_t buf[1] = {0};
    auto result = detector.detect(buf, 0);

    EXPECT_FALSE(result.success()) << "Detection should fail for zero-length buffer";
}

// ============================================================================
// Custom Detection Options
// ============================================================================

TEST_F(DialectDetectionTest, CustomDelimiters) {
    simdcsv::DetectionOptions opts;
    opts.delimiters = {'#'};  // Only test hash

    simdcsv::DialectDetector custom_detector(opts);

    const std::string csv_data = "a#b#c\n1#2#3\n4#5#6\n7#8#9\n";
    auto result = custom_detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, '#');
}

// ============================================================================
// Real-World File Tests
// ============================================================================

TEST_F(DialectDetectionTest, RealWorldFinancial) {
    std::string path = getTestDataPath("real_world", "financial.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for financial.csv";
    EXPECT_EQ(result.dialect.delimiter, ',');
}

TEST_F(DialectDetectionTest, RealWorldContacts) {
    std::string path = getTestDataPath("real_world", "contacts.csv");
    auto result = detector.detect_file(path);

    EXPECT_TRUE(result.success()) << "Detection should succeed for contacts.csv";
    EXPECT_EQ(result.dialect.delimiter, ',');
}

// ============================================================================
// Parser Integration Tests
// ============================================================================

TEST_F(DialectDetectionTest, ParseAutoWithCommaCSV) {
    std::string path = getTestDataPath("basic", "simple.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::DetectionResult detected;

    bool success = parser.parse_auto(data.data(), idx, data.size(), errors, &detected);

    EXPECT_TRUE(success) << "parse_auto should succeed for simple.csv";
    EXPECT_TRUE(detected.success()) << "Detection should succeed";
    EXPECT_EQ(detected.dialect.delimiter, ',');
    EXPECT_EQ(detected.detected_columns, 3);
    EXPECT_EQ(errors.error_count(), 0) << "Should have no errors for valid CSV";
}

TEST_F(DialectDetectionTest, ParseAutoWithSemicolonCSV) {
    std::string path = getTestDataPath("separators", "semicolon.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::DetectionResult detected;

    bool success = parser.parse_auto(data.data(), idx, data.size(), errors, &detected);

    EXPECT_TRUE(success) << "parse_auto should succeed";
    EXPECT_TRUE(detected.success()) << "Detection should succeed";
    EXPECT_EQ(detected.dialect.delimiter, ';') << "Should detect semicolon";

    // Parser now uses detected dialect - verify it parsed correctly
    // by checking the number of fields found (should match detected_columns)
    size_t total_fields = 0;
    for (int t = 0; t < idx.n_threads; ++t) {
        total_fields += idx.n_indexes[t];
    }
    // Should have found field separators with the semicolon delimiter
    EXPECT_GT(total_fields, 0) << "Should find field separators with detected dialect";
    EXPECT_EQ(detected.detected_columns, 3) << "Should detect 3 columns";
}

TEST_F(DialectDetectionTest, DetectDialectStatic) {
    const std::string csv_data = "a;b;c\n1;2;3\n4;5;6\n7;8;9\n";
    auto result = simdcsv::two_pass::detect_dialect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
    EXPECT_EQ(result.detected_columns, 3);
}

TEST_F(DialectDetectionTest, DetectDialectWithOptions) {
    const std::string csv_data = "a#b#c\n1#2#3\n4#5#6\n7#8#9\n";

    simdcsv::DetectionOptions opts;
    opts.delimiters = {'#'};

    auto result = simdcsv::two_pass::detect_dialect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size(),
        opts
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, '#');
}

// ============================================================================
// Dialect-Aware Parsing Tests
// ============================================================================

TEST_F(DialectDetectionTest, ParseWithTSVDialect) {
    std::string path = getTestDataPath("separators", "tab.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);
    simdcsv::Dialect tsv = simdcsv::Dialect::tsv();

    bool success = parser.parse(data.data(), idx, data.size(), tsv);

    EXPECT_TRUE(success) << "Should parse TSV successfully";
    EXPECT_GT(idx.n_indexes[0], 0) << "Should find tab separators";
}

TEST_F(DialectDetectionTest, ParseWithSemicolonDialect) {
    std::string path = getTestDataPath("separators", "semicolon.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);
    simdcsv::Dialect semicolon = simdcsv::Dialect::semicolon();

    bool success = parser.parse(data.data(), idx, data.size(), semicolon);

    EXPECT_TRUE(success) << "Should parse semicolon-separated successfully";
    EXPECT_GT(idx.n_indexes[0], 0) << "Should find semicolon separators";
}

TEST_F(DialectDetectionTest, ParseWithPipeDialect) {
    std::string path = getTestDataPath("separators", "pipe.csv");
    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);
    simdcsv::Dialect pipe = simdcsv::Dialect::pipe();

    bool success = parser.parse(data.data(), idx, data.size(), pipe);

    EXPECT_TRUE(success) << "Should parse pipe-separated successfully";
    EXPECT_GT(idx.n_indexes[0], 0) << "Should find pipe separators";
}

TEST_F(DialectDetectionTest, ParseWithErrorsDialect) {
    // Test parse_with_errors with semicolon dialect
    const std::string csv_data = "name;age;city\nAlice;30;Paris\nBob;25;London\n";

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(csv_data.size(), 1);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::Dialect semicolon = simdcsv::Dialect::semicolon();

    bool success = parser.parse_with_errors(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        idx, csv_data.size(), errors, semicolon);

    EXPECT_TRUE(success) << "Should parse successfully";
    EXPECT_EQ(errors.error_count(), 0) << "Should have no errors";
}

TEST_F(DialectDetectionTest, ParseValidateDialect) {
    // Test parse_validate with tab dialect
    const std::string tsv_data = "name\tage\tcity\nAlice\t30\tParis\nBob\t25\tLondon\n";

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(tsv_data.size(), 1);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::Dialect tsv = simdcsv::Dialect::tsv();

    bool success = parser.parse_validate(
        reinterpret_cast<const uint8_t*>(tsv_data.data()),
        idx, tsv_data.size(), errors, tsv);

    EXPECT_TRUE(success) << "Validation should pass";
    EXPECT_EQ(errors.error_count(), 0) << "Should have no validation errors";
}

TEST_F(DialectDetectionTest, ParseWithSingleQuote) {
    // Test parsing with single-quote as quote character
    const std::string csv_data = "name,description\nAlice,'Hello, World'\nBob,'Test \"quote\"'\n";

    simdcsv::Dialect single_quote;
    single_quote.delimiter = ',';
    single_quote.quote_char = '\'';

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(csv_data.size(), 1);

    bool success = parser.parse(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        idx, csv_data.size(), single_quote);

    EXPECT_TRUE(success) << "Should parse successfully with single-quote";
}

TEST_F(DialectDetectionTest, ParseTwoPassWithErrorsDialect) {
    // Test parse_two_pass_with_errors with semicolon dialect
    const std::string csv_data = "name;age;city\nAlice;30;Paris\nBob;25;London\nCharlie;35;Berlin\n";

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(csv_data.size(), 2);  // 2 threads
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::Dialect semicolon = simdcsv::Dialect::semicolon();

    bool success = parser.parse_two_pass_with_errors(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        idx, csv_data.size(), errors, semicolon);

    EXPECT_TRUE(success) << "Should parse successfully with multi-threading";
    EXPECT_EQ(errors.error_count(), 0) << "Should have no errors";
}

// ============================================================================
// Dialect Validation Tests
// ============================================================================

TEST_F(DialectDetectionTest, DialectValidation_Valid) {
    simdcsv::Dialect csv = simdcsv::Dialect::csv();
    EXPECT_TRUE(csv.is_valid()) << "Standard CSV should be valid";

    simdcsv::Dialect tsv = simdcsv::Dialect::tsv();
    EXPECT_TRUE(tsv.is_valid()) << "TSV should be valid";

    simdcsv::Dialect semicolon = simdcsv::Dialect::semicolon();
    EXPECT_TRUE(semicolon.is_valid()) << "Semicolon-separated should be valid";

    simdcsv::Dialect pipe = simdcsv::Dialect::pipe();
    EXPECT_TRUE(pipe.is_valid()) << "Pipe-separated should be valid";
}

TEST_F(DialectDetectionTest, DialectValidation_SameDelimiterAndQuote) {
    simdcsv::Dialect invalid;
    invalid.delimiter = '"';
    invalid.quote_char = '"';
    EXPECT_FALSE(invalid.is_valid()) << "Same delimiter and quote should be invalid";

    EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

TEST_F(DialectDetectionTest, DialectValidation_NewlineDelimiter) {
    simdcsv::Dialect invalid;
    invalid.delimiter = '\n';
    invalid.quote_char = '"';
    EXPECT_FALSE(invalid.is_valid()) << "Newline delimiter should be invalid";

    EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

TEST_F(DialectDetectionTest, DialectValidation_NewlineQuote) {
    simdcsv::Dialect invalid;
    invalid.delimiter = ',';
    invalid.quote_char = '\n';
    EXPECT_FALSE(invalid.is_valid()) << "Newline quote should be invalid";

    EXPECT_THROW(invalid.validate(), std::invalid_argument);
}

// ============================================================================
// Escape Sequence Detection Tests
// ============================================================================

TEST_F(DialectDetectionTest, DetectBackslashEscape) {
    // CSV with backslash-escaped quotes: \"
    const std::string csv_data =
        "Name,Value\n"
        "\"John \\\"Boss\\\" Smith\",100\n"
        "\"Jane Doe\",200\n"
        "\"Bob\",300\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success()) << "Detection should succeed for backslash-escaped CSV";
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_EQ(result.dialect.quote_char, '"');
    // Should detect backslash escape, not double-quote
    EXPECT_EQ(result.dialect.escape_char, '\\');
    EXPECT_FALSE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, DetectDoubleQuoteEscape) {
    // Standard RFC 4180 CSV with "" escaping
    const std::string csv_data =
        "Name,Value\n"
        "\"John \"\"Boss\"\" Smith\",100\n"
        "\"Jane Doe\",200\n"
        "\"Bob\",300\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success()) << "Detection should succeed for double-quote escaped CSV";
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_EQ(result.dialect.quote_char, '"');
    EXPECT_TRUE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, BackslashEscapedDelimiter) {
    // CSV with backslash-escaped delimiter
    const std::string csv_data =
        "Name,Description\n"
        "\"Item A\",\"Has \\, comma\"\n"
        "\"Item B\",\"Normal text\"\n"
        "\"Item C\",\"More text\"\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_EQ(result.detected_columns, 2);
}

TEST_F(DialectDetectionTest, EscapeCharOptions) {
    // Test with custom escape character options
    simdcsv::DetectionOptions opts;
    opts.escape_chars = {'\\', '~'};  // Test backslash and tilde

    simdcsv::DialectDetector custom_detector(opts);

    const std::string csv_data =
        "A,B\n"
        "\"X \\\" Y\",1\n"
        "\"Z\",2\n"
        "\"W\",3\n";

    auto result = custom_detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.escape_char, '\\');
}

TEST_F(DialectDetectionTest, NoEscapeNeeded) {
    // Simple CSV without any escape sequences
    const std::string csv_data =
        "Name,Value\n"
        "John,100\n"
        "Jane,200\n"
        "Bob,300\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ',');
    // Should default to double-quote style when no escapes are present
    EXPECT_TRUE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, MixedEscapeStyles) {
    // CSV with both \" and "" patterns - should be ambiguous
    // The tie-breaker should prefer RFC 4180 (double_quote = true)
    const std::string csv_data =
        "Name,Value\n"
        "\"John \\\"Boss\\\" Smith\",100\n"
        "\"Jane \"\"Doe\"\" Jones\",200\n"
        "\"Bob\",300\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ',');
    // When mixed, tie-breakers prefer RFC 4180
    EXPECT_TRUE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, EscapeInMiddleOfField) {
    // Test escape character appearing in the middle of field content
    const std::string csv_data =
        "Name,Description\n"
        "\"Test\",\"Hello \\\"World\\\" Here\"\n"
        "\"Item\",\"Normal\"\n"
        "\"Other\",\"Text\"\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_EQ(result.dialect.escape_char, '\\');
    EXPECT_FALSE(result.dialect.double_quote);
}

TEST_F(DialectDetectionTest, ConsecutiveEscapes) {
    // Test multiple consecutive escape sequences
    // Each row has backslash-escaped quotes to ensure clear signal
    const std::string csv_data =
        "A,B\n"
        "\"First \\\"One\\\" here\",1\n"
        "\"Second \\\"Two\\\" here\",2\n"
        "\"Third \\\"Three\\\" here\",3\n"
        "\"Fourth \\\"Four\\\" here\",4\n";

    auto result = detector.detect(
        reinterpret_cast<const uint8_t*>(csv_data.data()),
        csv_data.size()
    );

    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.escape_char, '\\');
    EXPECT_FALSE(result.dialect.double_quote);
}
