#include <gtest/gtest.h>
#include <libvroom.h>
#include <string>
#include <cstring>

class SimplifiedAPITest : public ::testing::Test {
protected:
    static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
        size_t len = content.size();
        uint8_t* buf = allocate_padded_buffer(len, 64);
        std::memcpy(buf, content.data(), len);
        return {buf, len};
    }
};

TEST_F(SimplifiedAPITest, FileBufferBasics) {
    libvroom::FileBuffer empty;
    EXPECT_FALSE(empty.valid());
    EXPECT_TRUE(empty.empty());

    auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
    libvroom::FileBuffer buffer(data, len);
    EXPECT_TRUE(buffer.valid());
    EXPECT_FALSE(buffer.empty());
}

TEST_F(SimplifiedAPITest, FileBufferMove) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
    libvroom::FileBuffer buffer1(data, len);
    libvroom::FileBuffer buffer2(std::move(buffer1));
    EXPECT_FALSE(buffer1.valid());
    EXPECT_TRUE(buffer2.valid());
}

TEST_F(SimplifiedAPITest, FileBufferRelease) {
    auto [data, len] = make_buffer("a,b,c\n");
    libvroom::FileBuffer buffer(data, len);
    uint8_t* released = buffer.release();
    EXPECT_FALSE(buffer.valid());
    aligned_free(released);
}

TEST_F(SimplifiedAPITest, ParserBasicParsing) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

TEST_F(SimplifiedAPITest, ParserWithErrors) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    libvroom::Parser parser;
    auto result = parser.parse_with_errors(buffer.data(), buffer.size(), errors);
    EXPECT_TRUE(result.success());
    EXPECT_TRUE(errors.has_errors());
}

TEST_F(SimplifiedAPITest, ParserDialects) {
    {
        auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n");
        libvroom::FileBuffer buffer(data, len);
        libvroom::Parser parser;
        auto result = parser.parse(buffer.data(), buffer.size(), libvroom::Dialect::tsv());
        EXPECT_TRUE(result.success());
    }
    {
        auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
        libvroom::FileBuffer buffer(data, len);
        libvroom::Parser parser;
        auto result = parser.parse(buffer.data(), buffer.size(), libvroom::Dialect::semicolon());
        EXPECT_TRUE(result.success());
    }
}

TEST_F(SimplifiedAPITest, DetectDialect) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);  // RAII wrapper handles cleanup
    auto detection = libvroom::detect_dialect(buffer.data(), buffer.size());
    EXPECT_TRUE(detection.success());
    EXPECT_EQ(detection.dialect.delimiter, ',');
}

TEST_F(SimplifiedAPITest, ParserAutoDetection) {
    auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    libvroom::Parser parser;
    auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
}

TEST_F(SimplifiedAPITest, ParserThreadCount) {
    libvroom::Parser parser1(1);
    EXPECT_EQ(parser1.num_threads(), 1);
    libvroom::Parser parser4(4);
    EXPECT_EQ(parser4.num_threads(), 4);
    parser4.set_num_threads(0);
    EXPECT_EQ(parser4.num_threads(), 1);
}

TEST_F(SimplifiedAPITest, CustomDialect) {
    auto [data, len] = make_buffer("a:b:c\n'hello':'world':'!'\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Dialect custom;
    custom.delimiter = ':';
    custom.quote_char = '\'';
    libvroom::Parser parser;
    auto result = parser.parse(buffer.data(), buffer.size(), custom);
    EXPECT_TRUE(result.success());
}

// ============================================================================
// Tests for the unified ParseOptions API
// ============================================================================

class UnifiedAPITest : public ::testing::Test {
protected:
    static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
        size_t len = content.size();
        uint8_t* buf = allocate_padded_buffer(len, 64);
        std::memcpy(buf, content.data(), len);
        return {buf, len};
    }
};

// Test: Default options (auto-detect dialect, fast path)
TEST_F(UnifiedAPITest, DefaultOptions) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    // Default: auto-detect dialect, throw on errors
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ',');
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: Auto-detect semicolon-separated data
TEST_F(UnifiedAPITest, AutoDetectSemicolon) {
    auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: Auto-detect tab-separated data
TEST_F(UnifiedAPITest, AutoDetectTSV) {
    auto [data, len] = make_buffer("name\tage\tcity\nJohn\t25\tNYC\nJane\t30\tLA\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, '\t');
}

// Test: Explicit dialect via ParseOptions
TEST_F(UnifiedAPITest, ExplicitDialect) {
    auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ParseOptions opts;
    opts.dialect = libvroom::Dialect::semicolon();

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: Explicit dialect using factory method
TEST_F(UnifiedAPITest, ExplicitDialectFactory) {
    auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::with_dialect(libvroom::Dialect::tsv()));
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, '\t');
}

// Test: Error collection via ParseOptions
TEST_F(UnifiedAPITest, ErrorCollection) {
    // CSV with inconsistent field count (row 3 has only 2 fields)
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    libvroom::ParseOptions opts;
    opts.errors = &errors;

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());  // Parsing succeeds in permissive mode
    EXPECT_TRUE(errors.has_errors());
}

// Test: Error collection using factory method
TEST_F(UnifiedAPITest, ErrorCollectionFactory) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::with_errors(errors));
    EXPECT_TRUE(result.success());
    EXPECT_TRUE(errors.has_errors());
}

// Test: Explicit dialect + error collection
TEST_F(UnifiedAPITest, ExplicitDialectWithErrors) {
    auto [data, len] = make_buffer("a;b;c\n1;2;3\n4;5\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    libvroom::ParseOptions opts;
    opts.dialect = libvroom::Dialect::semicolon();
    opts.errors = &errors;

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
    EXPECT_TRUE(errors.has_errors());
}

// Test: Explicit dialect + error collection using factory
TEST_F(UnifiedAPITest, ExplicitDialectWithErrorsFactory) {
    auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n4\t5\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    auto result = parser.parse(buffer.data(), buffer.size(),
        libvroom::ParseOptions::with_dialect_and_errors(libvroom::Dialect::tsv(), errors));
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, '\t');
    EXPECT_TRUE(errors.has_errors());
}

// Test: Detection result is populated
TEST_F(UnifiedAPITest, DetectionResultPopulated) {
    auto [data, len] = make_buffer("name|age|city\nJohn|25|NYC\nJane|30|LA\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, '|');
    // Detection result should be populated when auto-detecting
    EXPECT_TRUE(result.detection.success());
    EXPECT_EQ(result.detection.dialect.delimiter, '|');
}

// Test: Legacy parse(buf, len, dialect) still works
TEST_F(UnifiedAPITest, LegacyParseWithDialect) {
    auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(), libvroom::Dialect::semicolon());
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: Legacy parse_with_errors still works
TEST_F(UnifiedAPITest, LegacyParseWithErrors) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    auto result = parser.parse_with_errors(buffer.data(), buffer.size(), errors);
    EXPECT_TRUE(result.success());
    EXPECT_TRUE(errors.has_errors());
}

// Test: Legacy parse_auto still works
TEST_F(UnifiedAPITest, LegacyParseAuto) {
    auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
}

// Test: ParseOptions defaults factory
TEST_F(UnifiedAPITest, ParseOptionsDefaults) {
    auto opts = libvroom::ParseOptions::defaults();
    EXPECT_FALSE(opts.dialect.has_value());
    EXPECT_EQ(opts.errors, nullptr);
}

// Test: Custom detection options
TEST_F(UnifiedAPITest, CustomDetectionOptions) {
    auto [data, len] = make_buffer("a:b:c\n1:2:3\n4:5:6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ParseOptions opts;
    opts.detection_options.delimiters = {':', ','};  // Only check colon and comma

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ':');
}

// Test: Custom detection options with error collection
TEST_F(UnifiedAPITest, CustomDetectionOptionsWithErrors) {
    auto [data, len] = make_buffer("a:b:c\n1:2:3\n4:5\n");  // Inconsistent field count
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ErrorCollector errors(libvroom::ErrorMode::PERMISSIVE);
    libvroom::ParseOptions opts;
    opts.detection_options.delimiters = {':', ','};  // Only check colon and comma
    opts.errors = &errors;

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ':');
    EXPECT_TRUE(errors.has_errors());  // Should detect field count mismatch
}

// Test: Explicit dialect skips detection (performance optimization)
TEST_F(UnifiedAPITest, ExplicitDialectSkipsDetection) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(),
                               {.dialect = libvroom::Dialect::csv()});
    EXPECT_TRUE(result.success());
    // Detection should not run when dialect is explicit
    EXPECT_EQ(result.detection.confidence, 0.0);
    EXPECT_EQ(result.detection.rows_analyzed, 0);
}

// ============================================================================
// Tests for ParseAlgorithm selection
// ============================================================================

class AlgorithmSelectionTest : public ::testing::Test {
protected:
    static std::pair<uint8_t*, size_t> make_buffer(const std::string& content) {
        size_t len = content.size();
        uint8_t* buf = allocate_padded_buffer(len, 64);
        std::memcpy(buf, content.data(), len);
        return {buf, len};
    }
};

// Test: ParseAlgorithm::AUTO (default)
TEST_F(AlgorithmSelectionTest, AutoAlgorithm) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::with_algorithm(libvroom::ParseAlgorithm::AUTO));
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseAlgorithm::SPECULATIVE
TEST_F(AlgorithmSelectionTest, SpeculativeAlgorithm) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ParseOptions opts;
    opts.dialect = libvroom::Dialect::csv();
    opts.algorithm = libvroom::ParseAlgorithm::SPECULATIVE;

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseAlgorithm::TWO_PASS
TEST_F(AlgorithmSelectionTest, TwoPassAlgorithm) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ParseOptions opts;
    opts.dialect = libvroom::Dialect::csv();
    opts.algorithm = libvroom::ParseAlgorithm::TWO_PASS;

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseAlgorithm::BRANCHLESS
TEST_F(AlgorithmSelectionTest, BranchlessAlgorithm) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    libvroom::ParseOptions opts;
    opts.dialect = libvroom::Dialect::csv();
    opts.algorithm = libvroom::ParseAlgorithm::BRANCHLESS;

    auto result = parser.parse(buffer.data(), buffer.size(), opts);
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: ParseOptions::branchless() factory
TEST_F(AlgorithmSelectionTest, BranchlessFactory) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::branchless());
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: Branchless with custom dialect
TEST_F(AlgorithmSelectionTest, BranchlessWithDialect) {
    auto [data, len] = make_buffer("a;b;c\n1;2;3\n4;5;6\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::branchless(libvroom::Dialect::semicolon()));
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: Algorithm with multi-threading
TEST_F(AlgorithmSelectionTest, BranchlessMultiThreaded) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n7,8,9\n10,11,12\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser(4);  // 4 threads

    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::branchless());
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

// Test: Different algorithms produce same results
TEST_F(AlgorithmSelectionTest, AlgorithmsProduceSameResults) {
    auto [data, len] = make_buffer("name,age,city\nAlice,30,NYC\nBob,25,LA\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    // Parse with each algorithm
    auto result_auto = parser.parse(buffer.data(), buffer.size(),
        {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::AUTO});
    auto result_spec = parser.parse(buffer.data(), buffer.size(),
        {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::SPECULATIVE});
    auto result_two = parser.parse(buffer.data(), buffer.size(),
        {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::TWO_PASS});
    auto result_branch = parser.parse(buffer.data(), buffer.size(),
        {.dialect = libvroom::Dialect::csv(), .algorithm = libvroom::ParseAlgorithm::BRANCHLESS});

    // All should succeed and produce same number of indexes
    EXPECT_TRUE(result_auto.success());
    EXPECT_TRUE(result_spec.success());
    EXPECT_TRUE(result_two.success());
    EXPECT_TRUE(result_branch.success());

    EXPECT_EQ(result_auto.total_indexes(), result_spec.total_indexes());
    EXPECT_EQ(result_auto.total_indexes(), result_two.total_indexes());
    EXPECT_EQ(result_auto.total_indexes(), result_branch.total_indexes());
}

// Test: Algorithm selection with quoted fields
TEST_F(AlgorithmSelectionTest, BranchlessWithQuotedFields) {
    auto [data, len] = make_buffer("name,description\n\"Alice\",\"Hello, World\"\n\"Bob\",\"Line1\\nLine2\"\n");
    libvroom::FileBuffer buffer(data, len);
    libvroom::Parser parser;

    auto result = parser.parse(buffer.data(), buffer.size(),
                               libvroom::ParseOptions::branchless());
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}
