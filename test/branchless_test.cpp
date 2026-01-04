/**
 * @file branchless_test.cpp
 * @brief Unit tests for the branchless CSV state machine implementation.
 */

#include <gtest/gtest.h>
#include <string>
#include <cstring>

#include "branchless_state_machine.h"
#include "two_pass.h"
#include "io_util.h"

using namespace simdcsv;

// ============================================================================
// BRANCHLESS STATE MACHINE UNIT TESTS
// ============================================================================

class BranchlessStateMachineTest : public ::testing::Test {
protected:
    BranchlessStateMachine sm;

    BranchlessStateMachineTest() : sm(',', '"') {}
};

TEST_F(BranchlessStateMachineTest, CharacterClassification) {
    // Test character classification
    EXPECT_EQ(sm.classify(','), CHAR_DELIMITER);
    EXPECT_EQ(sm.classify('"'), CHAR_QUOTE);
    EXPECT_EQ(sm.classify('\n'), CHAR_NEWLINE);
    EXPECT_EQ(sm.classify('a'), CHAR_OTHER);
    EXPECT_EQ(sm.classify('1'), CHAR_OTHER);
    EXPECT_EQ(sm.classify(' '), CHAR_OTHER);
    EXPECT_EQ(sm.classify('\t'), CHAR_OTHER);
}

TEST_F(BranchlessStateMachineTest, CustomDelimiter) {
    BranchlessStateMachine sm_tab('\t', '"');
    EXPECT_EQ(sm_tab.classify('\t'), CHAR_DELIMITER);
    EXPECT_EQ(sm_tab.classify(','), CHAR_OTHER);

    BranchlessStateMachine sm_semicolon(';', '"');
    EXPECT_EQ(sm_semicolon.classify(';'), CHAR_DELIMITER);
    EXPECT_EQ(sm_semicolon.classify(','), CHAR_OTHER);
}

TEST_F(BranchlessStateMachineTest, CustomQuote) {
    BranchlessStateMachine sm_single(',', '\'');
    EXPECT_EQ(sm_single.classify('\''), CHAR_QUOTE);
    EXPECT_EQ(sm_single.classify('"'), CHAR_OTHER);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_RecordStart) {
    PackedResult r;

    // RECORD_START + DELIMITER -> FIELD_START (separator)
    r = sm.transition(STATE_RECORD_START, CHAR_DELIMITER);
    EXPECT_EQ(r.state(), STATE_FIELD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // RECORD_START + QUOTE -> QUOTED_FIELD
    r = sm.transition(STATE_RECORD_START, CHAR_QUOTE);
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // RECORD_START + NEWLINE -> RECORD_START (separator)
    r = sm.transition(STATE_RECORD_START, CHAR_NEWLINE);
    EXPECT_EQ(r.state(), STATE_RECORD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // RECORD_START + OTHER -> UNQUOTED_FIELD
    r = sm.transition(STATE_RECORD_START, CHAR_OTHER);
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_FieldStart) {
    PackedResult r;

    // FIELD_START + DELIMITER -> FIELD_START (empty field, separator)
    r = sm.transition(STATE_FIELD_START, CHAR_DELIMITER);
    EXPECT_EQ(r.state(), STATE_FIELD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // FIELD_START + QUOTE -> QUOTED_FIELD
    r = sm.transition(STATE_FIELD_START, CHAR_QUOTE);
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // FIELD_START + NEWLINE -> RECORD_START (separator)
    r = sm.transition(STATE_FIELD_START, CHAR_NEWLINE);
    EXPECT_EQ(r.state(), STATE_RECORD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // FIELD_START + OTHER -> UNQUOTED_FIELD
    r = sm.transition(STATE_FIELD_START, CHAR_OTHER);
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_UnquotedField) {
    PackedResult r;

    // UNQUOTED_FIELD + DELIMITER -> FIELD_START (separator)
    r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_DELIMITER);
    EXPECT_EQ(r.state(), STATE_FIELD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // UNQUOTED_FIELD + QUOTE -> error
    r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_QUOTE);
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_QUOTE_IN_UNQUOTED);

    // UNQUOTED_FIELD + NEWLINE -> RECORD_START (separator)
    r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_NEWLINE);
    EXPECT_EQ(r.state(), STATE_RECORD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // UNQUOTED_FIELD + OTHER -> UNQUOTED_FIELD
    r = sm.transition(STATE_UNQUOTED_FIELD, CHAR_OTHER);
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_QuotedField) {
    PackedResult r;

    // QUOTED_FIELD + DELIMITER -> QUOTED_FIELD (literal comma)
    r = sm.transition(STATE_QUOTED_FIELD, CHAR_DELIMITER);
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // QUOTED_FIELD + QUOTE -> QUOTED_END
    r = sm.transition(STATE_QUOTED_FIELD, CHAR_QUOTE);
    EXPECT_EQ(r.state(), STATE_QUOTED_END);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // QUOTED_FIELD + NEWLINE -> QUOTED_FIELD (literal newline)
    r = sm.transition(STATE_QUOTED_FIELD, CHAR_NEWLINE);
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // QUOTED_FIELD + OTHER -> QUOTED_FIELD
    r = sm.transition(STATE_QUOTED_FIELD, CHAR_OTHER);
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);
}

TEST_F(BranchlessStateMachineTest, StateTransitions_QuotedEnd) {
    PackedResult r;

    // QUOTED_END + DELIMITER -> FIELD_START (separator)
    r = sm.transition(STATE_QUOTED_END, CHAR_DELIMITER);
    EXPECT_EQ(r.state(), STATE_FIELD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // QUOTED_END + QUOTE -> QUOTED_FIELD (escaped quote)
    r = sm.transition(STATE_QUOTED_END, CHAR_QUOTE);
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // QUOTED_END + NEWLINE -> RECORD_START (separator)
    r = sm.transition(STATE_QUOTED_END, CHAR_NEWLINE);
    EXPECT_EQ(r.state(), STATE_RECORD_START);
    EXPECT_TRUE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_NONE);

    // QUOTED_END + OTHER -> error
    r = sm.transition(STATE_QUOTED_END, CHAR_OTHER);
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());
    EXPECT_EQ(r.error(), ERR_INVALID_AFTER_QUOTE);
}

TEST_F(BranchlessStateMachineTest, ProcessCharacter) {
    BranchlessState state = STATE_RECORD_START;
    PackedResult r;

    // Process "ab,cd\n"
    r = sm.process(state, 'a');
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    state = r.state();

    r = sm.process(state, 'b');
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    state = r.state();

    r = sm.process(state, ',');
    EXPECT_EQ(r.state(), STATE_FIELD_START);
    EXPECT_TRUE(r.is_separator());
    state = r.state();

    r = sm.process(state, 'c');
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    state = r.state();

    r = sm.process(state, 'd');
    EXPECT_EQ(r.state(), STATE_UNQUOTED_FIELD);
    state = r.state();

    r = sm.process(state, '\n');
    EXPECT_EQ(r.state(), STATE_RECORD_START);
    EXPECT_TRUE(r.is_separator());
}

TEST_F(BranchlessStateMachineTest, ProcessQuotedField) {
    BranchlessState state = STATE_RECORD_START;
    PackedResult r;

    // Process "\"a,b\""
    r = sm.process(state, '"');
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    state = r.state();

    r = sm.process(state, 'a');
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    state = r.state();

    r = sm.process(state, ',');
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD); // Comma inside quotes
    EXPECT_FALSE(r.is_separator());
    state = r.state();

    r = sm.process(state, 'b');
    EXPECT_EQ(r.state(), STATE_QUOTED_FIELD);
    state = r.state();

    r = sm.process(state, '"');
    EXPECT_EQ(r.state(), STATE_QUOTED_END);
    state = r.state();

    r = sm.process(state, ',');
    EXPECT_EQ(r.state(), STATE_FIELD_START);
    EXPECT_TRUE(r.is_separator()); // Comma after quote ends field
}

TEST_F(BranchlessStateMachineTest, ProcessEscapedQuote) {
    BranchlessState state = STATE_RECORD_START;
    PackedResult r;

    // Process "\"a\"\"b\""
    r = sm.process(state, '"');
    state = r.state();
    EXPECT_EQ(state, STATE_QUOTED_FIELD);

    r = sm.process(state, 'a');
    state = r.state();
    EXPECT_EQ(state, STATE_QUOTED_FIELD);

    r = sm.process(state, '"');
    state = r.state();
    EXPECT_EQ(state, STATE_QUOTED_END);

    r = sm.process(state, '"'); // Escaped quote
    state = r.state();
    EXPECT_EQ(state, STATE_QUOTED_FIELD);
    EXPECT_FALSE(r.is_separator());

    r = sm.process(state, 'b');
    state = r.state();
    EXPECT_EQ(state, STATE_QUOTED_FIELD);

    r = sm.process(state, '"');
    state = r.state();
    EXPECT_EQ(state, STATE_QUOTED_END);
}

// ============================================================================
// BRANCHLESS PARSING INTEGRATION TESTS
// ============================================================================

class BranchlessParsingTest : public ::testing::Test {
protected:
    std::string getTestDataPath(const std::string& category, const std::string& filename) {
        return "test/data/" + category + "/" + filename;
    }
};

TEST_F(BranchlessParsingTest, ParseSimpleCSV) {
    std::string path = getTestDataPath("basic", "simple.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should successfully parse simple.csv";
}

TEST_F(BranchlessParsingTest, ParseQuotedFields) {
    std::string path = getTestDataPath("quoted", "quoted_fields.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle quoted fields";
}

TEST_F(BranchlessParsingTest, ParseEscapedQuotes) {
    std::string path = getTestDataPath("quoted", "escaped_quotes.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle escaped quotes";
}

TEST_F(BranchlessParsingTest, ParseNewlinesInQuotes) {
    std::string path = getTestDataPath("quoted", "newlines_in_quotes.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle newlines in quoted fields";
}

TEST_F(BranchlessParsingTest, ParseManyRows) {
    std::string path = getTestDataPath("basic", "many_rows.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle many rows";
}

TEST_F(BranchlessParsingTest, ParseWideColumns) {
    std::string path = getTestDataPath("basic", "wide_columns.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle wide CSV";
}

TEST_F(BranchlessParsingTest, ParseEmptyFields) {
    std::string path = getTestDataPath("edge_cases", "empty_fields.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle empty fields";
}

TEST_F(BranchlessParsingTest, ParseCustomDelimiter) {
    // Test with semicolon delimiter
    std::vector<uint8_t> data;
    std::string content = "A;B;C\n1;2;3\n";
    data.resize(content.size() + SIMDCSV_PADDING);
    std::memcpy(data.data(), content.data(), content.size());

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    bool success = parser.parse_branchless(data.data(), idx, content.size(),
                                           simdcsv::Dialect::semicolon());

    EXPECT_TRUE(success) << "Branchless parser should handle semicolon delimiter";
}

TEST_F(BranchlessParsingTest, ParseCustomQuote) {
    // Test with single quote
    std::vector<uint8_t> data;
    std::string content = "A,B,C\n'a,b',2,3\n";
    data.resize(content.size() + SIMDCSV_PADDING);
    std::memcpy(data.data(), content.data(), content.size());

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 1);

    // Create dialect with single quote as quote character
    simdcsv::Dialect dialect{',', '\'', '\'', true, simdcsv::Dialect::LineEnding::UNKNOWN};
    bool success = parser.parse_branchless(data.data(), idx, content.size(), dialect);

    EXPECT_TRUE(success) << "Branchless parser should handle single quote character";
}

TEST_F(BranchlessParsingTest, MultiThreadedParsing) {
    std::string path = getTestDataPath("basic", "many_rows.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 2);

    bool success = parser.parse_branchless(data.data(), idx, data.size());

    EXPECT_TRUE(success) << "Branchless parser should handle multi-threaded parsing";
}

TEST_F(BranchlessParsingTest, ConsistencyWithStandardParser) {
    // Verify branchless parser produces same results as standard parser
    std::string path = getTestDataPath("basic", "simple.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;

    // Parse with standard parser
    simdcsv::index idx1 = parser.init(data.size(), 1);
    parser.parse(data.data(), idx1, data.size());

    // Parse with branchless parser
    simdcsv::index idx2 = parser.init(data.size(), 1);
    parser.parse_branchless(data.data(), idx2, data.size());

    // Compare results
    EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
        << "Branchless parser should find same number of field separators";

    for (size_t i = 0; i < idx1.n_indexes[0]; ++i) {
        EXPECT_EQ(idx1.indexes[i], idx2.indexes[i])
            << "Field separator positions should match at index " << i;
    }
}

TEST_F(BranchlessParsingTest, ConsistencyWithQuotedFields) {
    // Verify branchless parser produces same results with quoted fields
    std::string path = getTestDataPath("quoted", "quoted_fields.csv");

    auto data = get_corpus(path, SIMDCSV_PADDING);

    simdcsv::two_pass parser;

    // Parse with standard parser
    simdcsv::index idx1 = parser.init(data.size(), 1);
    parser.parse(data.data(), idx1, data.size());

    // Parse with branchless parser
    simdcsv::index idx2 = parser.init(data.size(), 1);
    parser.parse_branchless(data.data(), idx2, data.size());

    // Compare results
    EXPECT_EQ(idx1.n_indexes[0], idx2.n_indexes[0])
        << "Branchless parser should find same number of field separators";

    for (size_t i = 0; i < idx1.n_indexes[0]; ++i) {
        EXPECT_EQ(idx1.indexes[i], idx2.indexes[i])
            << "Field separator positions should match at index " << i;
    }
}

TEST_F(BranchlessParsingTest, LargeDataMultithreaded) {
    // Test with large generated data
    std::vector<uint8_t> data;
    std::string content;

    // Generate large CSV
    content = "A,B,C\n";
    for (int i = 0; i < 10000; i++) {
        content += std::to_string(i) + ",";
        content += "\"value" + std::to_string(i) + "\",";
        content += "data" + std::to_string(i) + "\n";
    }

    data.resize(content.size() + SIMDCSV_PADDING);
    std::memcpy(data.data(), content.data(), content.size());

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 4);

    bool success = parser.parse_branchless(data.data(), idx, content.size());

    EXPECT_TRUE(success) << "Branchless parser should handle large multithreaded data";
}

TEST_F(BranchlessParsingTest, CustomDelimiterMultithreaded) {
    // Test multi-threaded parsing with semicolon delimiter
    std::vector<uint8_t> data;
    std::string content;

    // Generate large semicolon-delimited data
    content = "A;B;C\n";
    for (int i = 0; i < 10000; i++) {
        content += std::to_string(i) + ";";
        content += "\"value" + std::to_string(i) + "\";";
        content += "data" + std::to_string(i) + "\n";
    }

    data.resize(content.size() + SIMDCSV_PADDING);
    std::memcpy(data.data(), content.data(), content.size());

    simdcsv::two_pass parser;
    simdcsv::index idx = parser.init(data.size(), 4);

    bool success = parser.parse_branchless(data.data(), idx, content.size(),
                                           simdcsv::Dialect::semicolon());

    EXPECT_TRUE(success) << "Branchless parser should handle multi-threaded semicolon delimiter";

    // Verify we found the expected number of separators
    uint64_t total_seps = 0;
    for (int i = 0; i < 4; i++) {
        total_seps += idx.n_indexes[i];
    }
    // Should have ~30000 separators (3 per row * 10001 rows including header)
    EXPECT_GT(total_seps, 30000) << "Should find separators with semicolon delimiter";
}

int main(int argc, char **argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
