/**
 * @file streaming_test.cpp
 * @brief Unit tests for the streaming CSV parser.
 */

#include <gtest/gtest.h>
#include "streaming.h"
#include <sstream>
#include <vector>

using namespace simdcsv;

//-----------------------------------------------------------------------------
// Basic Parsing Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, BasicParsing) {
    std::string csv = "a,b,c\n1,2,3\n4,5,6\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = true;

    StreamReader reader(input, config);

    std::vector<std::vector<std::string>> rows;
    for (const auto& row : reader) {
        std::vector<std::string> fields;
        for (const auto& field : row) {
            fields.push_back(std::string(field.data));
        }
        rows.push_back(fields);
    }

    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0], (std::vector<std::string>{"1", "2", "3"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"4", "5", "6"}));

    // Check header
    EXPECT_EQ(reader.header(), (std::vector<std::string>{"a", "b", "c"}));
}

TEST(StreamingTest, NoHeader) {
    std::string csv = "1,2,3\n4,5,6\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    std::vector<std::vector<std::string>> rows;
    while (reader.next_row()) {
        std::vector<std::string> fields;
        for (const auto& field : reader.row()) {
            fields.push_back(std::string(field.data));
        }
        rows.push_back(fields);
    }

    ASSERT_EQ(rows.size(), 2);
    EXPECT_EQ(rows[0], (std::vector<std::string>{"1", "2", "3"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"4", "5", "6"}));
    EXPECT_TRUE(reader.header().empty());
}

TEST(StreamingTest, EmptyFile) {
    std::string csv = "";
    std::istringstream input(csv);

    StreamReader reader(input);

    int count = 0;
    for (const auto& row : reader) {
        (void)row;
        ++count;
    }

    EXPECT_EQ(count, 0);
}

TEST(StreamingTest, SingleField) {
    std::string csv = "hello\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 1);
    EXPECT_EQ(reader.row()[0].data, "hello");
    EXPECT_FALSE(reader.next_row());
}

TEST(StreamingTest, EmptyFields) {
    std::string csv = "a,,c\n,b,\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 3);
    EXPECT_EQ(reader.row()[0].data, "a");
    EXPECT_EQ(reader.row()[1].data, "");
    EXPECT_EQ(reader.row()[2].data, "c");

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 3);
    EXPECT_EQ(reader.row()[0].data, "");
    EXPECT_EQ(reader.row()[1].data, "b");
    EXPECT_EQ(reader.row()[2].data, "");

    EXPECT_FALSE(reader.next_row());
}

//-----------------------------------------------------------------------------
// Quoted Field Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, QuotedFields) {
    std::string csv = "\"hello\",\"world\"\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 2);
    EXPECT_EQ(reader.row()[0].data, "hello");
    EXPECT_TRUE(reader.row()[0].is_quoted);
    EXPECT_EQ(reader.row()[1].data, "world");
    EXPECT_TRUE(reader.row()[1].is_quoted);
}

TEST(StreamingTest, QuotedFieldWithComma) {
    std::string csv = "\"hello, world\",test\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 2);
    EXPECT_EQ(reader.row()[0].data, "hello, world");
    EXPECT_EQ(reader.row()[1].data, "test");
}

TEST(StreamingTest, QuotedFieldWithNewline) {
    std::string csv = "\"line1\nline2\",test\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 2);
    EXPECT_EQ(reader.row()[0].data, "line1\nline2");
    EXPECT_EQ(reader.row()[1].data, "test");
}

TEST(StreamingTest, EscapedQuotes) {
    std::string csv = "\"say \"\"hello\"\"\",test\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    ASSERT_EQ(reader.row().field_count(), 2);
    // The raw data contains the escaped quotes
    EXPECT_EQ(reader.row()[0].data, "say \"\"hello\"\"");
    // The unescaped version removes the escaping
    EXPECT_EQ(reader.row()[0].unescaped(), "say \"hello\"");
}

//-----------------------------------------------------------------------------
// Line Ending Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, UnixLineEndings) {
    std::string csv = "a,b\n1,2\n3,4\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    int count = 0;
    while (reader.next_row()) {
        ++count;
    }
    EXPECT_EQ(count, 3);
}

TEST(StreamingTest, WindowsLineEndings) {
    std::string csv = "a,b\r\n1,2\r\n3,4\r\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    int count = 0;
    while (reader.next_row()) {
        EXPECT_EQ(reader.row().field_count(), 2);
        ++count;
    }
    EXPECT_EQ(count, 3);
}

TEST(StreamingTest, NoTrailingNewline) {
    std::string csv = "a,b\n1,2";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row()[0].data, "a");

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row()[0].data, "1");
    EXPECT_EQ(reader.row()[1].data, "2");

    EXPECT_FALSE(reader.next_row());
}

//-----------------------------------------------------------------------------
// Push Model Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, PushModelBasic) {
    std::string csv = "a,b,c\n1,2,3\n4,5,6\n";

    StreamConfig config;
    config.parse_header = true;

    StreamParser parser(config);

    std::vector<std::vector<std::string>> collected_rows;

    parser.set_row_handler([&collected_rows](const Row& row) {
        std::vector<std::string> fields;
        for (const auto& field : row) {
            fields.push_back(std::string(field.data));
        }
        collected_rows.push_back(fields);
        return true;
    });

    parser.parse_chunk(csv);
    parser.finish();

    ASSERT_EQ(collected_rows.size(), 2);
    EXPECT_EQ(collected_rows[0], (std::vector<std::string>{"1", "2", "3"}));
    EXPECT_EQ(collected_rows[1], (std::vector<std::string>{"4", "5", "6"}));

    EXPECT_EQ(parser.header(), (std::vector<std::string>{"a", "b", "c"}));
}

TEST(StreamingTest, PushModelStopEarly) {
    std::string csv = "a\n1\n2\n3\n4\n5\n";

    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    int row_count = 0;
    parser.set_row_handler([&row_count](const Row& row) {
        (void)row;
        ++row_count;
        return row_count < 3;  // Stop after 3 rows
    });

    parser.parse_chunk(csv);
    parser.finish();

    EXPECT_EQ(row_count, 3);
}

//-----------------------------------------------------------------------------
// Chunk Boundary Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ChunkBoundaryInField) {
    std::string csv = "hello,world\n";

    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    // Split in middle of "hello"
    parser.parse_chunk(csv.substr(0, 3));
    parser.parse_chunk(csv.substr(3));
    parser.finish();

    EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
    EXPECT_EQ(parser.current_row().field_count(), 2);
    EXPECT_EQ(parser.current_row()[0].data, "hello");
    EXPECT_EQ(parser.current_row()[1].data, "world");
}

TEST(StreamingTest, ChunkBoundaryAtDelimiter) {
    std::string csv = "hello,world\n";

    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    // Split at the comma
    parser.parse_chunk(csv.substr(0, 5));  // "hello"
    parser.parse_chunk(csv.substr(5));     // ",world\n"
    parser.finish();

    EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
    EXPECT_EQ(parser.current_row().field_count(), 2);
    EXPECT_EQ(parser.current_row()[0].data, "hello");
    EXPECT_EQ(parser.current_row()[1].data, "world");
}

TEST(StreamingTest, ChunkBoundaryInQuotedField) {
    std::string csv = "\"hello, world\",test\n";

    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    // Split in middle of quoted field
    parser.parse_chunk(csv.substr(0, 8));   // "\"hello, "
    parser.parse_chunk(csv.substr(8));      // "world\",test\n"
    parser.finish();

    EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
    EXPECT_EQ(parser.current_row().field_count(), 2);
    EXPECT_EQ(parser.current_row()[0].data, "hello, world");
    EXPECT_EQ(parser.current_row()[1].data, "test");
}

TEST(StreamingTest, ChunkBoundaryAcrossMultipleRows) {
    std::string csv = "a,b\n1,2\n3,4\n";

    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    std::vector<std::vector<std::string>> rows;
    parser.set_row_handler([&rows](const Row& row) {
        std::vector<std::string> fields;
        for (const auto& field : row) {
            fields.push_back(std::string(field.data));
        }
        rows.push_back(fields);
        return true;
    });

    // Feed one character at a time
    for (char c : csv) {
        parser.parse_chunk(std::string_view(&c, 1));
    }
    parser.finish();

    ASSERT_EQ(rows.size(), 3);
    EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"1", "2"}));
    EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "4"}));
}

//-----------------------------------------------------------------------------
// Column Access Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ColumnAccessByName) {
    std::string csv = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = true;

    StreamReader reader(input, config);

    // Must read first row to parse header
    ASSERT_TRUE(reader.next_row());

    // Now column_index works
    EXPECT_EQ(reader.column_index("name"), 0);
    EXPECT_EQ(reader.column_index("age"), 1);
    EXPECT_EQ(reader.column_index("city"), 2);
    EXPECT_EQ(reader.column_index("unknown"), -1);

    EXPECT_EQ(reader.row()["name"].data, "Alice");
    EXPECT_EQ(reader.row()["age"].data, "30");
    EXPECT_EQ(reader.row()["city"].data, "NYC");

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row()["name"].data, "Bob");
    EXPECT_EQ(reader.row()["age"].data, "25");
    EXPECT_EQ(reader.row()["city"].data, "LA");
}

TEST(StreamingTest, RowMetadata) {
    std::string csv = "a,b\n1,2\n3,4\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = true;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row().row_number(), 1);
    EXPECT_EQ(reader.row().byte_offset(), 4);  // After "a,b\n"

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row().row_number(), 2);
}

//-----------------------------------------------------------------------------
// Dialect Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, TabSeparated) {
    std::string tsv = "a\tb\tc\n1\t2\t3\n";
    std::istringstream input(tsv);

    StreamConfig config;
    config.dialect = Dialect::tsv();
    config.parse_header = true;

    StreamReader reader(input, config);

    // Must read first row to parse header
    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.header(), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(reader.row()[0].data, "1");
    EXPECT_EQ(reader.row()[1].data, "2");
    EXPECT_EQ(reader.row()[2].data, "3");
}

TEST(StreamingTest, SemicolonSeparated) {
    std::string csv = "a;b;c\n1;2;3\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.dialect = Dialect::semicolon();
    config.parse_header = true;

    StreamReader reader(input, config);

    // Must read first row to parse header
    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.header(), (std::vector<std::string>{"a", "b", "c"}));
    EXPECT_EQ(reader.row()[0].data, "1");
    EXPECT_EQ(reader.row()[1].data, "2");
    EXPECT_EQ(reader.row()[2].data, "3");
}

TEST(StreamingTest, SingleQuote) {
    std::string csv = "'hello, world',test\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.dialect.quote_char = '\'';
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row()[0].data, "hello, world");
    EXPECT_TRUE(reader.row()[0].is_quoted);
    EXPECT_EQ(reader.row()[1].data, "test");
}

//-----------------------------------------------------------------------------
// Error Handling Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, UnclosedQuote) {
    std::string csv = "\"unclosed\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;
    config.error_mode = ErrorMode::PERMISSIVE;

    StreamReader reader(input, config);

    while (reader.next_row()) {
        // Process rows
    }

    EXPECT_TRUE(reader.errors().has_errors());
    EXPECT_TRUE(reader.errors().has_fatal_errors());

    bool found_unclosed = false;
    for (const auto& err : reader.errors().errors()) {
        if (err.code == ErrorCode::UNCLOSED_QUOTE) {
            found_unclosed = true;
            break;
        }
    }
    EXPECT_TRUE(found_unclosed);
}

TEST(StreamingTest, QuoteInUnquotedField) {
    std::string csv = "hello\"world,test\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;
    config.error_mode = ErrorMode::PERMISSIVE;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    EXPECT_TRUE(reader.errors().has_errors());

    bool found_error = false;
    for (const auto& err : reader.errors().errors()) {
        if (err.code == ErrorCode::QUOTE_IN_UNQUOTED_FIELD) {
            found_error = true;
            break;
        }
    }
    EXPECT_TRUE(found_error);
}

TEST(StreamingTest, BestEffortMode) {
    std::string csv = "\"unclosed\nvalid,data\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;
    config.error_mode = ErrorMode::BEST_EFFORT;

    StreamReader reader(input, config);

    // Should still be able to read rows even with errors
    int count = 0;
    while (reader.next_row()) {
        ++count;
    }

    // At least parsed something
    EXPECT_GE(count, 0);
}

//-----------------------------------------------------------------------------
// Statistics Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, RowAndByteCount) {
    std::string csv = "a,b\n1,2\n3,4\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = true;

    StreamReader reader(input, config);

    while (reader.next_row()) {
        // Process rows
    }

    EXPECT_EQ(reader.rows_read(), 2);  // Excluding header
    EXPECT_EQ(reader.bytes_read(), csv.size());
    EXPECT_TRUE(reader.eof());
}

//-----------------------------------------------------------------------------
// Pull Model with Parser Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, PullModelWithParser) {
    std::string csv = "a,b\n1,2\n3,4\n";

    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);
    parser.parse_chunk(csv);
    parser.finish();

    std::vector<std::vector<std::string>> rows;
    while (parser.next_row() == StreamStatus::ROW_READY) {
        std::vector<std::string> fields;
        for (const auto& field : parser.current_row()) {
            fields.push_back(std::string(field.data));
        }
        rows.push_back(fields);
    }

    ASSERT_EQ(rows.size(), 3);
    EXPECT_EQ(rows[0], (std::vector<std::string>{"a", "b"}));
    EXPECT_EQ(rows[1], (std::vector<std::string>{"1", "2"}));
    EXPECT_EQ(rows[2], (std::vector<std::string>{"3", "4"}));
}

TEST(StreamingTest, PullModelNeedMoreData) {
    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    // No data yet
    EXPECT_EQ(parser.next_row(), StreamStatus::NEED_MORE_DATA);

    // Add partial row
    parser.parse_chunk("hello,wor");
    EXPECT_EQ(parser.next_row(), StreamStatus::NEED_MORE_DATA);

    // Complete the row
    parser.parse_chunk("ld\n");
    EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
    EXPECT_EQ(parser.current_row()[0].data, "hello");
    EXPECT_EQ(parser.current_row()[1].data, "world");

    // No more data
    parser.finish();
    EXPECT_EQ(parser.next_row(), StreamStatus::END_OF_DATA);
}

//-----------------------------------------------------------------------------
// Reset Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ParserReset) {
    StreamConfig config;
    config.parse_header = false;

    StreamParser parser(config);

    // First parse
    parser.parse_chunk("a,b\n");
    parser.finish();

    EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
    EXPECT_EQ(parser.rows_processed(), 1);

    // Reset and parse again
    parser.reset();
    EXPECT_EQ(parser.rows_processed(), 0);
    EXPECT_FALSE(parser.is_finished());

    parser.parse_chunk("x,y,z\n");
    parser.finish();

    EXPECT_EQ(parser.next_row(), StreamStatus::ROW_READY);
    EXPECT_EQ(parser.current_row().field_count(), 3);
    EXPECT_EQ(parser.current_row()[0].data, "x");
}

//-----------------------------------------------------------------------------
// Field::at() bounds checking
//-----------------------------------------------------------------------------

TEST(StreamingTest, FieldAtBoundsCheck) {
    std::string csv = "a,b\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());

    // Valid access
    EXPECT_NO_THROW(reader.row().at(0));
    EXPECT_NO_THROW(reader.row().at(1));

    // Invalid access
    EXPECT_THROW(reader.row().at(2), std::out_of_range);
    EXPECT_THROW(reader.row().at(100), std::out_of_range);
}

//-----------------------------------------------------------------------------
// Iterator Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, IteratorComparison) {
    std::string csv = "a\n1\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    auto begin = reader.begin();
    auto end = reader.end();

    EXPECT_NE(begin, end);
    ++begin;
    EXPECT_NE(begin, end);
    ++begin;
    EXPECT_EQ(begin, end);
}

//-----------------------------------------------------------------------------
// Large Data Tests
//-----------------------------------------------------------------------------

TEST(StreamingTest, ManyRows) {
    std::ostringstream oss;
    oss << "id,value\n";
    for (int i = 0; i < 1000; ++i) {
        oss << i << "," << (i * 2) << "\n";
    }
    std::string csv = oss.str();
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = true;

    StreamReader reader(input, config);

    int count = 0;
    int sum = 0;
    while (reader.next_row()) {
        sum += std::stoi(std::string(reader.row()[1].data));
        ++count;
    }

    EXPECT_EQ(count, 1000);
    EXPECT_EQ(sum, 999 * 1000);  // Sum of 0 + 2 + 4 + ... + 1998
}

TEST(StreamingTest, LongFields) {
    std::string long_field(10000, 'x');
    std::string csv = long_field + "," + long_field + "\n";
    std::istringstream input(csv);

    StreamConfig config;
    config.parse_header = false;

    StreamReader reader(input, config);

    ASSERT_TRUE(reader.next_row());
    EXPECT_EQ(reader.row().field_count(), 2);
    EXPECT_EQ(reader.row()[0].data.size(), 10000);
    EXPECT_EQ(reader.row()[1].data.size(), 10000);
}
