/**
 * CLI Integration Tests for scsv (cli.cpp)
 *
 * Tests the scsv command-line tool by spawning the process with various
 * arguments and validating exit codes and output.
 *
 * SECURITY NOTE: The CliRunner class uses popen() with shell execution.
 * All test file paths MUST come from trusted test fixtures only.
 * The runWithFileStdin() method uses file redirection with paths that are
 * hardcoded in the test file - never use with user-provided input.
 */

#include <gtest/gtest.h>
#include <sys/wait.h>
#include <unistd.h>

#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <memory>
#include <sstream>
#include <string>

// Helper class to run CLI commands and capture output
class CliRunner {
 public:
  struct Result {
    int exit_code;
    std::string output;  // Combined stdout/stderr output
  };

  // Run scsv with given arguments
  // Note: stderr is redirected to stdout for simpler output capture
  static Result run(const std::string& args) {
    Result result;

    // Build command - scsv binary is in the build directory
    std::string cmd = "./scsv " + args + " 2>&1";

    // Open pipe to command
    std::array<char, 4096> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);

    if (!pipe) {
      result.exit_code = -1;
      result.output = "Failed to run command";
      return result;
    }

    // Read output
    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      result.output += buffer.data();
    }

    // Get exit code - properly handle signal termination
    int status = pclose(pipe.release());
    if (WIFEXITED(status)) {
      result.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      result.exit_code = 128 + WTERMSIG(status);  // Common convention
    } else {
      result.exit_code = -1;
    }

    return result;
  }

  // Run with stdin from a file via redirection
  // Note: file_path is expected to be a trusted path from test fixtures
  static Result runWithFileStdin(const std::string& args, const std::string& file_path) {
    Result result;
    std::string cmd = "./scsv " + args + " < " + file_path + " 2>&1";

    std::array<char, 4096> buffer;
    std::unique_ptr<FILE, decltype(&pclose)> pipe(popen(cmd.c_str(), "r"), pclose);

    if (!pipe) {
      result.exit_code = -1;
      result.output = "Failed to run command";
      return result;
    }

    while (fgets(buffer.data(), buffer.size(), pipe.get()) != nullptr) {
      result.output += buffer.data();
    }

    // Get exit code - properly handle signal termination
    int status = pclose(pipe.release());
    if (WIFEXITED(status)) {
      result.exit_code = WEXITSTATUS(status);
    } else if (WIFSIGNALED(status)) {
      result.exit_code = 128 + WTERMSIG(status);
    } else {
      result.exit_code = -1;
    }

    return result;
  }
};

class CliTest : public ::testing::Test {
 protected:
  static std::string testDataPath(const std::string& relative_path) {
    return "test/data/" + relative_path;
  }
};

// =============================================================================
// Help and Version Tests
// =============================================================================

TEST_F(CliTest, NoArgsShowsUsage) {
  auto result = CliRunner::run("");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Usage:") != std::string::npos);
}

TEST_F(CliTest, HelpFlagShort) {
  auto result = CliRunner::run("-h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Usage:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Commands:") != std::string::npos);
}

TEST_F(CliTest, HelpFlagLong) {
  auto result = CliRunner::run("--help");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Usage:") != std::string::npos);
}

TEST_F(CliTest, VersionFlagShort) {
  auto result = CliRunner::run("-v");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("scsv version") != std::string::npos);
}

TEST_F(CliTest, VersionFlagLong) {
  auto result = CliRunner::run("--version");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("scsv version") != std::string::npos);
}

TEST_F(CliTest, UnknownCommandShowsError) {
  auto result = CliRunner::run("unknown");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Unknown command") != std::string::npos);
}

// =============================================================================
// Count Command Tests
// =============================================================================

TEST_F(CliTest, CountBasicFile) {
  auto result = CliRunner::run("count " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // simple.csv has header + 3 data rows, count subtracts header by default
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountNoHeader) {
  auto result = CliRunner::run("count -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Without header flag, counts all 4 rows
  EXPECT_TRUE(result.output.find("4") != std::string::npos);
}

TEST_F(CliTest, CountEmptyFile) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("0") != std::string::npos);
}

TEST_F(CliTest, CountManyRows) {
  auto result = CliRunner::run("count " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should successfully count rows without error
}

TEST_F(CliTest, CountWithThreads) {
  auto result = CliRunner::run("count -t 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountQuotedFields) {
  auto result = CliRunner::run("count " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // escaped_quotes.csv has header + 5 data rows
  EXPECT_TRUE(result.output.find("5") != std::string::npos);
}

// =============================================================================
// Head Command Tests
// =============================================================================

TEST_F(CliTest, HeadDefault) {
  auto result = CliRunner::run("head " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header and rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
}

TEST_F(CliTest, HeadWithNumRows) {
  auto result = CliRunner::run("head -n 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + 2 data rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  // Third data row should NOT be present
  EXPECT_TRUE(result.output.find("7,8,9") == std::string::npos);
}

TEST_F(CliTest, HeadZeroRows) {
  auto result = CliRunner::run("head -n 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output nothing (or just header if that counts)
}

TEST_F(CliTest, HeadEmptyFile) {
  auto result = CliRunner::run("head " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadQuotedNewlines) {
  auto result = CliRunner::run("head " + testDataPath("quoted/newlines_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Select Command Tests
// =============================================================================

TEST_F(CliTest, SelectByIndex) {
  auto result = CliRunner::run("select -c 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("1") != std::string::npos);
  // Should NOT contain columns B or C
  EXPECT_TRUE(result.output.find("B") == std::string::npos);
}

TEST_F(CliTest, SelectByName) {
  auto result = CliRunner::run("select -c B " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
  EXPECT_TRUE(result.output.find("2") != std::string::npos);
}

TEST_F(CliTest, SelectMultipleColumns) {
  auto result = CliRunner::run("select -c 0,2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
  // B should not be present
  EXPECT_TRUE(result.output.find("B") == std::string::npos);
}

TEST_F(CliTest, SelectInvalidColumnIndex) {
  auto result = CliRunner::run("select -c 99 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("out of range") != std::string::npos);
}

TEST_F(CliTest, SelectInvalidColumnName) {
  auto result = CliRunner::run("select -c nonexistent " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("not found") != std::string::npos);
}

TEST_F(CliTest, SelectMissingColumnArg) {
  auto result = CliRunner::run("select " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("-c option required") != std::string::npos);
}

TEST_F(CliTest, SelectNoHeaderWithColumnName) {
  auto result = CliRunner::run("select -H -c name " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Cannot use column names") != std::string::npos);
}

// =============================================================================
// Info Command Tests
// =============================================================================

TEST_F(CliTest, InfoBasicFile) {
  auto result = CliRunner::run("info " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Source:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Size:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Rows:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Columns:") != std::string::npos);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);  // columns
}

TEST_F(CliTest, InfoShowsColumnNames) {
  auto result = CliRunner::run("info " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Column names:") != std::string::npos);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
}

TEST_F(CliTest, InfoNoHeader) {
  auto result = CliRunner::run("info -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should NOT show column names section when no header
  EXPECT_TRUE(result.output.find("Column names:") == std::string::npos);
}

TEST_F(CliTest, InfoEmptyFile) {
  auto result = CliRunner::run("info " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Size: 0 bytes") != std::string::npos);
}

// =============================================================================
// Pretty Command Tests
// =============================================================================

TEST_F(CliTest, PrettyBasicFile) {
  auto result = CliRunner::run("pretty " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Pretty output should have table borders
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
  EXPECT_TRUE(result.output.find("|") != std::string::npos);
  EXPECT_TRUE(result.output.find("-") != std::string::npos);
}

TEST_F(CliTest, PrettyWithNumRows) {
  auto result = CliRunner::run("pretty -n 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have table format
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
  // Should have header and one data row
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
}

TEST_F(CliTest, PrettyEmptyFile) {
  auto result = CliRunner::run("pretty " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Delimiter and Dialect Tests
// =============================================================================

TEST_F(CliTest, TabDelimiter) {
  auto result = CliRunner::run("count -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, SemicolonDelimiter) {
  auto result = CliRunner::run("count -d semicolon " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, PipeDelimiter) {
  auto result = CliRunner::run("count -d pipe " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, SingleCharDelimiter) {
  auto result = CliRunner::run("count -d , " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, HeadWithTabDelimiter) {
  auto result = CliRunner::run("head -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Output should use tab delimiter
  EXPECT_TRUE(result.output.find("\t") != std::string::npos);
}

TEST_F(CliTest, AutoDetectDialect) {
  // Note: count uses optimized row counting that doesn't parse the file,
  // so we use head command which actually parses and shows auto-detect message
  auto result = CliRunner::run("head -a " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should auto-detect and report the dialect
  EXPECT_TRUE(result.output.find("Auto-detected") != std::string::npos);
}

// =============================================================================
// Error Handling Tests
// =============================================================================

TEST_F(CliTest, NonexistentFile) {
  auto result = CliRunner::run("count nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos ||
              result.output.find("Could not load") != std::string::npos);
}

TEST_F(CliTest, InvalidThreadCount) {
  auto result = CliRunner::run("count -t 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Thread count") != std::string::npos);
}

TEST_F(CliTest, InvalidThreadCountTooHigh) {
  auto result = CliRunner::run("count -t 999 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Thread count") != std::string::npos);
}

TEST_F(CliTest, InvalidRowCount) {
  auto result = CliRunner::run("head -n abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Invalid row count") != std::string::npos);
}

TEST_F(CliTest, NegativeRowCount) {
  auto result = CliRunner::run("head -n -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, InvalidQuoteChar) {
  auto result = CliRunner::run("count -q abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Quote character must be a single character") !=
              std::string::npos);
}

// =============================================================================
// Stdin Input Tests
// =============================================================================

TEST_F(CliTest, CountFromStdin) {
  auto result = CliRunner::runWithFileStdin("count -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountFromStdinNoExplicitDash) {
  auto result = CliRunner::runWithFileStdin("count", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, HeadFromStdin) {
  auto result = CliRunner::runWithFileStdin("head -n 2 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, InfoFromStdin) {
  auto result = CliRunner::runWithFileStdin("info -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("<stdin>") != std::string::npos);
}

// =============================================================================
// Edge Cases Tests
// =============================================================================

TEST_F(CliTest, SingleColumn) {
  auto result = CliRunner::run("count " + testDataPath("basic/single_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, WideColumns) {
  auto result = CliRunner::run("info " + testDataPath("basic/wide_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, EmptyFields) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/empty_fields.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, WhitespaceFields) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/whitespace_fields.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CrlfLineEndings) {
  auto result = CliRunner::run("count " + testDataPath("line_endings/crlf.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CrLineEndings) {
  auto result = CliRunner::run("count " + testDataPath("line_endings/cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, NoFinalNewline) {
  auto result = CliRunner::run("count " + testDataPath("line_endings/no_final_newline.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, QuotedFieldsWithNewlines) {
  auto result = CliRunner::run("count " + testDataPath("quoted/newlines_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, EscapedQuotes) {
  auto result = CliRunner::run("head " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SingleRowHeaderOnly) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/single_row_header_only.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("0") != std::string::npos);
}

// =============================================================================
// Command Help within Command Tests
// =============================================================================

TEST_F(CliTest, HelpAfterCommand) {
  auto result = CliRunner::run("count -h");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Usage:") != std::string::npos);
}

TEST_F(CliTest, VersionAfterCommand) {
  auto result = CliRunner::run("head -v");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("scsv version") != std::string::npos);
}

// =============================================================================
// Combined Options Tests
// =============================================================================

TEST_F(CliTest, HeadWithMultipleOptions) {
  auto result =
      CliRunner::run("head -n 2 -t 2 -d comma " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, SelectWithMultipleColumns) {
  auto result = CliRunner::run("select -c A,C " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("C") != std::string::npos);
}

TEST_F(CliTest, InfoWithAutoDetect) {
  auto result = CliRunner::run("info -a " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Auto-detected") != std::string::npos);
}

// =============================================================================
// Malformed CSV Handling Tests
// =============================================================================

TEST_F(CliTest, MalformedUnclosedQuote) {
  // File has an unclosed quote in the middle - parser should handle gracefully
  auto result = CliRunner::run("count " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser processes what it can - row count may vary based on quote interpretation
  // but should return some reasonable value (not crash or hang)
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedUnclosedQuoteEof) {
  // Quote never closes until end of file
  auto result = CliRunner::run("head " + testDataPath("malformed/unclosed_quote_eof.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output what it can parse
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, MalformedUnescapedQuoteInQuoted) {
  // Has unescaped quote inside quoted field: "has " unescaped quote"
  auto result = CliRunner::run("count " + testDataPath("malformed/unescaped_quote_in_quoted.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser handles this - may interpret differently than expected
}

TEST_F(CliTest, MalformedQuoteNotAtStart) {
  // Quote appears mid-field: x"quoted"
  auto result = CliRunner::run("head " + testDataPath("malformed/quote_not_at_start.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser should process the file
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, MalformedTripleQuote) {
  // Contains triple quotes which is ambiguous
  auto result = CliRunner::run("count " + testDataPath("malformed/triple_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should process the file and return a count
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedNullByte) {
  // Contains a null byte in data
  auto result = CliRunner::run("count " + testDataPath("malformed/null_byte.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should count rows despite null byte
  EXPECT_TRUE(result.output.find("2") != std::string::npos);
}

TEST_F(CliTest, MalformedInconsistentColumns) {
  // Rows have different numbers of columns
  auto result = CliRunner::run("info " + testDataPath("malformed/inconsistent_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Info command should still work
  EXPECT_TRUE(result.output.find("Columns:") != std::string::npos);
}

TEST_F(CliTest, MalformedEmptyHeader) {
  // Header row has empty column names
  auto result = CliRunner::run("head " + testDataPath("malformed/empty_header.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, MalformedDuplicateColumnNames) {
  // Header has duplicate column names
  auto result = CliRunner::run("info " + testDataPath("malformed/duplicate_column_names.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Info command should work
  EXPECT_TRUE(result.output.find("Column names:") != std::string::npos);
}

TEST_F(CliTest, MalformedMixedLineEndings) {
  // File has mix of CRLF, LF, and CR line endings
  auto result = CliRunner::run("count " + testDataPath("malformed/mixed_line_endings.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should process the file and return a count
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedTrailingQuote) {
  // Field ends with quote in unexpected position
  auto result = CliRunner::run("head " + testDataPath("malformed/trailing_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should produce some output
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedMultipleErrors) {
  // File with multiple types of malformed content
  auto result = CliRunner::run("count " + testDataPath("malformed/multiple_errors.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should process the file and return a count
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedSelectFromBadFile) {
  // Try selecting columns from malformed file
  auto result = CliRunner::run("select -c 0 " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output first column from parseable rows
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
}

TEST_F(CliTest, MalformedPrettyFromBadFile) {
  // Pretty print of malformed file
  auto result = CliRunner::run("pretty -n 5 " + testDataPath("malformed/inconsistent_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should still produce table output
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

// =============================================================================
// Large File / Parallel Processing Tests
// =============================================================================

TEST_F(CliTest, LargeFileParallelCount) {
  // Test parallel counting on a multi-MB file
  auto result = CliRunner::run("count -t 4 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should return a valid count without error
}

TEST_F(CliTest, LargeFileParallelCountVerify) {
  // Verify parallel counting produces same result as single-threaded
  auto single = CliRunner::run("count -t 1 " + testDataPath("large/parallel_chunk_boundary.csv"));
  auto parallel = CliRunner::run("count -t 4 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(single.exit_code, 0);
  EXPECT_EQ(parallel.exit_code, 0);
  // Both should produce the same count
  EXPECT_EQ(single.output, parallel.output);
}

TEST_F(CliTest, LargeFileParallelMaxThreads) {
  // Test with higher thread count
  auto result = CliRunner::run("count -t 8 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, LargeFileHead) {
  // Head command on large file should be fast (only reads what's needed)
  auto result = CliRunner::run("head -n 5 " + testDataPath("large/parallel_chunk_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + 5 data rows
}

TEST_F(CliTest, LargeFieldFile) {
  // File with a very large field (70KB)
  auto result = CliRunner::run("count " + testDataPath("large/large_field.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, LongLineFile) {
  // File with very long lines
  auto result = CliRunner::run("head -n 2 " + testDataPath("large/long_line.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, BufferBoundaryFile) {
  // File designed to test SIMD buffer boundaries (200 rows)
  auto result = CliRunner::run("count -t 2 " + testDataPath("large/buffer_boundary.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should count all 200 rows
  EXPECT_TRUE(result.output.find("200") != std::string::npos);
}

// =============================================================================
// Invalid Option Combinations Tests
// =============================================================================

TEST_F(CliTest, AutoDetectWithExplicitDelimiter) {
  // When -a (auto-detect) is used with explicit -d, auto-detect should take precedence
  auto result = CliRunner::run("head -a -d semicolon " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Auto-detect should report its finding
  EXPECT_TRUE(result.output.find("Auto-detected") != std::string::npos);
}

TEST_F(CliTest, AutoDetectWithExplicitDelimiterOutput) {
  // Verify the auto-detect correctly identifies comma-delimited file
  auto result = CliRunner::run("info -a -d semicolon " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should auto-detect comma, not use semicolon
  EXPECT_TRUE(result.output.find("delimiter=','") != std::string::npos);
}

TEST_F(CliTest, NoHeaderWithColumnNameSelect) {
  // Already tested, but included here for completeness of option combinations
  auto result = CliRunner::run("select -H -c name " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Cannot use column names") != std::string::npos);
}

TEST_F(CliTest, ExcessiveThreadsInvalid) {
  // More than 256 threads is invalid
  auto result = CliRunner::run("count -t 300 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, NegativeThreadCount) {
  // Negative thread count
  auto result = CliRunner::run("count -t -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, HeadWithZeroAndFile) {
  // head -n 0 should show nothing (or just header depending on implementation)
  auto result = CliRunner::run("head -n 0 -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectMissingFile) {
  // Select command with nonexistent file
  auto result = CliRunner::run("select -c 0 nonexistent.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos ||
              result.output.find("Could not load") != std::string::npos);
}

TEST_F(CliTest, MultipleDelimiterSpecs) {
  // Multiple -d flags - last one should win
  auto result = CliRunner::run("count -d tab -d comma " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should use comma (the last specified)
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

// =============================================================================
// Encoding Tests
// =============================================================================

TEST_F(CliTest, Utf8BomFile) {
  // File with UTF-8 BOM
  auto result = CliRunner::run("count " + testDataPath("encoding/utf8_bom.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, Latin1Encoding) {
  // File with Latin-1 encoding (non-UTF8 but valid bytes)
  auto result = CliRunner::run("head " + testDataPath("encoding/latin1.csv"));
  EXPECT_EQ(result.exit_code, 0);
}
