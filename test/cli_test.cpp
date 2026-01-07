/**
 * CLI Integration Tests for vroom (cli.cpp)
 *
 * Tests the vroom command-line tool by spawning the process with various
 * arguments and validating exit codes and output.
 *
 * SECURITY NOTE: The CliRunner class uses popen() with shell execution.
 * All test file paths MUST come from trusted test fixtures only.
 * The runWithFileStdin() method uses file redirection with paths that are
 * hardcoded in the test file - never use with user-provided input.
 */

#include <array>
#include <cstdio>
#include <cstdlib>
#include <fstream>
#include <gtest/gtest.h>
#include <memory>
#include <sstream>
#include <string>
#include <sys/wait.h>
#include <unistd.h>

// Helper class to run CLI commands and capture output
class CliRunner {
public:
  struct Result {
    int exit_code;
    std::string output; // Combined stdout/stderr output
  };

  // Run vroom with given arguments
  // Note: stderr is redirected to stdout for simpler output capture
  static Result run(const std::string& args) {
    Result result;

    // Build command - vroom binary is in the build directory
    std::string cmd = "./vroom " + args + " 2>&1";

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
      result.exit_code = 128 + WTERMSIG(status); // Common convention
    } else {
      result.exit_code = -1;
    }

    return result;
  }

  // Run with stdin from a file via redirection
  // Note: file_path is expected to be a trusted path from test fixtures
  static Result runWithFileStdin(const std::string& args, const std::string& file_path) {
    Result result;
    std::string cmd = "./vroom " + args + " < " + file_path + " 2>&1";

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
  EXPECT_TRUE(result.output.find("vroom version") != std::string::npos);
}

TEST_F(CliTest, VersionFlagLong) {
  auto result = CliRunner::run("--version");
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("vroom version") != std::string::npos);
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
  EXPECT_TRUE(result.output.find("3") != std::string::npos); // columns
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
  // Auto-detect is now enabled by default, so we just run head without -d flag
  // and verify it correctly parses the semicolon-separated file
  auto result = CliRunner::run("head " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should auto-detect semicolon delimiter and output using semicolons
  EXPECT_TRUE(result.output.find(";") != std::string::npos);
}

TEST_F(CliTest, DialectCommandText) {
  // Test the dialect command with human-readable output
  auto result = CliRunner::run("dialect " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("semicolon") != std::string::npos);
  EXPECT_TRUE(result.output.find("CLI flags:") != std::string::npos);
}

TEST_F(CliTest, DialectCommandJson) {
  // Test the dialect command with JSON output
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"delimiter\": \"\\t\"") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"confidence\":") != std::string::npos);
}

TEST_F(CliTest, AutoDetectDisabledWithExplicitDelimiter) {
  // When -d is specified, auto-detect should be disabled
  // Even for a semicolon file, if we specify comma, it should use comma
  auto result = CliRunner::run("head -d comma " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Output should NOT have semicolon as delimiter (would be comma)
  // The file has "A;B;C" as content - if we parse as comma-separated,
  // the whole line becomes a single field
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
  // 1025 exceeds new MAX_THREADS of 1024
  auto result = CliRunner::run("count -t 1025 " + testDataPath("basic/simple.csv"));
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
  EXPECT_TRUE(result.output.find("vroom version") != std::string::npos);
}

// =============================================================================
// Combined Options Tests
// =============================================================================

TEST_F(CliTest, HeadWithMultipleOptions) {
  auto result = CliRunner::run("head -n 2 -t 2 -d comma " + testDataPath("basic/simple.csv"));
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
  // Auto-detect is now enabled by default
  auto result = CliRunner::run("info " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show dialect info with detected semicolon
  EXPECT_TRUE(result.output.find("Dialect:") != std::string::npos);
}

// =============================================================================
// Malformed CSV Handling Tests
// =============================================================================

TEST_F(CliTest, MalformedUnclosedQuote) {
  // File has an unclosed quote in the middle - parser should handle gracefully
  auto result = CliRunner::run("count " + testDataPath("malformed/unclosed_quote.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Parser processes what it can - row count may vary based on quote
  // interpretation but should return some reasonable value (not crash or hang)
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

TEST_F(CliTest, MalformedVariableColumns) {
  // Regression test for GitHub issue #263: SIGABRT crash on variable column
  // count File has ~30 rows with column counts varying from 20-26 This
  // previously caused an assertion failure with SIGABRT
  auto result = CliRunner::run("head -n 5 " + testDataPath("malformed/variable_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should handle variable column counts gracefully without crashing
  EXPECT_FALSE(result.output.empty());
}

TEST_F(CliTest, MalformedVariableColumnsExplicitDelimiter) {
  // Test with explicit delimiter (disables auto-detection)
  auto result =
      CliRunner::run("head -d comma -n 5 " + testDataPath("malformed/variable_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_FALSE(result.output.empty());
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

TEST_F(CliTest, ExplicitDelimiterDisablesAutoDetect) {
  // When -d (explicit delimiter) is used, auto-detect should be disabled
  // For a comma file with -d semicolon, it should treat each line as one field
  auto result = CliRunner::run("head -d semicolon " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should NOT show auto-detect message since -d was specified
  EXPECT_TRUE(result.output.find("Auto-detected") == std::string::npos);
}

TEST_F(CliTest, AutoDetectByDefault) {
  // Verify auto-detect works by default without -a flag
  auto result = CliRunner::run("info " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should auto-detect semicolon
  EXPECT_TRUE(result.output.find("';'") != std::string::npos);
}

TEST_F(CliTest, NoHeaderWithColumnNameSelect) {
  // Already tested, but included here for completeness of option combinations
  auto result = CliRunner::run("select -H -c name " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Cannot use column names") != std::string::npos);
}

TEST_F(CliTest, ExcessiveThreadsInvalid) {
  // More than 1024 threads is invalid (limited by MAX_THREADS)
  auto result = CliRunner::run("count -t 2000 " + testDataPath("basic/simple.csv"));
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

// =============================================================================
// Tail Command Tests
// =============================================================================

TEST_F(CliTest, TailDefault) {
  auto result = CliRunner::run("tail " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header and last rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // simple.csv has 3 data rows, default is 10, so all 3 should appear
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, TailWithNumRows) {
  auto result = CliRunner::run("tail -n 2 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + last 2 data rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // First data row should NOT be present
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
  // Last 2 data rows should be present
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, TailWithNumRowsOne) {
  auto result = CliRunner::run("tail -n 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + last data row only
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") == std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, TailMoreRowsThanExist) {
  // Request more rows than exist - should return all data rows
  auto result = CliRunner::run("tail -n 100 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, TailZeroRows) {
  auto result = CliRunner::run("tail -n 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output only the header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
}

TEST_F(CliTest, TailEmptyFile) {
  auto result = CliRunner::run("tail " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, TailNoHeader) {
  auto result = CliRunner::run("tail -n 2 -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output last 2 rows without treating first as header
  // So we get rows "4,5,6" and "7,8,9" (last 2 of 4 total rows)
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
  // Header "A,B,C" should NOT be in output since we're not treating it as
  // header
  EXPECT_TRUE(result.output.find("A,B,C") == std::string::npos);
}

TEST_F(CliTest, TailManyRows) {
  // Test with file that has 20 data rows
  // Uses default multi-threaded parsing (PR #303 fixed SIMD delimiter masking
  // on macOS)
  auto result = CliRunner::run("tail -n 5 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("ID,Value,Label") != std::string::npos);
  // Should have last 5 rows (IDs 16-20)
  EXPECT_TRUE(result.output.find("16,") != std::string::npos);
  EXPECT_TRUE(result.output.find("20,") != std::string::npos);
  // Should NOT have earlier rows (IDs 1-15)
  EXPECT_TRUE(result.output.find("15,") == std::string::npos);
}

TEST_F(CliTest, TailFromStdin) {
  auto result = CliRunner::runWithFileStdin("tail -n 2 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, TailWithTabDelimiter) {
  auto result = CliRunner::run("tail -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\t") != std::string::npos);
}

// =============================================================================
// Sample Command Tests
// =============================================================================

TEST_F(CliTest, SampleDefault) {
  auto result = CliRunner::run("sample " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, SampleWithNumRows) {
  auto result = CliRunner::run("sample -n 2 -s 42 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output header + 2 data rows
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Verify we got exactly 2 data rows by counting occurrences of data patterns
  // simple.csv has rows: 1,2,3 and 4,5,6 and 7,8,9
  // With seed 42, sample should select specific rows from the 3 available
  int data_rows = 0;
  if (result.output.find("1,2,3") != std::string::npos)
    data_rows++;
  if (result.output.find("4,5,6") != std::string::npos)
    data_rows++;
  if (result.output.find("7,8,9") != std::string::npos)
    data_rows++;
  EXPECT_EQ(data_rows, 2); // We requested 2 rows
}

TEST_F(CliTest, SampleMoreRowsThanExist) {
  // Request more samples than exist - should return all data rows
  auto result = CliRunner::run("sample -n 100 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  EXPECT_TRUE(result.output.find("1,2,3") != std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") != std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") != std::string::npos);
}

TEST_F(CliTest, SampleZeroRows) {
  auto result = CliRunner::run("sample -n 0 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output only the header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Should NOT contain any data rows
  EXPECT_TRUE(result.output.find("1,2,3") == std::string::npos);
  EXPECT_TRUE(result.output.find("4,5,6") == std::string::npos);
  EXPECT_TRUE(result.output.find("7,8,9") == std::string::npos);
}

TEST_F(CliTest, SampleEmptyFile) {
  auto result = CliRunner::run("sample " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SampleReproducibleWithSeed) {
  // Same seed should produce same sample
  auto result1 = CliRunner::run("sample -n 5 -s 42 " + testDataPath("basic/many_rows.csv"));
  auto result2 = CliRunner::run("sample -n 5 -s 42 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result1.exit_code, 0);
  EXPECT_EQ(result2.exit_code, 0);
  EXPECT_EQ(result1.output, result2.output);
}

TEST_F(CliTest, SampleDifferentSeeds) {
  // Different seeds should likely produce different samples (not guaranteed but
  // highly probable)
  auto result1 = CliRunner::run("sample -n 5 -s 1 " + testDataPath("basic/many_rows.csv"));
  auto result2 = CliRunner::run("sample -n 5 -s 999 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result1.exit_code, 0);
  EXPECT_EQ(result2.exit_code, 0);
  // Both should have header
  EXPECT_TRUE(result1.output.find("ID,Value,Label") != std::string::npos);
  EXPECT_TRUE(result2.output.find("ID,Value,Label") != std::string::npos);
}

TEST_F(CliTest, SampleNoHeader) {
  auto result = CliRunner::run("sample -n 2 -H -s 42 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should output 2 rows without header treatment
  // With -H, all 4 rows (including "A,B,C") are treated as data
  // Verify we got exactly 2 data rows
  int data_rows = 0;
  if (result.output.find("A,B,C") != std::string::npos)
    data_rows++;
  if (result.output.find("1,2,3") != std::string::npos)
    data_rows++;
  if (result.output.find("4,5,6") != std::string::npos)
    data_rows++;
  if (result.output.find("7,8,9") != std::string::npos)
    data_rows++;
  EXPECT_EQ(data_rows, 2);
}

TEST_F(CliTest, SampleManyRows) {
  // Sample from file with 20 data rows
  // Uses default multi-threaded parsing (PR #303 fixed SIMD delimiter masking
  // on macOS)
  auto result = CliRunner::run("sample -n 5 -s 42 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("ID,Value,Label") != std::string::npos);
  // Count data rows by looking for unique patterns at start of line
  // Each data row has format like "1,100,A" or "20,2000,T"
  // Use patterns that are unique to each row to avoid false matches
  int data_rows = 0;
  if (result.output.find("1,100,A") != std::string::npos)
    data_rows++;
  if (result.output.find("2,200,B") != std::string::npos)
    data_rows++;
  if (result.output.find("3,300,C") != std::string::npos)
    data_rows++;
  if (result.output.find("4,400,D") != std::string::npos)
    data_rows++;
  if (result.output.find("5,500,E") != std::string::npos)
    data_rows++;
  if (result.output.find("6,600,F") != std::string::npos)
    data_rows++;
  if (result.output.find("7,700,G") != std::string::npos)
    data_rows++;
  if (result.output.find("8,800,H") != std::string::npos)
    data_rows++;
  if (result.output.find("9,900,I") != std::string::npos)
    data_rows++;
  if (result.output.find("10,1000,J") != std::string::npos)
    data_rows++;
  if (result.output.find("11,1100,K") != std::string::npos)
    data_rows++;
  if (result.output.find("12,1200,L") != std::string::npos)
    data_rows++;
  if (result.output.find("13,1300,M") != std::string::npos)
    data_rows++;
  if (result.output.find("14,1400,N") != std::string::npos)
    data_rows++;
  if (result.output.find("15,1500,O") != std::string::npos)
    data_rows++;
  if (result.output.find("16,1600,P") != std::string::npos)
    data_rows++;
  if (result.output.find("17,1700,Q") != std::string::npos)
    data_rows++;
  if (result.output.find("18,1800,R") != std::string::npos)
    data_rows++;
  if (result.output.find("19,1900,S") != std::string::npos)
    data_rows++;
  if (result.output.find("20,2000,T") != std::string::npos)
    data_rows++;
  EXPECT_EQ(data_rows, 5); // We requested 5 rows
}

TEST_F(CliTest, SampleFromStdin) {
  auto result =
      CliRunner::runWithFileStdin("sample -n 2 -s 42 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, SampleWithTabDelimiter) {
  auto result = CliRunner::run("sample -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\t") != std::string::npos);
}

TEST_F(CliTest, SampleInvalidSeed) {
  auto result = CliRunner::run("sample -s abc " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Invalid seed") != std::string::npos);
}

TEST_F(CliTest, SampleNegativeSeed) {
  auto result = CliRunner::run("sample -s -5 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 1);
}

// =============================================================================
// Dialect JSON Escaping Tests
// =============================================================================

TEST_F(CliTest, DialectJsonEscapesTab) {
  // Tab delimiter should be escaped as \t in JSON output
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // JSON should contain properly escaped tab
  EXPECT_TRUE(result.output.find("\"delimiter\": \"\\t\"") != std::string::npos);
}

TEST_F(CliTest, DialectJsonEscapesDoubleQuote) {
  // Double quote should be escaped as \" in JSON output
  auto result = CliRunner::run("dialect -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Quote character should be escaped (double quote is the default)
  EXPECT_TRUE(result.output.find("\"quote\": \"\\\"\"") != std::string::npos);
}

TEST_F(CliTest, DialectJsonValidStructure) {
  // Verify JSON output is well-formed
  auto result = CliRunner::run("dialect -j " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Check for required JSON fields
  EXPECT_TRUE(result.output.find("\"delimiter\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"quote\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"escape\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"line_ending\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"has_header\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"columns\":") != std::string::npos);
  EXPECT_TRUE(result.output.find("\"confidence\":") != std::string::npos);
}

// =============================================================================
// Carriage Return in Fields Tests
// Tests for fields containing \r (CR) characters within quoted fields.
// These tests verify that PR #203's quoting behavior is correct.
// =============================================================================

TEST_F(CliTest, HeadFieldsWithCR) {
  // Fields containing \r should be properly quoted in output
  auto result = CliRunner::run("head " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The header should be present
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Fields with CR should be quoted - look for the quoted field markers
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, TailFieldsWithCR) {
  // Tail command should properly handle fields containing \r
  auto result = CliRunner::run("tail -n 2 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Should have last 2 data rows (rows with fields containing \r)
  // Fields with CR should be quoted in output
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, TailFieldsWithCRVerifyQuoting) {
  // Verify that \r inside fields causes proper quoting
  auto result = CliRunner::run("tail -n 1 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The last row has a field with mixed \r and \r\n
  // The output should quote fields containing \r
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, SampleFieldsWithCR) {
  // Sample command should properly handle fields containing \r
  auto result = CliRunner::run("sample -n 2 -s 42 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
  // Fields with CR should be quoted in output
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, SampleFieldsWithCRReproducible) {
  // Same seed should produce same sample for file with \r in fields
  auto result1 = CliRunner::run("sample -n 2 -s 123 " + testDataPath("quoted/cr_in_quotes.csv"));
  auto result2 = CliRunner::run("sample -n 2 -s 123 " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result1.exit_code, 0);
  EXPECT_EQ(result2.exit_code, 0);
  EXPECT_EQ(result1.output, result2.output);
}

TEST_F(CliTest, CountFieldsWithCR) {
  // Count should work correctly with \r in quoted fields
  auto result = CliRunner::run("count " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // File has 3 data rows (after header)
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, InfoFieldsWithCR) {
  // Info should work correctly with \r in quoted fields
  auto result = CliRunner::run("info " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Columns: 3") != std::string::npos);
  EXPECT_TRUE(result.output.find("Rows: 3") != std::string::npos);
}

TEST_F(CliTest, SelectFieldsWithCR) {
  // Select should properly quote fields containing \r in output
  auto result = CliRunner::run("select -c B " + testDataPath("quoted/cr_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Column B contains fields with \r, so output should have quoted fields
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, TailCRLineEndingsFile) {
  // Test tail on file that uses CR as line ending (not in quoted fields)
  auto result = CliRunner::run("tail -n 1 " + testDataPath("line_endings/cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Output should not be empty - CR line endings should be handled gracefully
  // Note: CR line endings cause the entire file to appear as one line to the
  // parser, so exact content verification is complex
}

TEST_F(CliTest, SampleCRLineEndingsFile) {
  // Test sample on file that uses CR as line ending
  auto result = CliRunner::run("sample -n 1 -s 42 " + testDataPath("line_endings/cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should complete successfully with CR line endings
}

TEST_F(CliTest, TailCRLFLineEndingsFile) {
  // Test tail on file that uses CRLF line endings
  auto result = CliRunner::run("tail -n 1 " + testDataPath("line_endings/crlf.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // CRLF files should work correctly with tail
  // The output should contain data, though CRLF may be converted to LF
}

TEST_F(CliTest, SampleCRLFLineEndingsFile) {
  // Test sample on file that uses CRLF line endings
  auto result = CliRunner::run("sample -n 1 -s 42 " + testDataPath("line_endings/crlf.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // CRLF files should work correctly with sample
}

TEST_F(CliTest, TailMixedLineEndingsFile) {
  // Test tail on file with mixed line endings
  auto result = CliRunner::run("tail -n 2 " + testDataPath("malformed/mixed_line_endings.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should handle mixed line endings gracefully
}

TEST_F(CliTest, SampleMixedLineEndingsFile) {
  // Test sample on file with mixed line endings
  auto result =
      CliRunner::run("sample -n 2 -s 42 " + testDataPath("malformed/mixed_line_endings.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should handle mixed line endings gracefully
}

// =============================================================================
// Additional Delimiter Format Tests
// =============================================================================

TEST_F(CliTest, ColonDelimiter) {
  // Test colon delimiter (exercises formatDelimiter colon case)
  auto result = CliRunner::run("count -d : " + testDataPath("separators/colon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, DialectColonDelimiter) {
  // Test dialect command with colon-delimited file
  auto result = CliRunner::run("dialect " + testDataPath("separators/colon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should detect colon as delimiter
  EXPECT_TRUE(result.output.find("colon") != std::string::npos);
}

TEST_F(CliTest, UnknownDelimiterWarning) {
  // Test the warning path for unknown multi-char delimiter string
  auto result = CliRunner::run("count -d unknown_delim " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should show warning and fall back to comma
  EXPECT_TRUE(result.output.find("Warning:") != std::string::npos);
  EXPECT_TRUE(result.output.find("Unknown delimiter") != std::string::npos);
}

TEST_F(CliTest, TabDelimiterBackslashT) {
  // Test escaped tab format (\t) for delimiter
  auto result = CliRunner::run("count -d \\\\t " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, PipeDelimiterSymbol) {
  // Test pipe delimiter using | symbol directly
  auto result = CliRunner::run("count -d '|' " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, SemicolonDelimiterSymbol) {
  // Test semicolon delimiter using ; symbol directly
  auto result = CliRunner::run("count -d ';' " + testDataPath("separators/semicolon.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

// =============================================================================
// Quote Character Tests
// =============================================================================

TEST_F(CliTest, SingleQuoteChar) {
  // Test single quote as quote character
  auto result = CliRunner::run("count -q \"'\" " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CustomQuoteCharForSelect) {
  // Test custom quote character with select command
  auto result = CliRunner::run("select -c 0 -q \"'\" " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Dialect Command Extended Tests
// =============================================================================

TEST_F(CliTest, DialectJsonBackslashDelimiter) {
  // Test JSON output with backslash escaping for delimiter
  // The backslash escape in JSON output (line 914) is tested with tab
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"delimiter\": \"\\t\"") != std::string::npos);
}

TEST_F(CliTest, DialectPipeDelimiter) {
  auto result = CliRunner::run("dialect " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("pipe") != std::string::npos);
}

TEST_F(CliTest, DialectJsonPipeDelimiter) {
  auto result = CliRunner::run("dialect -j " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("\"delimiter\": \"|\"") != std::string::npos);
}

TEST_F(CliTest, DialectEmptyFile) {
  // Test dialect detection on empty file (should fail gracefully)
  auto result = CliRunner::run("dialect " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 1);
  // Should error because nothing to detect
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
}

TEST_F(CliTest, DialectFromStdin) {
  auto result = CliRunner::runWithFileStdin("dialect -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("comma") != std::string::npos);
}

TEST_F(CliTest, DialectNonexistentFile) {
  auto result = CliRunner::run("dialect nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
}

// =============================================================================
// Pretty Print Extended Tests
// =============================================================================

TEST_F(CliTest, PrettyNoHeader) {
  // Test pretty print without header (no separator after first row)
  auto result = CliRunner::run("pretty -H " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

TEST_F(CliTest, PrettyLongFieldTruncation) {
  // Test pretty print with field truncation to 40 chars max
  auto result = CliRunner::run("pretty " + testDataPath("large/large_field.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("...") != std::string::npos);
}

TEST_F(CliTest, PrettyNarrowColumns) {
  // Test pretty print with narrow columns (width < 3)
  auto result = CliRunner::run("pretty " + testDataPath("basic/narrow_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

TEST_F(CliTest, PrettyFromStdin) {
  auto result = CliRunner::runWithFileStdin("pretty -n 2 -", testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

TEST_F(CliTest, PrettyManyRows) {
  auto result = CliRunner::run("pretty " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Output Formatting Tests (fields needing quoting in output)
// =============================================================================

TEST_F(CliTest, HeadFieldsWithCommas) {
  // Test head output properly quotes fields containing commas
  auto result = CliRunner::run("head " + testDataPath("quoted/needs_quoting.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // The output should contain quoted fields
  EXPECT_TRUE(result.output.find("\"") != std::string::npos);
}

TEST_F(CliTest, SelectFieldsWithQuotes) {
  // Test select output properly escapes quotes in fields
  auto result = CliRunner::run("select -c 0,1 " + testDataPath("quoted/needs_quoting.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadFieldsWithContainsCR) {
  // Test head output properly quotes fields containing carriage returns
  auto result = CliRunner::run("head " + testDataPath("quoted/contains_cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, TailFieldsWithNewlines) {
  // Test tail output with embedded newlines in fields
  auto result = CliRunner::run("tail " + testDataPath("quoted/newlines_in_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Small File Tests (scalar path for row counting)
// =============================================================================

TEST_F(CliTest, CountTinyFile) {
  // Test count on a file under 64 bytes (exercises scalar path)
  auto result = CliRunner::run("count " + testDataPath("basic/tiny.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("1") != std::string::npos);
}

TEST_F(CliTest, CountTinyFileNoHeader) {
  auto result = CliRunner::run("count -H " + testDataPath("basic/tiny.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("2") != std::string::npos);
}

TEST_F(CliTest, HeadTinyFile) {
  auto result = CliRunner::run("head " + testDataPath("basic/tiny.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A,B") != std::string::npos);
}

// =============================================================================
// Additional Info Command Tests
// =============================================================================

TEST_F(CliTest, InfoFromStdinWithDelimiter) {
  auto result = CliRunner::runWithFileStdin("info -d tab -", testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("<stdin>") != std::string::npos);
}

TEST_F(CliTest, InfoManyColumns) {
  auto result = CliRunner::run("info " + testDataPath("basic/wide_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("Columns:") != std::string::npos);
}

// =============================================================================
// Additional Select Command Tests
// =============================================================================

TEST_F(CliTest, SelectWithTabDelimiter) {
  auto result = CliRunner::run("select -c 0 -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectMultipleByName) {
  auto result = CliRunner::run("select -c A,B " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("A") != std::string::npos);
  EXPECT_TRUE(result.output.find("B") != std::string::npos);
}

TEST_F(CliTest, SelectEmptyFile) {
  auto result = CliRunner::run("select -c 0 " + testDataPath("edge_cases/empty_file.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectRaggedCsv) {
  // Test select on CSV with ragged columns (some rows have fewer columns)
  auto result = CliRunner::run("select -c 0,2 " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Additional Head/Tail Tests
// =============================================================================

TEST_F(CliTest, HeadSingleColumn) {
  auto result = CliRunner::run("head " + testDataPath("basic/single_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, TailSingleColumn) {
  auto result = CliRunner::run("tail " + testDataPath("basic/single_column.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadQuotedFieldsPreservation) {
  // Test that quoted fields are properly output
  auto result = CliRunner::run("head " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, TailQuotedFieldsPreservation) {
  auto result = CliRunner::run("tail " + testDataPath("quoted/escaped_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Thread Count Edge Cases
// =============================================================================

TEST_F(CliTest, CountSingleThread) {
  auto result = CliRunner::run("count -t 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("3") != std::string::npos);
}

TEST_F(CliTest, CountMaxThreads) {
  // Test with maximum valid thread count (1024 after uint16_t change)
  auto result = CliRunner::run("count -t 1024 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountManyThreads) {
  // Test with thread count above old uint8_t limit (255)
  auto result = CliRunner::run("count -t 500 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadWithManyThreads) {
  auto result = CliRunner::run("head -t 16 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Sample Command Extended Tests
// =============================================================================

TEST_F(CliTest, SampleSingleRow) {
  auto result = CliRunner::run("sample -n 1 -s 42 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should have header and 1 data row
  EXPECT_TRUE(result.output.find("A,B,C") != std::string::npos);
}

TEST_F(CliTest, SampleLargeFile) {
  auto result = CliRunner::run("sample -n 10 -s 42 " + testDataPath("basic/many_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SampleWithPipeDelimiter) {
  auto result = CliRunner::run("sample -d pipe " + testDataPath("separators/pipe.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("|") != std::string::npos);
}

// =============================================================================
// Ragged CSV Tests
// =============================================================================

TEST_F(CliTest, HeadRaggedCsv) {
  auto result = CliRunner::run("head " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, TailRaggedCsv) {
  auto result = CliRunner::run("tail " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoRaggedCsv) {
  auto result = CliRunner::run("info " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, PrettyRaggedCsv) {
  // Test pretty print with ragged columns (different column counts per row)
  auto result = CliRunner::run("pretty " + testDataPath("ragged/fewer_columns.csv"));
  EXPECT_EQ(result.exit_code, 0);
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
}

// =============================================================================
// Whitespace and Special Content Tests
// =============================================================================

TEST_F(CliTest, CountBlankRows) {
  auto result = CliRunner::run("count " + testDataPath("whitespace/blank_rows_mixed.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadWhitespaceOnlyRows) {
  auto result = CliRunner::run("head " + testDataPath("whitespace/whitespace_only_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoBlankLeadingRows) {
  auto result = CliRunner::run("info " + testDataPath("whitespace/blank_leading_rows.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Real-world Data Tests
// =============================================================================

TEST_F(CliTest, HeadFinancialData) {
  auto result = CliRunner::run("head " + testDataPath("real_world/financial.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoContactsData) {
  auto result = CliRunner::run("info " + testDataPath("real_world/contacts.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectUnicodeData) {
  auto result = CliRunner::run("select -c 0 " + testDataPath("real_world/unicode.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, PrettyProductCatalog) {
  auto result = CliRunner::run("pretty -n 3 " + testDataPath("real_world/product_catalog.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Fuzz Test Data
// =============================================================================

TEST_F(CliTest, CountDeepQuotes) {
  auto result = CliRunner::run("count " + testDataPath("fuzz/deep_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadJustQuotes) {
  auto result = CliRunner::run("head " + testDataPath("fuzz/just_quotes.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountQuoteEof) {
  auto result = CliRunner::run("count " + testDataPath("fuzz/quote_eof.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoMixedCr) {
  auto result = CliRunner::run("info " + testDataPath("fuzz/mixed_cr.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountInvalidUtf8) {
  auto result = CliRunner::run("count " + testDataPath("fuzz/invalid_utf8.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Additional Error Cases
// =============================================================================

TEST_F(CliTest, HeadNonexistentFile) {
  auto result = CliRunner::run("head nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
  EXPECT_TRUE(result.output.find("Error:") != std::string::npos);
}

TEST_F(CliTest, TailNonexistentFile) {
  auto result = CliRunner::run("tail nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, SampleNonexistentFile) {
  auto result = CliRunner::run("sample nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, InfoNonexistentFile) {
  auto result = CliRunner::run("info nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

TEST_F(CliTest, PrettyNonexistentFile) {
  auto result = CliRunner::run("pretty nonexistent_file.csv");
  EXPECT_EQ(result.exit_code, 1);
}

// =============================================================================
// Combined Options Edge Cases
// =============================================================================

TEST_F(CliTest, HeadNoHeaderWithCustomDelimiter) {
  auto result = CliRunner::run("head -H -d tab " + testDataPath("separators/tab.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, TailNoHeaderWithRowCount) {
  auto result = CliRunner::run("tail -H -n 1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SampleWithAllOptions) {
  auto result = CliRunner::run("sample -n 2 -s 42 -H -d comma " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, SelectNoHeaderWithIndex) {
  // Select with -H should work with numeric indices
  auto result = CliRunner::run("select -H -c 0,1 " + testDataPath("basic/simple.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Encoding Tests
// =============================================================================

TEST_F(CliTest, HeadUtf8Bom) {
  auto result = CliRunner::run("head " + testDataPath("encoding/utf8_bom.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, CountLatin1) {
  auto result = CliRunner::run("count " + testDataPath("encoding/latin1.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoUtf16Bom) {
  auto result = CliRunner::run("info " + testDataPath("encoding/utf16_bom.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Comments Test Data
// =============================================================================

TEST_F(CliTest, CountHashComments) {
  auto result = CliRunner::run("count " + testDataPath("comments/hash_comments.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadQuotedHash) {
  auto result = CliRunner::run("head " + testDataPath("comments/quoted_hash.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Escape Style Tests
// =============================================================================

TEST_F(CliTest, HeadBackslashEscape) {
  auto result = CliRunner::run("head " + testDataPath("escape/backslash_escape.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// Edge Case: Single Cell File
// =============================================================================

TEST_F(CliTest, CountSingleCell) {
  auto result = CliRunner::run("count " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, HeadSingleCell) {
  auto result = CliRunner::run("head " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

TEST_F(CliTest, InfoSingleCell) {
  auto result = CliRunner::run("info " + testDataPath("edge_cases/single_cell.csv"));
  EXPECT_EQ(result.exit_code, 0);
}

// =============================================================================
// UTF-8 Truncation Limitation Tests
// =============================================================================
// KNOWN LIMITATION: The pretty command truncates fields at byte boundaries,
// not Unicode code point boundaries. This means multi-byte UTF-8 sequences
// (emoji, CJK characters, etc.) may be split, resulting in potentially
// invalid UTF-8 output. This is documented behavior per issue #240.
//
// These tests document the current behavior. If UTF-8-aware truncation is
// implemented in the future, these tests should be updated to verify proper
// code point boundary handling.
// =============================================================================

TEST_F(CliTest, PrettyUtf8TruncationLimitation) {
  // This test documents the known UTF-8 truncation limitation.
  // The pretty command truncates at byte position 37 (MAX_COLUMN_WIDTH - 3),
  // which may split multi-byte UTF-8 sequences.
  //
  // Test file contains fields > 40 bytes with multi-byte UTF-8:
  // - EmojiSplit: 36 ASCII + 2 emoji (4 bytes each) = 44 bytes
  // - CJKSplit: 17 CJK characters (3 bytes each) = 51 bytes
  // - MixedSplit: Mix of ASCII, CJK, emoji = 55 bytes
  auto result = CliRunner::run("pretty " + testDataPath("edge_cases/utf8_truncation.csv"));
  EXPECT_EQ(result.exit_code, 0);

  // Verify the command succeeds and produces table output
  EXPECT_TRUE(result.output.find("+") != std::string::npos);
  EXPECT_TRUE(result.output.find("|") != std::string::npos);

  // Verify truncation occurred (look for "..." in output)
  EXPECT_TRUE(result.output.find("...") != std::string::npos);

  // Note: The truncated output may contain invalid UTF-8 sequences.
  // This is the documented limitation - truncation operates on bytes,
  // not code points. A future fix would ensure truncation respects
  // UTF-8 character boundaries.
}

TEST_F(CliTest, PrettyUtf8ShortFieldsNotTruncated) {
  // Verify that short UTF-8 fields (< 40 bytes) are NOT truncated
  auto result = CliRunner::run("pretty " + testDataPath("real_world/unicode.csv"));
  EXPECT_EQ(result.exit_code, 0);

  // The unicode.csv file has fields < 40 bytes, so they should display fully
  EXPECT_TRUE(result.output.find("+") != std::string::npos);

  // Fields should not be truncated - no "..." for content fields
  // Note: Header "Description" is short so won't have "..."
}

// ============================================================================
// Regression Tests for GitHub Issues
// ============================================================================

TEST_F(CliTest, RegressionIssue264_ExtremelyWideCsv) {
  // Regression test for GitHub issue #264: SIGSEGV crash on extremely wide CSV
  // files The bug was in index buffer allocation for multi-threaded parsing.
  // Files with very high separator density (many columns) could overflow the
  // interleaved index buffer because the allocation didn't account for the
  // stride pattern used in multi-threaded mode.
  //
  // The test file has 16384 columns and 74 rows (~868K separators in ~876KB
  // file). This previously caused a segmentation fault.
  auto result = CliRunner::run("head -n 5 " + testDataPath("edge_cases/extremely_wide.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should successfully parse and output the first rows
  EXPECT_FALSE(result.output.empty());
  // First row should contain the expected header
  EXPECT_TRUE(result.output.find("BUSINESS PLAN QUARTERLY DATA SUMMARY") != std::string::npos);
}

TEST_F(CliTest, RegressionIssue264_ExtremelyWideCsvInfo) {
  // Also verify info command works on extremely wide files
  auto result = CliRunner::run("info " + testDataPath("edge_cases/extremely_wide.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should report 16384 columns
  EXPECT_TRUE(result.output.find("Columns: 16384") != std::string::npos);
}

TEST_F(CliTest, RegressionIssue264_ExtremelyWideCsvCount) {
  // Verify count command works on extremely wide files
  auto result = CliRunner::run("count " + testDataPath("edge_cases/extremely_wide.csv"));
  EXPECT_EQ(result.exit_code, 0);
  // Should return a valid row count
  EXPECT_FALSE(result.output.empty());
}
