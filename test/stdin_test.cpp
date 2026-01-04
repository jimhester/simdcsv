/**
 * @file stdin_test.cpp
 * @brief Automated tests for stdin functionality (get_corpus_stdin and CLI stdin support)
 *
 * This file provides test coverage for:
 * - get_corpus_stdin() function from io_util.h
 * - CLI stdin integration (via subprocess testing)
 *
 * Related to: GitHub Issue #77
 */

#include <gtest/gtest.h>
#include <io_util.h>
#include <mem_util.h>
#include <cstdio>
#include <cstring>
#include <string>
#include <unistd.h>  // for dup, dup2, pipe, close, write
#include <fcntl.h>   // for open flags

class StdinTest : public ::testing::Test {
protected:
    int original_stdin_fd_ = -1;

    void SetUp() override {
        // Save original stdin so we can restore it
        original_stdin_fd_ = dup(STDIN_FILENO);
        ASSERT_NE(original_stdin_fd_, -1) << "Failed to save original stdin";
    }

    void TearDown() override {
        // Restore original stdin file descriptor
        if (original_stdin_fd_ != -1) {
            dup2(original_stdin_fd_, STDIN_FILENO);
            close(original_stdin_fd_);
            original_stdin_fd_ = -1;
        }
        // Clear any stdio buffering/error state
        clearerr(stdin);
    }

    // Helper to redirect a string to stdin
    // Returns true on success
    bool redirectStringToStdin(const std::string& data) {
        int pipefd[2];
        if (pipe(pipefd) == -1) {
            return false;
        }

        // Write data to pipe
        ssize_t written = write(pipefd[1], data.c_str(), data.size());
        close(pipefd[1]);  // Close write end (signals EOF)

        if (written != static_cast<ssize_t>(data.size())) {
            close(pipefd[0]);
            return false;
        }

        // Redirect pipe read end to stdin
        if (dup2(pipefd[0], STDIN_FILENO) == -1) {
            close(pipefd[0]);
            return false;
        }
        close(pipefd[0]);

        // Clear stdio buffer state after changing underlying fd
        clearerr(stdin);

        return true;
    }
};

// Test basic stdin reading with simple CSV data
TEST_F(StdinTest, BasicCSVReading) {
    std::string csv_data = "a,b,c\n1,2,3\n4,5,6\n";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), csv_data.size());
    EXPECT_EQ(std::memcmp(corpus.data(), csv_data.data(), csv_data.size()), 0);

    aligned_free((void*)corpus.data());
}

// Test stdin reading with quoted fields
TEST_F(StdinTest, QuotedFieldReading) {
    std::string csv_data = "name,description\n\"John\",\"A \"\"quoted\"\" value\"\n";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), csv_data.size());
    EXPECT_EQ(std::memcmp(corpus.data(), csv_data.data(), csv_data.size()), 0);

    aligned_free((void*)corpus.data());
}

// Test stdin reading with moderately large data
// Note: We keep this under the pipe buffer size (~64KB on macOS/Linux)
// to avoid blocking in the single-threaded test setup
TEST_F(StdinTest, ModerateDataReading) {
    std::string row = "field1,field2,field3,field4,field5\n";
    std::string csv_data;
    csv_data.reserve(50000);  // ~50KB, well under pipe buffer

    // Add header
    csv_data = "col1,col2,col3,col4,col5\n";
    // Add rows to get ~40KB of data
    while (csv_data.size() < 40000) {
        csv_data += row;
    }

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(64);
    EXPECT_EQ(corpus.size(), csv_data.size());
    EXPECT_EQ(std::memcmp(corpus.data(), csv_data.data(), csv_data.size()), 0);

    aligned_free((void*)corpus.data());
}

// Test stdin reading with LF line endings (Unix)
TEST_F(StdinTest, LineEndingLF) {
    std::string csv_data = "a,b\n1,2\n";
    ASSERT_TRUE(redirectStringToStdin(csv_data));
    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), csv_data.size());
    aligned_free((void*)corpus.data());
}

// Test stdin reading with CRLF line endings (Windows)
TEST_F(StdinTest, LineEndingCRLF) {
    std::string csv_data = "a,b\r\n1,2\r\n";
    ASSERT_TRUE(redirectStringToStdin(csv_data));
    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), csv_data.size());
    aligned_free((void*)corpus.data());
}

// Test stdin reading with binary data (embedded special characters)
TEST_F(StdinTest, BinaryDataReading) {
    // CSV with embedded special characters (not null, but other binary-ish data)
    std::string csv_data = "a,b\n\x01\x02,\x03\x04\n";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), csv_data.size());
    EXPECT_EQ(std::memcmp(corpus.data(), csv_data.data(), csv_data.size()), 0);

    aligned_free((void*)corpus.data());
}

// Test that empty stdin throws an exception
TEST_F(StdinTest, EmptyStdinThrows) {
    std::string empty_data = "";

    ASSERT_TRUE(redirectStringToStdin(empty_data));

    EXPECT_THROW({
        auto corpus = get_corpus_stdin(32);
        aligned_free((void*)corpus.data());
    }, std::runtime_error);
}

// Test padding is applied correctly (buffer is larger than data)
TEST_F(StdinTest, PaddingApplied) {
    std::string csv_data = "x,y\n1,2\n";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    size_t padding = 64;
    auto corpus = get_corpus_stdin(padding);

    // Size should be exactly the data size (padding not included in size)
    EXPECT_EQ(corpus.size(), csv_data.size());

    // The data should be correct
    EXPECT_EQ(std::memcmp(corpus.data(), csv_data.data(), csv_data.size()), 0);

    // We can't easily test the actual allocation size, but we verify
    // the returned size is correct and data matches

    aligned_free((void*)corpus.data());
}

// Test stdin reading with Unicode/UTF-8 data
TEST_F(StdinTest, UnicodeDataReading) {
    std::string csv_data = "name,city\nJosé,São Paulo\n田中,東京\n";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), csv_data.size());
    EXPECT_EQ(std::memcmp(corpus.data(), csv_data.data(), csv_data.size()), 0);

    aligned_free((void*)corpus.data());
}

// Test stdin reading with single byte
TEST_F(StdinTest, SingleByteReading) {
    std::string csv_data = "x";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(32);
    EXPECT_EQ(corpus.size(), 1);
    EXPECT_EQ(corpus[0], 'x');

    aligned_free((void*)corpus.data());
}

// Test that the buffer is properly aligned (64-byte cache line alignment)
TEST_F(StdinTest, BufferAlignment) {
    std::string csv_data = "a,b,c\n1,2,3\n";

    ASSERT_TRUE(redirectStringToStdin(csv_data));

    auto corpus = get_corpus_stdin(32);

    // Check 64-byte alignment
    uintptr_t addr = reinterpret_cast<uintptr_t>(corpus.data());
    EXPECT_EQ(addr % 64, 0) << "Buffer should be 64-byte aligned";

    aligned_free((void*)corpus.data());
}
