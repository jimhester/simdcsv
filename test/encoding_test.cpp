/**
 * @file encoding_test.cpp
 * @brief Tests for encoding detection and transcoding functionality.
 */

#include <gtest/gtest.h>
#include "encoding.h"
#include "io_util.h"
#include "mem_util.h"
#include <cstring>
#include <fstream>
#include <string>
#include <vector>

using namespace simdcsv;

// ============================================================================
// BOM Detection Tests
// ============================================================================

class BomDetectionTest : public ::testing::Test {
protected:
    static std::string test_data_dir() {
        return "test/data/encoding/";
    }
};

TEST_F(BomDetectionTest, DetectsUtf16LeBom) {
    // UTF-16 LE BOM: FF FE
    uint8_t data[] = {0xFF, 0xFE, 'a', 0x00, 'b', 0x00};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
    EXPECT_EQ(result.bom_length, 2);
    EXPECT_TRUE(result.needs_transcoding);
    EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf16BeBom) {
    // UTF-16 BE BOM: FE FF
    uint8_t data[] = {0xFE, 0xFF, 0x00, 'a', 0x00, 'b'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF16_BE);
    EXPECT_EQ(result.bom_length, 2);
    EXPECT_TRUE(result.needs_transcoding);
    EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf32LeBom) {
    // UTF-32 LE BOM: FF FE 00 00
    uint8_t data[] = {0xFF, 0xFE, 0x00, 0x00, 'a', 0x00, 0x00, 0x00};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF32_LE);
    EXPECT_EQ(result.bom_length, 4);
    EXPECT_TRUE(result.needs_transcoding);
    EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf32BeBom) {
    // UTF-32 BE BOM: 00 00 FE FF
    uint8_t data[] = {0x00, 0x00, 0xFE, 0xFF, 0x00, 0x00, 0x00, 'a'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF32_BE);
    EXPECT_EQ(result.bom_length, 4);
    EXPECT_TRUE(result.needs_transcoding);
    EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, DetectsUtf8Bom) {
    // UTF-8 BOM: EF BB BF
    uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'e', 'l', 'l', 'o'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF8_BOM);
    EXPECT_EQ(result.bom_length, 3);
    EXPECT_FALSE(result.needs_transcoding);
    EXPECT_DOUBLE_EQ(result.confidence, 1.0);
}

TEST_F(BomDetectionTest, NoBomDefaultsToUtf8) {
    // Plain ASCII - no BOM
    uint8_t data[] = {'h', 'e', 'l', 'l', 'o', '\n'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF8);
    EXPECT_EQ(result.bom_length, 0);
    EXPECT_FALSE(result.needs_transcoding);
}

TEST_F(BomDetectionTest, PartialUtf8BomOneByte) {
    // Only first byte of UTF-8 BOM (EF BB BF) - should not detect as UTF-8 BOM
    uint8_t data[] = {0xEF, 'h', 'e', 'l', 'l', 'o'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_NE(result.encoding, Encoding::UTF8_BOM);
    EXPECT_EQ(result.bom_length, 0);
}

TEST_F(BomDetectionTest, PartialUtf8BomTwoBytes) {
    // First two bytes of UTF-8 BOM (EF BB BF) - should not detect as UTF-8 BOM
    uint8_t data[] = {0xEF, 0xBB, 'h', 'e', 'l', 'l', 'o'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_NE(result.encoding, Encoding::UTF8_BOM);
    EXPECT_EQ(result.bom_length, 0);
}

TEST_F(BomDetectionTest, PartialUtf16BomOneByte) {
    // Only first byte of UTF-16 LE BOM (FF FE) - should not detect as UTF-16
    uint8_t data[] = {0xFF, 'h', 'e', 'l', 'l', 'o'};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_NE(result.encoding, Encoding::UTF16_LE);
    EXPECT_EQ(result.bom_length, 0);
}

TEST_F(BomDetectionTest, PartialUtf32BomTwoBytes) {
    // First two bytes of UTF-32 LE BOM (FF FE 00 00) - matches UTF-16 LE BOM
    // This is expected behavior: FF FE is a valid UTF-16 LE BOM
    uint8_t data[] = {0xFF, 0xFE, 'a', 0x00, 'b', 0x00};
    auto result = detect_encoding(data, sizeof(data));
    // Should detect as UTF-16 LE, not UTF-32 LE (which requires 4 bytes)
    EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
    EXPECT_EQ(result.bom_length, 2);
}

TEST_F(BomDetectionTest, PartialUtf32BomThreeBytes) {
    // First three bytes of UTF-32 LE BOM (FF FE 00 00) - still UTF-16 LE
    uint8_t data[] = {0xFF, 0xFE, 0x00, 'a', 0x00, 'b'};
    auto result = detect_encoding(data, sizeof(data));
    // Should detect as UTF-16 LE since FF FE 00 00 pattern not complete
    EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
    EXPECT_EQ(result.bom_length, 2);
}

TEST_F(BomDetectionTest, TinyBufferOneByte) {
    // Buffer too small to contain any BOM
    uint8_t data[] = {0xEF};
    auto result = detect_encoding(data, 1);
    EXPECT_EQ(result.bom_length, 0);
}

// ============================================================================
// Heuristic Detection Tests
// ============================================================================

class HeuristicDetectionTest : public ::testing::Test {};

TEST_F(HeuristicDetectionTest, DetectsUtf16LeWithoutBom) {
    // UTF-16 LE: ASCII characters with null byte after each
    std::vector<uint8_t> data;
    const char* text = "hello";
    for (const char* p = text; *p; ++p) {
        data.push_back(static_cast<uint8_t>(*p));
        data.push_back(0x00);
    }

    auto result = detect_encoding(data.data(), data.size());
    EXPECT_EQ(result.encoding, Encoding::UTF16_LE);
    EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsUtf16BeWithoutBom) {
    // UTF-16 BE: null byte before each ASCII character
    std::vector<uint8_t> data;
    const char* text = "hello";
    for (const char* p = text; *p; ++p) {
        data.push_back(0x00);
        data.push_back(static_cast<uint8_t>(*p));
    }

    auto result = detect_encoding(data.data(), data.size());
    EXPECT_EQ(result.encoding, Encoding::UTF16_BE);
    EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsUtf32LeWithoutBom) {
    // UTF-32 LE: ASCII character followed by three null bytes
    std::vector<uint8_t> data;
    const char* text = "hello world test more text";  // Need more data for detection
    for (const char* p = text; *p; ++p) {
        data.push_back(static_cast<uint8_t>(*p));
        data.push_back(0x00);
        data.push_back(0x00);
        data.push_back(0x00);
    }

    auto result = detect_encoding(data.data(), data.size());
    EXPECT_EQ(result.encoding, Encoding::UTF32_LE);
    EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsUtf32BeWithoutBom) {
    // UTF-32 BE: three null bytes followed by ASCII character
    std::vector<uint8_t> data;
    const char* text = "hello world test more text";  // Need more data for detection
    for (const char* p = text; *p; ++p) {
        data.push_back(0x00);
        data.push_back(0x00);
        data.push_back(0x00);
        data.push_back(static_cast<uint8_t>(*p));
    }

    auto result = detect_encoding(data.data(), data.size());
    EXPECT_EQ(result.encoding, Encoding::UTF32_BE);
    EXPECT_TRUE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, DetectsValidUtf8) {
    // Valid UTF-8 with multibyte characters
    // "café" in UTF-8: 63 61 66 c3 a9
    uint8_t data[] = {0x63, 0x61, 0x66, 0xc3, 0xa9};
    auto result = detect_encoding(data, sizeof(data));
    EXPECT_EQ(result.encoding, Encoding::UTF8);
    EXPECT_FALSE(result.needs_transcoding);
}

TEST_F(HeuristicDetectionTest, EmptyDataIsUtf8) {
    auto result = detect_encoding(nullptr, 0);
    EXPECT_EQ(result.encoding, Encoding::UTF8);
}

// ============================================================================
// Transcoding Tests
// ============================================================================

class TranscodingTest : public ::testing::Test {};

TEST_F(TranscodingTest, TranscodesUtf16LeToUtf8) {
    // "AB" in UTF-16 LE: 41 00 42 00
    uint8_t data[] = {0x41, 0x00, 0x42, 0x00};
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.length, 2);
    EXPECT_EQ(result.data[0], 'A');
    EXPECT_EQ(result.data[1], 'B');

    aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf16BeToUtf8) {
    // "AB" in UTF-16 BE: 00 41 00 42
    uint8_t data[] = {0x00, 0x41, 0x00, 0x42};
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_BE, 0, 32);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.length, 2);
    EXPECT_EQ(result.data[0], 'A');
    EXPECT_EQ(result.data[1], 'B');

    aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf16LeWithAccents) {
    // "é" (U+00E9) in UTF-16 LE: E9 00
    // In UTF-8 this becomes: C3 A9
    uint8_t data[] = {0xE9, 0x00};
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.length, 2);
    EXPECT_EQ(result.data[0], 0xC3);
    EXPECT_EQ(result.data[1], 0xA9);

    aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf32LeToUtf8) {
    // "AB" in UTF-32 LE
    uint8_t data[] = {0x41, 0x00, 0x00, 0x00, 0x42, 0x00, 0x00, 0x00};
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF32_LE, 0, 32);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.length, 2);
    EXPECT_EQ(result.data[0], 'A');
    EXPECT_EQ(result.data[1], 'B');

    aligned_free(result.data);
}

TEST_F(TranscodingTest, TranscodesUtf32BeToUtf8) {
    // "AB" in UTF-32 BE
    uint8_t data[] = {0x00, 0x00, 0x00, 0x41, 0x00, 0x00, 0x00, 0x42};
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF32_BE, 0, 32);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.length, 2);
    EXPECT_EQ(result.data[0], 'A');
    EXPECT_EQ(result.data[1], 'B');

    aligned_free(result.data);
}

TEST_F(TranscodingTest, HandlesUtf16Surrogate) {
    // Emoji "😀" (U+1F600) in UTF-16 LE: D8 3D DE 00 (surrogate pair)
    // High surrogate: D83D, Low surrogate: DE00
    uint8_t data[] = {0x3D, 0xD8, 0x00, 0xDE};  // Little-endian byte order
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

    ASSERT_TRUE(result.success);
    // U+1F600 in UTF-8: F0 9F 98 80
    EXPECT_EQ(result.length, 4);
    EXPECT_EQ(result.data[0], 0xF0);
    EXPECT_EQ(result.data[1], 0x9F);
    EXPECT_EQ(result.data[2], 0x98);
    EXPECT_EQ(result.data[3], 0x80);

    aligned_free(result.data);
}

TEST_F(TranscodingTest, StripsUtf8Bom) {
    // UTF-8 BOM followed by "hi"
    uint8_t data[] = {0xEF, 0xBB, 0xBF, 'h', 'i'};
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF8_BOM, 3, 32);

    ASSERT_TRUE(result.success);
    EXPECT_EQ(result.length, 2);
    EXPECT_EQ(result.data[0], 'h');
    EXPECT_EQ(result.data[1], 'i');

    aligned_free(result.data);
}

TEST_F(TranscodingTest, RejectsOddLengthUtf16) {
    uint8_t data[] = {0x41, 0x00, 0x42};  // 3 bytes - invalid for UTF-16
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF16_LE, 0, 32);

    EXPECT_FALSE(result.success);
    EXPECT_FALSE(result.error.empty());
}

TEST_F(TranscodingTest, RejectsNonDivisibleUtf32) {
    uint8_t data[] = {0x41, 0x00, 0x00, 0x00, 0x42};  // 5 bytes - invalid for UTF-32
    auto result = transcode_to_utf8(data, sizeof(data), Encoding::UTF32_LE, 0, 32);

    EXPECT_FALSE(result.success);
    EXPECT_FALSE(result.error.empty());
}

// ============================================================================
// File Loading Tests
// ============================================================================

class FileLoadingTest : public ::testing::Test {
protected:
    static std::string test_data_dir() {
        return "test/data/encoding/";
    }
};

TEST_F(FileLoadingTest, LoadsUtf16LeFile) {
    try {
        auto result = get_corpus_with_encoding(test_data_dir() + "utf16_le_bom.csv", 64);
        EXPECT_EQ(result.encoding.encoding, Encoding::UTF16_LE);
        EXPECT_TRUE(result.encoding.needs_transcoding);

        // The data should now be UTF-8
        std::string content(reinterpret_cast<const char*>(result.data.data()),
                           result.data.size());
        EXPECT_TRUE(content.find("name") != std::string::npos);

        aligned_free((void*)result.data.data());
    } catch (const std::exception& e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(FileLoadingTest, LoadsUtf16BeFile) {
    try {
        auto result = get_corpus_with_encoding(test_data_dir() + "utf16_be_bom.csv", 64);
        EXPECT_EQ(result.encoding.encoding, Encoding::UTF16_BE);
        EXPECT_TRUE(result.encoding.needs_transcoding);

        std::string content(reinterpret_cast<const char*>(result.data.data()),
                           result.data.size());
        EXPECT_TRUE(content.find("name") != std::string::npos);

        aligned_free((void*)result.data.data());
    } catch (const std::exception& e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(FileLoadingTest, LoadsUtf32LeFile) {
    try {
        auto result = get_corpus_with_encoding(test_data_dir() + "utf32_le_bom.csv", 64);
        EXPECT_EQ(result.encoding.encoding, Encoding::UTF32_LE);
        EXPECT_TRUE(result.encoding.needs_transcoding);

        std::string content(reinterpret_cast<const char*>(result.data.data()),
                           result.data.size());
        EXPECT_TRUE(content.find("name") != std::string::npos);

        aligned_free((void*)result.data.data());
    } catch (const std::exception& e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(FileLoadingTest, LoadsUtf32BeFile) {
    try {
        auto result = get_corpus_with_encoding(test_data_dir() + "utf32_be_bom.csv", 64);
        EXPECT_EQ(result.encoding.encoding, Encoding::UTF32_BE);
        EXPECT_TRUE(result.encoding.needs_transcoding);

        std::string content(reinterpret_cast<const char*>(result.data.data()),
                           result.data.size());
        EXPECT_TRUE(content.find("name") != std::string::npos);

        aligned_free((void*)result.data.data());
    } catch (const std::exception& e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(FileLoadingTest, LoadsUtf8BomFile) {
    try {
        auto result = get_corpus_with_encoding(test_data_dir() + "utf8_bom.csv", 64);
        EXPECT_EQ(result.encoding.encoding, Encoding::UTF8_BOM);
        EXPECT_EQ(result.encoding.bom_length, 3);

        // BOM should be stripped
        std::string content(reinterpret_cast<const char*>(result.data.data()),
                           result.data.size());
        EXPECT_FALSE(content.empty());
        // Should not start with BOM
        EXPECT_NE(result.data.data()[0], 0xEF);

        aligned_free((void*)result.data.data());
    } catch (const std::exception& e) {
        FAIL() << "Exception: " << e.what();
    }
}

TEST_F(FileLoadingTest, LoadsPlainUtf8File) {
    try {
        auto result = get_corpus_with_encoding(test_data_dir() + "latin1.csv", 64);
        // Latin-1 and UTF-8 are similar for ASCII subset
        EXPECT_FALSE(result.encoding.needs_transcoding);

        std::string content(reinterpret_cast<const char*>(result.data.data()),
                           result.data.size());
        EXPECT_FALSE(content.empty());

        aligned_free((void*)result.data.data());
    } catch (const std::exception& e) {
        FAIL() << "Exception: " << e.what();
    }
}

// ============================================================================
// encoding_to_string Tests
// ============================================================================

TEST(EncodingToStringTest, ReturnsCorrectStrings) {
    EXPECT_STREQ(encoding_to_string(Encoding::UTF8), "UTF-8");
    EXPECT_STREQ(encoding_to_string(Encoding::UTF8_BOM), "UTF-8 (BOM)");
    EXPECT_STREQ(encoding_to_string(Encoding::UTF16_LE), "UTF-16LE");
    EXPECT_STREQ(encoding_to_string(Encoding::UTF16_BE), "UTF-16BE");
    EXPECT_STREQ(encoding_to_string(Encoding::UTF32_LE), "UTF-32LE");
    EXPECT_STREQ(encoding_to_string(Encoding::UTF32_BE), "UTF-32BE");
    EXPECT_STREQ(encoding_to_string(Encoding::LATIN1), "Latin-1");
    EXPECT_STREQ(encoding_to_string(Encoding::UNKNOWN), "Unknown");
}
