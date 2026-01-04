/**
 * @file debug_test.cpp
 * @brief Tests for the debug mode functionality.
 */

#include <gtest/gtest.h>
#include <cstdio>
#include <cstring>
#include "debug.h"
#include "debug_parser.h"

using namespace simdcsv;

class DebugTest : public ::testing::Test {
protected:
    void SetUp() override {
        output_file_ = tmpfile();
    }

    void TearDown() override {
        if (output_file_) {
            fclose(output_file_);
        }
    }

    std::string get_output() {
        if (!output_file_) return "";
        fflush(output_file_);
        rewind(output_file_);
        std::string result;
        char buffer[1024];
        while (fgets(buffer, sizeof(buffer), output_file_)) {
            result += buffer;
        }
        return result;
    }

    FILE* output_file_ = nullptr;
};

TEST_F(DebugTest, DebugConfigDefaults) {
    DebugConfig config;
    EXPECT_FALSE(config.verbose);
    EXPECT_FALSE(config.dump_masks);
    EXPECT_FALSE(config.timing);
    EXPECT_FALSE(config.enabled());
}

TEST_F(DebugTest, DebugConfigAll) {
    DebugConfig config = DebugConfig::all();
    EXPECT_TRUE(config.verbose);
    EXPECT_TRUE(config.dump_masks);
    EXPECT_TRUE(config.timing);
    EXPECT_TRUE(config.enabled());
}

TEST_F(DebugTest, DebugTraceLog) {
    DebugConfig config;
    config.verbose = true;
    config.output = output_file_;
    DebugTrace trace(config);

    trace.log("Test message %d", 42);

    std::string output = get_output();
    EXPECT_NE(output.find("[simdcsv] Test message 42"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceLogDisabled) {
    DebugConfig config;
    config.verbose = false;
    config.output = output_file_;
    DebugTrace trace(config);

    trace.log("This should not appear");

    std::string output = get_output();
    EXPECT_TRUE(output.empty());
}

TEST_F(DebugTest, DebugTraceDumpMask) {
    DebugConfig config;
    config.dump_masks = true;
    config.output = output_file_;
    DebugTrace trace(config);

    trace.dump_mask("test_mask", 0xFF, 0);

    std::string output = get_output();
    EXPECT_NE(output.find("MASK test_mask"), std::string::npos);
    EXPECT_NE(output.find("hex:"), std::string::npos);
}

TEST_F(DebugTest, DebugTraceTiming) {
    DebugConfig config;
    config.timing = true;
    DebugTrace trace(config);

    trace.start_phase("test_phase");
    trace.end_phase(1000);

    const auto& times = trace.get_phase_times();
    EXPECT_EQ(times.size(), 1u);
    EXPECT_EQ(times[0].name, "test_phase");
    EXPECT_EQ(times[0].bytes_processed, 1000u);
}

TEST_F(DebugTest, SimdPathName) {
    const char* path = get_simd_path_name();
    EXPECT_NE(path, nullptr);
    EXPECT_GT(strlen(path), 0u);
}

TEST_F(DebugTest, SimdVectorBytes) {
    size_t bytes = get_simd_vector_bytes();
    EXPECT_GE(bytes, 16u);
    EXPECT_LE(bytes, 64u);
}

TEST_F(DebugTest, DebugParserParse) {
    DebugConfig config;
    config.verbose = true;
    config.timing = true;
    config.output = output_file_;
    DebugTrace trace(config);

    debug_parser parser;
    const char* csv = "a,b,c\n1,2,3\n";
    const uint8_t* buf = reinterpret_cast<const uint8_t*>(csv);
    size_t len = strlen(csv);

    simdcsv::index idx = parser.init(len, 1);
    bool result = parser.parse_debug(buf, idx, len, trace);

    EXPECT_TRUE(result);

    std::string output = get_output();
    EXPECT_NE(output.find("[simdcsv]"), std::string::npos);
    EXPECT_NE(output.find("Starting parse"), std::string::npos);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
