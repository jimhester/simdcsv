#include <gtest/gtest.h>
#include <simdcsv.h>
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
    simdcsv::FileBuffer empty;
    EXPECT_FALSE(empty.valid());
    EXPECT_TRUE(empty.empty());

    auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
    simdcsv::FileBuffer buffer(data, len);
    EXPECT_TRUE(buffer.valid());
    EXPECT_FALSE(buffer.empty());
}

TEST_F(SimplifiedAPITest, FileBufferMove) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n");
    simdcsv::FileBuffer buffer1(data, len);
    simdcsv::FileBuffer buffer2(std::move(buffer1));
    EXPECT_FALSE(buffer1.valid());
    EXPECT_TRUE(buffer2.valid());
}

TEST_F(SimplifiedAPITest, FileBufferRelease) {
    auto [data, len] = make_buffer("a,b,c\n");
    simdcsv::FileBuffer buffer(data, len);
    uint8_t* released = buffer.release();
    EXPECT_FALSE(buffer.valid());
    aligned_free(released);
}

TEST_F(SimplifiedAPITest, ParserBasicParsing) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    simdcsv::FileBuffer buffer(data, len);
    simdcsv::Parser parser;
    auto result = parser.parse(buffer.data(), buffer.size());
    EXPECT_TRUE(result.success());
    EXPECT_GT(result.total_indexes(), 0);
}

TEST_F(SimplifiedAPITest, ParserWithErrors) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5\n");
    simdcsv::FileBuffer buffer(data, len);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::Parser parser;
    auto result = parser.parse_with_errors(buffer.data(), buffer.size(), errors);
    EXPECT_TRUE(result.success());
    EXPECT_TRUE(errors.has_errors());
}

TEST_F(SimplifiedAPITest, ParserDialects) {
    {
        auto [data, len] = make_buffer("a\tb\tc\n1\t2\t3\n");
        simdcsv::FileBuffer buffer(data, len);
        simdcsv::Parser parser;
        auto result = parser.parse(buffer.data(), buffer.size(), simdcsv::Dialect::tsv());
        EXPECT_TRUE(result.success());
    }
    {
        auto [data, len] = make_buffer("a;b;c\n1;2;3\n");
        simdcsv::FileBuffer buffer(data, len);
        simdcsv::Parser parser;
        auto result = parser.parse(buffer.data(), buffer.size(), simdcsv::Dialect::semicolon());
        EXPECT_TRUE(result.success());
    }
}

TEST_F(SimplifiedAPITest, DetectDialect) {
    auto [data, len] = make_buffer("a,b,c\n1,2,3\n4,5,6\n");
    simdcsv::FileBuffer buffer(data, len);  // RAII wrapper handles cleanup
    auto detection = simdcsv::detect_dialect(buffer.data(), buffer.size());
    EXPECT_TRUE(detection.success());
    EXPECT_EQ(detection.dialect.delimiter, ',');
}

TEST_F(SimplifiedAPITest, ParserAutoDetection) {
    auto [data, len] = make_buffer("name;age;city\nJohn;25;NYC\nJane;30;LA\n");
    simdcsv::FileBuffer buffer(data, len);
    simdcsv::ErrorCollector errors(simdcsv::ErrorMode::PERMISSIVE);
    simdcsv::Parser parser;
    auto result = parser.parse_auto(buffer.data(), buffer.size(), errors);
    EXPECT_TRUE(result.success());
    EXPECT_EQ(result.dialect.delimiter, ';');
}

TEST_F(SimplifiedAPITest, ParserThreadCount) {
    simdcsv::Parser parser1(1);
    EXPECT_EQ(parser1.num_threads(), 1);
    simdcsv::Parser parser4(4);
    EXPECT_EQ(parser4.num_threads(), 4);
    parser4.set_num_threads(0);
    EXPECT_EQ(parser4.num_threads(), 1);
}

TEST_F(SimplifiedAPITest, CustomDialect) {
    auto [data, len] = make_buffer("a:b:c\n'hello':'world':'!'\n");
    simdcsv::FileBuffer buffer(data, len);
    simdcsv::Dialect custom;
    custom.delimiter = ':';
    custom.quote_char = '\'';
    simdcsv::Parser parser;
    auto result = parser.parse(buffer.data(), buffer.size(), custom);
    EXPECT_TRUE(result.success());
}
