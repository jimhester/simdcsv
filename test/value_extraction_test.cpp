#include <gtest/gtest.h>
#include <cstring>
#include <cmath>
#include <limits>
#include "value_extraction.h"
#include "two_pass.h"
#include "mem_util.h"

using namespace simdcsv;

class TestBuffer {
public:
    explicit TestBuffer(const std::string& content) : content_(content) {
        buffer_ = new uint8_t[content.size() + 64];
        std::memcpy(buffer_, content.data(), content.size());
        std::memset(buffer_ + content.size(), 0, 64);
    }
    ~TestBuffer() { delete[] buffer_; }
    const uint8_t* data() const { return buffer_; }
    size_t size() const { return content_.size(); }
private:
    std::string content_;
    uint8_t* buffer_;
};

class IntegerParsingTest : public ::testing::Test {
protected:
    ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(IntegerParsingTest, ParseZero) {
    EXPECT_EQ(parse_integer<int64_t>("0", 1, config_).get(), 0);
}

TEST_F(IntegerParsingTest, ParsePositive) {
    EXPECT_EQ(parse_integer<int64_t>("12345", 5, config_).get(), 12345);
}

TEST_F(IntegerParsingTest, ParseNegative) {
    EXPECT_EQ(parse_integer<int64_t>("-12345", 6, config_).get(), -12345);
}

TEST_F(IntegerParsingTest, EmptyIsNA) {
    EXPECT_TRUE(parse_integer<int64_t>("", 0, config_).is_na());
}

class DoubleParsingTest : public ::testing::Test {
protected:
    ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(DoubleParsingTest, ParseDecimal) {
    EXPECT_NEAR(parse_double("3.14", 4, config_).get(), 3.14, 0.01);
}

TEST_F(DoubleParsingTest, ParseScientific) {
    EXPECT_NEAR(parse_double("1e10", 4, config_).get(), 1e10, 1e5);
}

TEST_F(DoubleParsingTest, ParseNaN) {
    EXPECT_TRUE(std::isnan(parse_double("NaN", 3, config_).get()));
}

class BoolParsingTest : public ::testing::Test {
protected:
    ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(BoolParsingTest, ParseTrue) { EXPECT_TRUE(parse_bool("true", 4, config_).get()); }
TEST_F(BoolParsingTest, ParseFalse) { EXPECT_FALSE(parse_bool("false", 5, config_).get()); }

class NATest : public ::testing::Test {
protected:
    ExtractionConfig config_ = ExtractionConfig::defaults();
};

TEST_F(NATest, EmptyIsNA) { EXPECT_TRUE(is_na("", 0, config_)); }
TEST_F(NATest, NAIsNA) { EXPECT_TRUE(is_na("NA", 2, config_)); }
TEST_F(NATest, ValueNotNA) { EXPECT_FALSE(is_na("hello", 5, config_)); }

class ValueExtractorTest : public ::testing::Test {
protected:
    std::unique_ptr<TestBuffer> buffer_;
    simdcsv::two_pass parser_;
    simdcsv::index idx_;

    void ParseCSV(const std::string& csv) {
        buffer_ = std::make_unique<TestBuffer>(csv);
        idx_ = parser_.init(buffer_->size(), 1);
        parser_.parse(buffer_->data(), idx_, buffer_->size());
    }
};

TEST_F(ValueExtractorTest, SimpleCSV) {
    ParseCSV("name,age\nAlice,30\nBob,25\n");
    ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);
    EXPECT_EQ(extractor.num_columns(), 2);
    EXPECT_EQ(extractor.num_rows(), 2);
    EXPECT_EQ(extractor.get_string_view(0, 0), "Alice");
    EXPECT_EQ(extractor.get<int64_t>(0, 1).get(), 30);
}

TEST_F(ValueExtractorTest, NoHeader) {
    ParseCSV("Alice,30\nBob,25\n");
    ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);
    extractor.set_has_header(false);
    EXPECT_EQ(extractor.num_rows(), 2);
    EXPECT_EQ(extractor.get_string_view(0, 0), "Alice");
    EXPECT_EQ(extractor.get_string_view(1, 0), "Bob");
}

TEST_F(ValueExtractorTest, ColumnExtraction) {
    ParseCSV("id\n1\n2\n3\n");
    ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);
    auto ids = extractor.extract_column<int64_t>(0);
    EXPECT_EQ(ids.size(), 3);
    EXPECT_EQ(*ids[0], 1);
    EXPECT_EQ(*ids[1], 2);
    EXPECT_EQ(*ids[2], 3);
}

TEST_F(ValueExtractorTest, EmptyField) {
    ParseCSV("a,b\n1,\n");
    ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);
    EXPECT_TRUE(extractor.get<int64_t>(0, 1).is_na());
}

TEST_F(ValueExtractorTest, RowIterator) {
    ParseCSV("id\n1\n2\n");
    ValueExtractor extractor(buffer_->data(), buffer_->size(), idx_);
    int count = 0;
    for (const auto& row : extractor) {
        EXPECT_EQ(row.get<int64_t>(0).get(), count + 1);
        count++;
    }
    EXPECT_EQ(count, 2);
}

int main(int argc, char** argv) {
    ::testing::InitGoogleTest(&argc, argv);
    return RUN_ALL_TESTS();
}
