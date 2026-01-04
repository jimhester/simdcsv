#ifdef SIMDCSV_ENABLE_ARROW
#include <gtest/gtest.h>
#include <arrow/api.h>
#include "arrow_output.h"
#include "io_util.h"
#include "mem_util.h"
#include "two_pass.h"
#include <cstring>

namespace simdcsv {

struct TestBuffer {
    uint8_t* data;
    size_t len;
    explicit TestBuffer(const std::string& content) {
        len = content.size();
        data = allocate_padded_buffer(len, 64);
        std::memcpy(data, content.data(), len);
    }
    ~TestBuffer() { if (data) aligned_free(data); }
    TestBuffer(const TestBuffer&) = delete;
    TestBuffer& operator=(const TestBuffer&) = delete;
};

class ArrowOutputTest : public ::testing::Test {
protected:
    ArrowConvertResult parseAndConvert(const std::string& csv, const ArrowConvertOptions& opts = ArrowConvertOptions()) {
        TestBuffer buf(csv);
        two_pass parser;
        index idx = parser.init(buf.len, 1);
        parser.parse(buf.data, idx, buf.len);
        ArrowConverter converter(opts);
        return converter.convert(buf.data, buf.len, idx);
    }
};

TEST_F(ArrowOutputTest, BasicConversion) {
    auto result = parseAndConvert("name,age\nAlice,30\nBob,25\n");
    ASSERT_TRUE(result.ok()) << result.error_message;
    EXPECT_EQ(result.num_columns, 2);
    EXPECT_EQ(result.num_rows, 2);
}

TEST_F(ArrowOutputTest, TypeInferenceInteger) {
    ArrowConvertOptions opts; opts.infer_types = true;
    auto result = parseAndConvert("id,count\n1,100\n2,200\n", opts);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::INT64);
}

TEST_F(ArrowOutputTest, TypeInferenceDouble) {
    ArrowConvertOptions opts; opts.infer_types = true;
    auto result = parseAndConvert("value\n1.5\n2.7\n", opts);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::DOUBLE);
}

TEST_F(ArrowOutputTest, TypeInferenceBoolean) {
    ArrowConvertOptions opts; opts.infer_types = true;
    auto result = parseAndConvert("flag\ntrue\nfalse\n", opts);
    ASSERT_TRUE(result.ok());
    EXPECT_EQ(result.schema->field(0)->type()->id(), arrow::Type::BOOL);
}

TEST_F(ArrowOutputTest, ColumnTypeHelpers) {
    EXPECT_EQ(column_type_to_arrow(ColumnType::STRING)->id(), arrow::Type::STRING);
    EXPECT_EQ(column_type_to_arrow(ColumnType::INT64)->id(), arrow::Type::INT64);
    EXPECT_STREQ(column_type_to_string(ColumnType::STRING), "STRING");
}

}  // namespace simdcsv

#else
#include <gtest/gtest.h>
TEST(ArrowOutputTest, ArrowNotEnabled) { GTEST_SKIP() << "Arrow not enabled"; }
#endif
