/**
 * @file column_config_test.cpp
 * @brief Tests for per-column configuration feature.
 *
 * Tests the ColumnConfig, ColumnConfigMap, and related functionality
 * for specifying different extraction settings per column.
 */

#include "libvroom.h"

#include "extraction_config.h"
#include "mem_util.h"
#include "value_extraction.h"

#include <cstring>
#include <gtest/gtest.h>

using namespace libvroom;

// Helper class to create test buffers
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

// =============================================================================
// ColumnConfig Tests
// =============================================================================

class ColumnConfigTest : public ::testing::Test {};

TEST_F(ColumnConfigTest, DefaultConfig) {
  ColumnConfig config;
  EXPECT_FALSE(config.type_hint.has_value());
  EXPECT_FALSE(config.na_values.has_value());
  EXPECT_FALSE(config.true_values.has_value());
  EXPECT_FALSE(config.false_values.has_value());
  EXPECT_FALSE(config.trim_whitespace.has_value());
  EXPECT_FALSE(config.has_overrides());
}

TEST_F(ColumnConfigTest, FactoryAsString) {
  ColumnConfig config = ColumnConfig::as_string();
  EXPECT_TRUE(config.type_hint.has_value());
  EXPECT_EQ(*config.type_hint, TypeHint::STRING);
  EXPECT_TRUE(config.has_overrides());
}

TEST_F(ColumnConfigTest, FactoryAsInteger) {
  ColumnConfig config = ColumnConfig::as_integer();
  EXPECT_TRUE(config.type_hint.has_value());
  EXPECT_EQ(*config.type_hint, TypeHint::INTEGER);
}

TEST_F(ColumnConfigTest, FactoryAsDouble) {
  ColumnConfig config = ColumnConfig::as_double();
  EXPECT_TRUE(config.type_hint.has_value());
  EXPECT_EQ(*config.type_hint, TypeHint::DOUBLE);
}

TEST_F(ColumnConfigTest, FactoryAsBoolean) {
  ColumnConfig config = ColumnConfig::as_boolean();
  EXPECT_TRUE(config.type_hint.has_value());
  EXPECT_EQ(*config.type_hint, TypeHint::BOOLEAN);
}

TEST_F(ColumnConfigTest, FactorySkip) {
  ColumnConfig config = ColumnConfig::skip();
  EXPECT_TRUE(config.type_hint.has_value());
  EXPECT_EQ(*config.type_hint, TypeHint::SKIP);
}

TEST_F(ColumnConfigTest, MergeWithGlobalConfig) {
  ExtractionConfig global;
  global.trim_whitespace = false;
  global.allow_leading_zeros = false;

  ColumnConfig column;
  column.trim_whitespace = true; // Override global setting

  ExtractionConfig merged = column.merge_with(global);
  EXPECT_TRUE(merged.trim_whitespace);      // Overridden
  EXPECT_FALSE(merged.allow_leading_zeros); // From global
}

TEST_F(ColumnConfigTest, MergeWithCustomNaValues) {
  ExtractionConfig global;

  ColumnConfig column;
  column.na_values = std::vector<std::string_view>{"", "N/A", "-"};

  ExtractionConfig merged = column.merge_with(global);
  ASSERT_EQ(merged.na_values.size(), 3);
  EXPECT_EQ(merged.na_values[0], "");
  EXPECT_EQ(merged.na_values[1], "N/A");
  EXPECT_EQ(merged.na_values[2], "-");
}

TEST_F(ColumnConfigTest, TypeHintToString) {
  EXPECT_STREQ(type_hint_to_string(TypeHint::AUTO), "auto");
  EXPECT_STREQ(type_hint_to_string(TypeHint::BOOLEAN), "boolean");
  EXPECT_STREQ(type_hint_to_string(TypeHint::INTEGER), "integer");
  EXPECT_STREQ(type_hint_to_string(TypeHint::DOUBLE), "double");
  EXPECT_STREQ(type_hint_to_string(TypeHint::STRING), "string");
  EXPECT_STREQ(type_hint_to_string(TypeHint::DATE), "date");
  EXPECT_STREQ(type_hint_to_string(TypeHint::DATETIME), "datetime");
  EXPECT_STREQ(type_hint_to_string(TypeHint::SKIP), "skip");
}

// =============================================================================
// ColumnConfigMap Tests
// =============================================================================

class ColumnConfigMapTest : public ::testing::Test {};

TEST_F(ColumnConfigMapTest, EmptyByDefault) {
  ColumnConfigMap configs;
  EXPECT_TRUE(configs.empty());
  EXPECT_EQ(configs.get(0), nullptr);
  EXPECT_EQ(configs.get("nonexistent"), nullptr);
}

TEST_F(ColumnConfigMapTest, SetByIndex) {
  ColumnConfigMap configs;
  configs.set(0, ColumnConfig::as_integer());
  configs.set(2, ColumnConfig::as_double());

  EXPECT_FALSE(configs.empty());

  const ColumnConfig* config0 = configs.get(0);
  ASSERT_NE(config0, nullptr);
  EXPECT_EQ(*config0->type_hint, TypeHint::INTEGER);

  EXPECT_EQ(configs.get(1), nullptr); // Not set

  const ColumnConfig* config2 = configs.get(2);
  ASSERT_NE(config2, nullptr);
  EXPECT_EQ(*config2->type_hint, TypeHint::DOUBLE);
}

TEST_F(ColumnConfigMapTest, SetByName) {
  ColumnConfigMap configs;
  configs.set("id", ColumnConfig::as_integer());
  configs.set("price", ColumnConfig::as_double());

  EXPECT_FALSE(configs.empty());

  const ColumnConfig* configId = configs.get("id");
  ASSERT_NE(configId, nullptr);
  EXPECT_EQ(*configId->type_hint, TypeHint::INTEGER);

  const ColumnConfig* configPrice = configs.get("price");
  ASSERT_NE(configPrice, nullptr);
  EXPECT_EQ(*configPrice->type_hint, TypeHint::DOUBLE);

  EXPECT_EQ(configs.get("nonexistent"), nullptr);
}

TEST_F(ColumnConfigMapTest, Clear) {
  ColumnConfigMap configs;
  configs.set(0, ColumnConfig::as_integer());
  configs.set("name", ColumnConfig::as_string());

  EXPECT_FALSE(configs.empty());

  configs.clear();

  EXPECT_TRUE(configs.empty());
  EXPECT_EQ(configs.get(0), nullptr);
  EXPECT_EQ(configs.get("name"), nullptr);
}

TEST_F(ColumnConfigMapTest, ResolveNames) {
  ColumnConfigMap configs;
  configs.set("id", ColumnConfig::as_integer());
  configs.set("name", ColumnConfig::as_string());

  std::unordered_map<std::string, size_t> name_to_index;
  name_to_index["id"] = 0;
  name_to_index["name"] = 1;
  name_to_index["value"] = 2;

  configs.resolve_names(name_to_index);

  // After resolving, should be able to get by index
  const ColumnConfig* config0 = configs.get(0);
  ASSERT_NE(config0, nullptr);
  EXPECT_EQ(*config0->type_hint, TypeHint::INTEGER);

  const ColumnConfig* config1 = configs.get(1);
  ASSERT_NE(config1, nullptr);
  EXPECT_EQ(*config1->type_hint, TypeHint::STRING);

  EXPECT_EQ(configs.get(2), nullptr); // "value" not configured
}

// =============================================================================
// ValueExtractor with ColumnConfig Tests
// =============================================================================

class ValueExtractorColumnConfigTest : public ::testing::Test {
protected:
  void SetUp() override {
    // CSV: name,age,is_active
    //      Alice,30,true
    //      Bob,NA,false
    csv_content_ = "name,age,is_active\nAlice,30,true\nBob,NA,false\n";
    buffer_ = std::make_unique<TestBuffer>(csv_content_);
    parser_ = std::make_unique<Parser>();
    result_ = parser_->parse(buffer_->data(), buffer_->size());
  }

  std::string csv_content_;
  std::unique_ptr<TestBuffer> buffer_;
  std::unique_ptr<Parser> parser_;
  Parser::Result result_;
};

TEST_F(ValueExtractorColumnConfigTest, GetTypeHintWithNoConfig) {
  // Without any column config, all columns should have AUTO type hint
  EXPECT_EQ(result_.get_type_hint(0), TypeHint::AUTO);
  EXPECT_EQ(result_.get_type_hint(1), TypeHint::AUTO);
  EXPECT_EQ(result_.get_type_hint(2), TypeHint::AUTO);
}

TEST_F(ValueExtractorColumnConfigTest, SetColumnConfigByIndex) {
  result_.set_column_config(0, ColumnConfig::as_string());
  result_.set_column_config(1, ColumnConfig::as_integer());
  result_.set_column_config(2, ColumnConfig::as_boolean());

  EXPECT_EQ(result_.get_type_hint(0), TypeHint::STRING);
  EXPECT_EQ(result_.get_type_hint(1), TypeHint::INTEGER);
  EXPECT_EQ(result_.get_type_hint(2), TypeHint::BOOLEAN);
}

TEST_F(ValueExtractorColumnConfigTest, SetColumnConfigByName) {
  result_.set_column_config("name", ColumnConfig::as_string());
  result_.set_column_config("age", ColumnConfig::as_integer());
  result_.set_column_config("is_active", ColumnConfig::as_boolean());

  // Note: Column configs by name are resolved lazily
  EXPECT_EQ(result_.get_type_hint(0), TypeHint::STRING);
  EXPECT_EQ(result_.get_type_hint(1), TypeHint::INTEGER);
  EXPECT_EQ(result_.get_type_hint(2), TypeHint::BOOLEAN);
}

TEST_F(ValueExtractorColumnConfigTest, ShouldSkipColumn) {
  EXPECT_FALSE(result_.should_skip_column(0));
  EXPECT_FALSE(result_.should_skip_column(1));

  result_.set_column_config(1, ColumnConfig::skip());

  EXPECT_FALSE(result_.should_skip_column(0));
  EXPECT_TRUE(result_.should_skip_column(1));
}

TEST_F(ValueExtractorColumnConfigTest, CustomNaValuesForColumn) {
  // Create a config that treats "-" as NA for the age column
  ColumnConfig age_config;
  age_config.type_hint = TypeHint::INTEGER;
  age_config.na_values = std::vector<std::string_view>{"", "NA", "N/A", "-"};

  result_.set_column_config(1, age_config);

  // The extractor should now use the custom NA values for column 1
  const auto& configs = result_.column_configs();
  const ColumnConfig* col1_config = configs.get(1);
  ASSERT_NE(col1_config, nullptr);
  ASSERT_TRUE(col1_config->na_values.has_value());
  EXPECT_EQ(col1_config->na_values->size(), 4);
}

// =============================================================================
// ParseOptions with ColumnConfig Tests
// =============================================================================

class ParseOptionsColumnConfigTest : public ::testing::Test {};

TEST_F(ParseOptionsColumnConfigTest, DefaultOptionsHaveEmptyColumnConfigs) {
  ParseOptions opts = ParseOptions::defaults();
  EXPECT_TRUE(opts.column_configs.empty());
}

TEST_F(ParseOptionsColumnConfigTest, FactoryWithColumnConfigs) {
  ColumnConfigMap configs;
  configs.set(0, ColumnConfig::as_string());
  configs.set(1, ColumnConfig::as_integer());

  ParseOptions opts = ParseOptions::with_column_configs(configs);

  EXPECT_FALSE(opts.column_configs.empty());
  EXPECT_NE(opts.column_configs.get(0), nullptr);
  EXPECT_NE(opts.column_configs.get(1), nullptr);
}

TEST_F(ParseOptionsColumnConfigTest, ParseWithColumnConfigsPassedThrough) {
  std::string csv = "id,value\n1,100\n2,200\n";
  TestBuffer buffer(csv);

  ColumnConfigMap configs;
  configs.set("id", ColumnConfig::as_integer());
  configs.set("value", ColumnConfig::as_double());

  ParseOptions opts;
  opts.column_configs = configs;

  Parser parser;
  auto result = parser.parse(buffer.data(), buffer.size(), opts);

  // Verify configs were passed to result
  EXPECT_FALSE(result.column_configs().empty());
  EXPECT_EQ(result.get_type_hint(0), TypeHint::INTEGER);
  EXPECT_EQ(result.get_type_hint(1), TypeHint::DOUBLE);
}

// =============================================================================
// Per-Column Extraction Config Tests
// =============================================================================

class PerColumnExtractionTest : public ::testing::Test {
protected:
  void SetUp() override {
    // CSV with values that can be interpreted differently per column
    csv_content_ = "col_a,col_b,col_c\n1,NA,yes\n2,-,no\n3,NULL,1\n";
    buffer_ = std::make_unique<TestBuffer>(csv_content_);
    parser_ = std::make_unique<Parser>();
  }

  std::string csv_content_;
  std::unique_ptr<TestBuffer> buffer_;
  std::unique_ptr<Parser> parser_;
};

TEST_F(PerColumnExtractionTest, CustomNaValuesPerColumn) {
  // Column B treats "-" as NA, Column C doesn't
  ColumnConfigMap configs;

  ColumnConfig col_b_config;
  col_b_config.na_values = std::vector<std::string_view>{"NA", "-", "NULL"};
  configs.set(1, col_b_config);

  ParseOptions opts;
  opts.column_configs = configs;

  auto result = parser_->parse(buffer_->data(), buffer_->size(), opts);

  // The config should be available on the result
  const ColumnConfig* col_b = result.column_configs().get(1);
  ASSERT_NE(col_b, nullptr);
  EXPECT_TRUE(col_b->na_values.has_value());
}

// =============================================================================
// C API Column Config Tests
// =============================================================================

#include "libvroom_c.h"

class CApiColumnConfigTest : public ::testing::Test {};

TEST_F(CApiColumnConfigTest, CreateAndDestroy) {
  libvroom_column_config_t* config = libvroom_column_config_create();
  ASSERT_NE(config, nullptr);
  EXPECT_TRUE(libvroom_column_config_empty(config));
  libvroom_column_config_destroy(config);
}

TEST_F(CApiColumnConfigTest, SetTypeByIndex) {
  libvroom_column_config_t* config = libvroom_column_config_create();
  ASSERT_NE(config, nullptr);

  libvroom_error_t err = libvroom_column_config_set_type_by_index(config, 0, LIBVROOM_TYPE_INTEGER);
  EXPECT_EQ(err, LIBVROOM_OK);

  EXPECT_FALSE(libvroom_column_config_empty(config));
  EXPECT_EQ(libvroom_column_config_get_type_by_index(config, 0), LIBVROOM_TYPE_INTEGER);
  EXPECT_EQ(libvroom_column_config_get_type_by_index(config, 1), LIBVROOM_TYPE_AUTO);

  libvroom_column_config_destroy(config);
}

TEST_F(CApiColumnConfigTest, SetTypeByName) {
  libvroom_column_config_t* config = libvroom_column_config_create();
  ASSERT_NE(config, nullptr);

  libvroom_error_t err =
      libvroom_column_config_set_type_by_name(config, "price", LIBVROOM_TYPE_DOUBLE);
  EXPECT_EQ(err, LIBVROOM_OK);

  EXPECT_FALSE(libvroom_column_config_empty(config));

  libvroom_column_config_destroy(config);
}

TEST_F(CApiColumnConfigTest, Clear) {
  libvroom_column_config_t* config = libvroom_column_config_create();

  libvroom_column_config_set_type_by_index(config, 0, LIBVROOM_TYPE_INTEGER);
  EXPECT_FALSE(libvroom_column_config_empty(config));

  libvroom_column_config_clear(config);
  EXPECT_TRUE(libvroom_column_config_empty(config));

  libvroom_column_config_destroy(config);
}

TEST_F(CApiColumnConfigTest, TypeHintString) {
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_AUTO), "auto");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_BOOLEAN), "boolean");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_INTEGER), "integer");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_DOUBLE), "double");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_STRING), "string");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_DATE), "date");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_DATETIME), "datetime");
  EXPECT_STREQ(libvroom_type_hint_string(LIBVROOM_TYPE_SKIP), "skip");
}

TEST_F(CApiColumnConfigTest, NullPointerHandling) {
  EXPECT_EQ(libvroom_column_config_set_type_by_index(nullptr, 0, LIBVROOM_TYPE_INTEGER),
            LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_column_config_set_type_by_name(nullptr, "col", LIBVROOM_TYPE_INTEGER),
            LIBVROOM_ERROR_NULL_POINTER);

  libvroom_column_config_t* config = libvroom_column_config_create();
  EXPECT_EQ(libvroom_column_config_set_type_by_name(config, nullptr, LIBVROOM_TYPE_INTEGER),
            LIBVROOM_ERROR_NULL_POINTER);
  libvroom_column_config_destroy(config);

  // These should not crash when passed nullptr
  EXPECT_TRUE(libvroom_column_config_empty(nullptr));
  EXPECT_EQ(libvroom_column_config_get_type_by_index(nullptr, 0), LIBVROOM_TYPE_AUTO);
  libvroom_column_config_clear(nullptr);
  libvroom_column_config_destroy(nullptr);
}

TEST_F(CApiColumnConfigTest, ParserSetAndGetColumnConfig) {
  // Create parser
  libvroom_parser_t* parser = libvroom_parser_create();
  ASSERT_NE(parser, nullptr);

  // Initially should have no column config
  EXPECT_EQ(libvroom_parser_get_column_config(parser), nullptr);

  // Create column config
  libvroom_column_config_t* config = libvroom_column_config_create();
  libvroom_column_config_set_type_by_index(config, 0, LIBVROOM_TYPE_INTEGER);
  libvroom_column_config_set_type_by_index(config, 1, LIBVROOM_TYPE_DOUBLE);

  // Set on parser
  libvroom_error_t err = libvroom_parser_set_column_config(parser, config);
  EXPECT_EQ(err, LIBVROOM_OK);

  // Should now have column config
  const libvroom_column_config_t* parser_config = libvroom_parser_get_column_config(parser);
  ASSERT_NE(parser_config, nullptr);
  EXPECT_EQ(libvroom_column_config_get_type_by_index(parser_config, 0), LIBVROOM_TYPE_INTEGER);
  EXPECT_EQ(libvroom_column_config_get_type_by_index(parser_config, 1), LIBVROOM_TYPE_DOUBLE);

  // Clear config
  err = libvroom_parser_clear_column_config(parser);
  EXPECT_EQ(err, LIBVROOM_OK);
  EXPECT_EQ(libvroom_parser_get_column_config(parser), nullptr);

  // Cleanup
  libvroom_column_config_destroy(config);
  libvroom_parser_destroy(parser);
}

TEST_F(CApiColumnConfigTest, ParserColumnConfigNullPointerHandling) {
  EXPECT_EQ(libvroom_parser_set_column_config(nullptr, nullptr), LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_parser_clear_column_config(nullptr), LIBVROOM_ERROR_NULL_POINTER);
  EXPECT_EQ(libvroom_parser_get_column_config(nullptr), nullptr);
}
