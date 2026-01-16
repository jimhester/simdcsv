/**
 * @file buffer_lifetime_test.cpp
 * @brief Tests for buffer lifetime safety with shared_ptr ownership.
 *
 * These tests verify that ParseIndex and ValueExtractor can safely share
 * ownership of buffers and index data, preventing use-after-free bugs
 * when the original objects are moved or destroyed.
 */

#include "libvroom.h"

#include <cstring>
#include <gtest/gtest.h>
#include <memory>
#include <vector>

using namespace libvroom;

class BufferLifetimeTest : public ::testing::Test {
protected:
  // Helper to create a test buffer with proper padding
  static std::shared_ptr<std::vector<uint8_t>> make_buffer(const std::string& content) {
    auto buf = std::make_shared<std::vector<uint8_t>>(content.size() + 64);
    std::memcpy(buf->data(), content.data(), content.size());
    std::memset(buf->data() + content.size(), 0, 64);
    return buf;
  }
};

// Test: ParseIndex can store and retrieve a shared buffer
TEST_F(BufferLifetimeTest, ParseIndexCanStoreSharedBuffer) {
  auto buffer = make_buffer("a,b,c\n1,2,3\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);

  // Set the shared buffer
  result.idx.set_buffer(buffer);

  EXPECT_TRUE(result.idx.has_buffer());
  EXPECT_EQ(result.idx.buffer(), buffer);
  EXPECT_EQ(result.idx.buffer_data(), buffer->data());
  EXPECT_EQ(result.idx.buffer_size(), buffer->size());
}

// Test: ParseIndex::share() creates a valid shared copy
TEST_F(BufferLifetimeTest, ShareCreatesValidCopy) {
  auto buffer = make_buffer("a,b,c\n1,2,3\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);
  result.idx.set_buffer(buffer);

  auto shared = result.idx.share();

  EXPECT_NE(shared, nullptr);
  EXPECT_TRUE(shared->is_valid());
  EXPECT_TRUE(shared->has_buffer());
  EXPECT_EQ(shared->columns, result.idx.columns);
  EXPECT_EQ(shared->n_threads, result.idx.n_threads);
  EXPECT_EQ(shared->buffer(), buffer);
}

// Test: Shared index remains valid after original is moved
TEST_F(BufferLifetimeTest, SharedIndexValidAfterOriginalMoved) {
  auto buffer = make_buffer("a,b,c\n1,2,3\n");

  std::shared_ptr<const ParseIndex> shared;
  {
    Parser parser(1);
    auto result = parser.parse(buffer->data(), buffer->size() - 64);
    result.idx.set_buffer(buffer);
    shared = result.idx.share();

    // Move the original index away
    ParseIndex moved = std::move(result.idx);
  }
  // Original is now destroyed

  // Shared copy should still be valid
  EXPECT_TRUE(shared->is_valid());
  EXPECT_TRUE(shared->has_buffer());
  EXPECT_NE(shared->indexes, nullptr);
  EXPECT_NE(shared->n_indexes, nullptr);
}

// Test: Multiple shares of the same index work correctly
TEST_F(BufferLifetimeTest, MultipleSharesWork) {
  auto buffer = make_buffer("a,b,c\n1,2,3\n4,5,6\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);
  result.idx.set_buffer(buffer);

  auto shared1 = result.idx.share();
  auto shared2 = result.idx.share();
  auto shared3 = shared1; // Copy the shared_ptr

  EXPECT_TRUE(shared1->is_valid());
  EXPECT_TRUE(shared2->is_valid());
  EXPECT_TRUE(shared3->is_valid());

  // All point to the same buffer
  EXPECT_EQ(shared1->buffer_data(), shared2->buffer_data());
  EXPECT_EQ(shared2->buffer_data(), shared3->buffer_data());
}

// Test: ValueExtractor with shared ParseIndex
TEST_F(BufferLifetimeTest, ValueExtractorWithSharedIndex) {
  auto buffer = make_buffer("a,b,c\n1,2,3\n4,5,6\n");

  std::shared_ptr<const ParseIndex> shared;
  {
    Parser parser(1);
    auto result = parser.parse(buffer->data(), buffer->size() - 64);
    result.idx.set_buffer(buffer);
    shared = result.idx.share();
  }
  // Original ParseIndex is destroyed

  // Create ValueExtractor from shared index
  ValueExtractor extractor(shared, Dialect::csv(), ExtractionConfig::defaults());

  EXPECT_EQ(extractor.num_columns(), 3);
  EXPECT_EQ(extractor.num_rows(), 2);
  EXPECT_EQ(extractor.get_string(0, 0), "1");
  EXPECT_EQ(extractor.get_string(0, 1), "2");
  EXPECT_EQ(extractor.get_string(0, 2), "3");
  EXPECT_EQ(extractor.get_string(1, 0), "4");
}

// Test: ValueExtractor with shared index works after buffer's original shared_ptr is reset
TEST_F(BufferLifetimeTest, ValueExtractorMaintainsBufferLifetime) {
  std::shared_ptr<const ParseIndex> shared;
  {
    auto buffer = make_buffer("name,value\ntest,42\n");

    Parser parser(1);
    auto result = parser.parse(buffer->data(), buffer->size() - 64);
    result.idx.set_buffer(buffer);
    shared = result.idx.share();

    // Reset the local buffer reference
    buffer.reset();
  }
  // Buffer is only kept alive by the shared ParseIndex

  ValueExtractor extractor(shared, Dialect::csv(), ExtractionConfig::defaults());

  EXPECT_EQ(extractor.num_columns(), 2);
  EXPECT_EQ(extractor.get_string(0, 0), "test");
  EXPECT_EQ(extractor.get_string(0, 1), "42");
}

// Test: is_shared() returns correct value
TEST_F(BufferLifetimeTest, IsSharedReturnsCorrectValue) {
  auto buffer = make_buffer("a,b\n1,2\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);

  // Before share(), is_shared() should return false
  EXPECT_FALSE(result.idx.is_shared());

  result.idx.set_buffer(buffer);
  auto shared = result.idx.share();

  // After share(), the original should now use shared ownership
  EXPECT_TRUE(result.idx.is_shared());

  // The shared copy should also be in shared mode
  EXPECT_TRUE(shared->is_shared());
}

// Test: ValueExtractor throws when shared_idx is null
TEST_F(BufferLifetimeTest, ValueExtractorThrowsOnNullSharedIndex) {
  std::shared_ptr<const ParseIndex> null_shared;

  EXPECT_THROW(ValueExtractor(null_shared, Dialect::csv(), ExtractionConfig::defaults()),
               std::invalid_argument);
}

// Test: ValueExtractor throws when ParseIndex has no buffer
TEST_F(BufferLifetimeTest, ValueExtractorThrowsOnMissingBuffer) {
  auto buffer = make_buffer("a,b\n1,2\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);

  // Don't set buffer, just share
  auto shared = result.idx.share();

  EXPECT_THROW(ValueExtractor(shared, Dialect::csv(), ExtractionConfig::defaults()),
               std::invalid_argument);
}

// Test: Buffer data pointer matches after share
TEST_F(BufferLifetimeTest, BufferDataPointerMatchesAfterShare) {
  auto buffer = make_buffer("col1,col2\nval1,val2\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);
  result.idx.set_buffer(buffer);

  const uint8_t* original_data = result.idx.buffer_data();
  auto shared = result.idx.share();

  EXPECT_EQ(shared->buffer_data(), original_data);
  EXPECT_EQ(shared->buffer_data(), buffer->data());
}

// Test: Shared index preserves column count
TEST_F(BufferLifetimeTest, SharedIndexPreservesMetadata) {
  auto buffer = make_buffer("a,b,c,d,e\n1,2,3,4,5\n");

  Parser parser(1);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);
  result.idx.set_buffer(buffer);

  uint64_t original_columns = result.idx.columns;
  uint16_t original_n_threads = result.idx.n_threads;
  uint64_t original_region_size = result.idx.region_size;

  auto shared = result.idx.share();

  EXPECT_EQ(shared->columns, original_columns);
  EXPECT_EQ(shared->n_threads, original_n_threads);
  EXPECT_EQ(shared->region_size, original_region_size);
}

// Test: share() correctly handles compact() called after first share()
// This tests an edge case where:
// 1. First share() converts unique_ptr to shared_ptr
// 2. compact() is called, creating new flat_indexes_ptr_
// 3. Second share() must correctly convert flat_indexes_ptr_ to shared
TEST_F(BufferLifetimeTest, ShareAfterCompactPreservesFlatIndex) {
  auto buffer = make_buffer("name,value,extra\ntest,42,x\nalpha,99,y\n");

  Parser parser(2);
  auto result = parser.parse(buffer->data(), buffer->size() - 64);
  result.idx.set_buffer(buffer);

  // First share() - converts to shared ownership
  auto shared1 = result.idx.share();
  EXPECT_TRUE(result.idx.is_shared());
  EXPECT_FALSE(result.idx.is_flat()); // Not yet compacted

  // Compact after sharing
  result.idx.compact();
  EXPECT_TRUE(result.idx.is_flat());

  // Second share() - must correctly handle flat_indexes_ptr_
  auto shared2 = result.idx.share();
  EXPECT_TRUE(shared2->is_flat());
  EXPECT_EQ(shared2->flat_indexes_count, result.idx.flat_indexes_count);

  // Verify field access still works on the shared copy
  // after original is destroyed
  {
    // Move original away to destroy it
    ParseIndex moved = std::move(result.idx);
  }

  // shared2 should still have valid flat index
  // (would crash with use-after-free if bug exists)
  FieldSpan span = shared2->get_field_span(0, 0);
  EXPECT_TRUE(span.is_valid());
}
