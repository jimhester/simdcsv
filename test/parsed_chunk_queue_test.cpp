/**
 * @file parsed_chunk_queue_test.cpp
 * @brief Tests for ParsedChunkQueue — ordered bounded queue (Issue #645).
 *
 * Verifies that ParsedChunkQueue delivers parsed chunks in sequential order
 * even when producers push out of order, with proper backpressure and
 * close semantics.
 */

#include "libvroom/parsed_chunk_queue.h"

#include <atomic>
#include <chrono>
#include <gtest/gtest.h>
#include <thread>

using namespace libvroom;

// Helper: create a single-column chunk with a known int32 value for identification
static std::vector<std::unique_ptr<ArrowColumnBuilder>> make_chunk(int32_t id_value) {
  std::vector<std::unique_ptr<ArrowColumnBuilder>> cols;
  auto col = ArrowColumnBuilder::create_int32();
  auto ctx = col->create_context();
  std::string_view sv = std::to_string(id_value);
  ctx.append_fn(ctx, sv);
  cols.push_back(std::move(col));
  return cols;
}

// Helper: extract the id value from a chunk created by make_chunk
static int32_t chunk_id(const std::vector<std::unique_ptr<ArrowColumnBuilder>>& cols) {
  auto& col = static_cast<ArrowInt32ColumnBuilder&>(*cols[0]);
  return col.values().data()[0];
}

// =============================================================================
// Basic Push/Pop Tests
// =============================================================================

TEST(ParsedChunkQueueTest, BasicPushPop) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);

  ASSERT_TRUE(queue.push(0, make_chunk(100)));
  ASSERT_TRUE(queue.push(1, make_chunk(200)));
  ASSERT_TRUE(queue.push(2, make_chunk(300)));

  auto c0 = queue.pop();
  ASSERT_TRUE(c0.has_value());
  EXPECT_EQ(chunk_id(*c0), 100);

  auto c1 = queue.pop();
  ASSERT_TRUE(c1.has_value());
  EXPECT_EQ(chunk_id(*c1), 200);

  auto c2 = queue.pop();
  ASSERT_TRUE(c2.has_value());
  EXPECT_EQ(chunk_id(*c2), 300);

  // All chunks consumed — should return nullopt
  auto c3 = queue.pop();
  EXPECT_FALSE(c3.has_value());
}

TEST(ParsedChunkQueueTest, SingleChunk) {
  ParsedChunkQueue queue(/*num_chunks=*/1, /*max_buffered=*/4);

  ASSERT_TRUE(queue.push(0, make_chunk(42)));

  auto c0 = queue.pop();
  ASSERT_TRUE(c0.has_value());
  EXPECT_EQ(chunk_id(*c0), 42);

  auto c1 = queue.pop();
  EXPECT_FALSE(c1.has_value());
}

// =============================================================================
// Ordered Delivery Tests
// =============================================================================

TEST(ParsedChunkQueueTest, OrderedDeliveryOutOfOrderPush) {
  ParsedChunkQueue queue(/*num_chunks=*/4, /*max_buffered=*/8);

  // Push out of order: 2, 0, 3, 1
  ASSERT_TRUE(queue.push(2, make_chunk(20)));
  ASSERT_TRUE(queue.push(0, make_chunk(0)));
  ASSERT_TRUE(queue.push(3, make_chunk(30)));
  ASSERT_TRUE(queue.push(1, make_chunk(10)));

  // Pop should deliver in order: 0, 1, 2, 3
  auto c0 = queue.pop();
  ASSERT_TRUE(c0.has_value());
  EXPECT_EQ(chunk_id(*c0), 0);

  auto c1 = queue.pop();
  ASSERT_TRUE(c1.has_value());
  EXPECT_EQ(chunk_id(*c1), 10);

  auto c2 = queue.pop();
  ASSERT_TRUE(c2.has_value());
  EXPECT_EQ(chunk_id(*c2), 20);

  auto c3 = queue.pop();
  ASSERT_TRUE(c3.has_value());
  EXPECT_EQ(chunk_id(*c3), 30);

  auto end = queue.pop();
  EXPECT_FALSE(end.has_value());
}

TEST(ParsedChunkQueueTest, ReversePushOrder) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/8);

  ASSERT_TRUE(queue.push(2, make_chunk(200)));
  ASSERT_TRUE(queue.push(1, make_chunk(100)));
  ASSERT_TRUE(queue.push(0, make_chunk(0)));

  for (int i = 0; i < 3; ++i) {
    auto c = queue.pop();
    ASSERT_TRUE(c.has_value());
    EXPECT_EQ(chunk_id(*c), i * 100);
  }
}

// =============================================================================
// Threading Tests: Blocking Until Chunk Ready
// =============================================================================

TEST(ParsedChunkQueueTest, ConsumerBlocksUntilChunkReady) {
  ParsedChunkQueue queue(/*num_chunks=*/2, /*max_buffered=*/4);

  std::atomic<bool> consumer_got_chunk{false};

  // Consumer thread — will block because chunk 0 is not yet pushed
  std::thread consumer([&] {
    auto c = queue.pop();
    if (c.has_value()) {
      consumer_got_chunk = true;
    }
  });

  // Give consumer time to block
  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(consumer_got_chunk.load());

  // Push chunk 0 — unblocks consumer
  queue.push(0, make_chunk(0));

  consumer.join();
  EXPECT_TRUE(consumer_got_chunk.load());
}

TEST(ParsedChunkQueueTest, ConsumerBlocksWhenNextChunkNotReady) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);

  // Push chunk 1 (not chunk 0) — consumer should block waiting for chunk 0
  queue.push(1, make_chunk(10));

  std::atomic<bool> consumer_got_chunk{false};

  std::thread consumer([&] {
    auto c = queue.pop();
    if (c.has_value()) {
      consumer_got_chunk = true;
    }
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(consumer_got_chunk.load());

  // Now push chunk 0 — consumer should get it
  queue.push(0, make_chunk(0));

  consumer.join();
  EXPECT_TRUE(consumer_got_chunk.load());
}

// =============================================================================
// Threading Tests: Backpressure
// =============================================================================

TEST(ParsedChunkQueueTest, BackpressureBlocksProducer) {
  // max_buffered=2 and num_chunks=4, so after 2 pushes producer blocks
  ParsedChunkQueue queue(/*num_chunks=*/4, /*max_buffered=*/2);

  // Fill the buffer
  queue.push(0, make_chunk(0));
  queue.push(1, make_chunk(10));

  std::atomic<bool> producer_completed{false};

  // Producer thread — should block because buffer is full
  std::thread producer([&] {
    queue.push(2, make_chunk(20));
    producer_completed = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(producer_completed.load());

  // Pop one chunk — frees space, unblocks producer
  auto c = queue.pop();
  ASSERT_TRUE(c.has_value());
  EXPECT_EQ(chunk_id(*c), 0);

  producer.join();
  EXPECT_TRUE(producer_completed.load());
}

// =============================================================================
// Close Semantics
// =============================================================================

TEST(ParsedChunkQueueTest, CloseUnblocksWaitingConsumer) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);

  std::atomic<bool> consumer_returned{false};
  std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> result;

  std::thread consumer([&] {
    result = queue.pop();
    consumer_returned = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(consumer_returned.load());

  queue.close();

  consumer.join();
  EXPECT_TRUE(consumer_returned.load());
  EXPECT_FALSE(result.has_value()); // closed without data -> nullopt
}

TEST(ParsedChunkQueueTest, CloseUnblocksWaitingProducer) {
  ParsedChunkQueue queue(/*num_chunks=*/4, /*max_buffered=*/1);

  // Fill the buffer
  queue.push(0, make_chunk(0));

  std::atomic<bool> producer_returned{false};
  bool push_result = true;

  std::thread producer([&] {
    push_result = queue.push(1, make_chunk(10));
    producer_returned = true;
  });

  std::this_thread::sleep_for(std::chrono::milliseconds(20));
  EXPECT_FALSE(producer_returned.load());

  queue.close();

  producer.join();
  EXPECT_TRUE(producer_returned.load());
  EXPECT_FALSE(push_result); // push returns false on closed queue
}

TEST(ParsedChunkQueueTest, PushReturnsFalseAfterClose) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);
  queue.close();

  bool result = queue.push(0, make_chunk(0));
  EXPECT_FALSE(result);
}

TEST(ParsedChunkQueueTest, PopReturnsNulloptAfterClose) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);
  queue.close();

  auto result = queue.pop();
  EXPECT_FALSE(result.has_value());
}

TEST(ParsedChunkQueueTest, IsClosedReflectsState) {
  ParsedChunkQueue queue(/*num_chunks=*/3, /*max_buffered=*/4);
  EXPECT_FALSE(queue.is_closed());

  queue.close();
  EXPECT_TRUE(queue.is_closed());
}

// =============================================================================
// Producer-Consumer Pipeline Test
// =============================================================================

TEST(ParsedChunkQueueTest, FullPipelineMultipleProducers) {
  constexpr size_t kNumChunks = 8;
  // Buffer must be large enough to hold all out-of-order chunks to avoid deadlock:
  // if chunk 0 has the longest delay, earlier-arriving high-index chunks must not
  // fill the buffer and block the producer of chunk 0.
  ParsedChunkQueue queue(kNumChunks, /*max_buffered=*/kNumChunks);

  // Consumer thread — collects all chunks in order
  std::vector<int32_t> received_ids;
  std::thread consumer([&] {
    while (true) {
      auto c = queue.pop();
      if (!c.has_value())
        break;
      received_ids.push_back(chunk_id(*c));
    }
  });

  // Producer threads — each pushes one chunk
  std::vector<std::thread> producers;
  for (size_t i = 0; i < kNumChunks; ++i) {
    producers.emplace_back([&queue, i] {
      // Small staggered delay to create out-of-order arrival
      // Higher indices arrive first to exercise reordering
      std::this_thread::sleep_for(std::chrono::milliseconds((kNumChunks - i) * 2));
      queue.push(i, make_chunk(static_cast<int32_t>(i * 10)));
    });
  }

  for (auto& t : producers) {
    t.join();
  }

  consumer.join();

  // Verify all chunks received in order
  ASSERT_EQ(received_ids.size(), kNumChunks);
  for (size_t i = 0; i < kNumChunks; ++i) {
    EXPECT_EQ(received_ids[i], static_cast<int32_t>(i * 10));
  }
}
