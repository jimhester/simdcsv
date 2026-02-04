#include "libvroom.h"

#include "test_util.h"

#include <cstring>
#include <gtest/gtest.h>
#include <string>

class StreamingCsvReaderTest : public ::testing::Test {
protected:
  std::string testDataPath(const std::string& subpath) { return "test/data/" + subpath; }
};

TEST_F(StreamingCsvReaderTest, BasicStreaming) {
  std::string csv = "a,b,c\n1,2,3\n4,5,6\n7,8,9\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto open_result = reader.open(f.path());
  ASSERT_TRUE(open_result.ok);

  auto start_result = reader.start_streaming();
  ASSERT_TRUE(start_result.ok);

  size_t total_rows = 0;
  size_t chunk_count = 0;
  while (auto chunk = reader.next_chunk()) {
    chunk_count++;
    if (!chunk->empty()) {
      total_rows += (*chunk)[0]->size();
    }
  }
  EXPECT_EQ(total_rows, 3u);
  EXPECT_GE(chunk_count, 1u);
}

TEST_F(StreamingCsvReaderTest, StreamingMatchesReadAll) {
  std::string csv = "x,y\n1,hello\n2,world\n3,foo\n4,bar\n5,baz\n";
  test_util::TempCsvFile f(csv);

  // read_all path
  libvroom::CsvReader reader1(libvroom::CsvOptions{});
  reader1.open(f.path());
  auto all = reader1.read_all();
  ASSERT_TRUE(all.ok);

  // streaming path
  libvroom::CsvReader reader2(libvroom::CsvOptions{});
  reader2.open(f.path());
  auto start = reader2.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t streaming_rows = 0;
  while (auto chunk = reader2.next_chunk()) {
    if (!chunk->empty()) {
      streaming_rows += (*chunk)[0]->size();
    }
  }

  EXPECT_EQ(streaming_rows, all.value.total_rows);
}

TEST_F(StreamingCsvReaderTest, StartStreamingBeforeOpen) {
  libvroom::CsvReader reader(libvroom::CsvOptions{});
  auto result = reader.start_streaming();
  EXPECT_FALSE(result.ok);
}

TEST_F(StreamingCsvReaderTest, DoubleStartStreaming) {
  std::string csv = "a\n1\n2\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());
  auto start1 = reader.start_streaming();
  ASSERT_TRUE(start1.ok);

  auto start2 = reader.start_streaming();
  EXPECT_FALSE(start2.ok);
}

TEST_F(StreamingCsvReaderTest, NextChunkWithoutStartStreaming) {
  std::string csv = "a\n1\n";
  test_util::TempCsvFile f(csv);

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open(f.path());

  auto chunk = reader.next_chunk();
  EXPECT_FALSE(chunk.has_value());
}

TEST_F(StreamingCsvReaderTest, StreamingFromBuffer) {
  std::string csv = "x,y\n1,2\n3,4\n";
  auto buf = libvroom::AlignedBuffer::allocate(csv.size());
  std::memcpy(buf.data(), csv.data(), csv.size());

  libvroom::CsvReader reader(libvroom::CsvOptions{});
  reader.open_from_buffer(std::move(buf));
  auto start = reader.start_streaming();
  ASSERT_TRUE(start.ok);

  size_t total = 0;
  while (auto chunk = reader.next_chunk()) {
    if (!chunk->empty())
      total += (*chunk)[0]->size();
  }
  EXPECT_EQ(total, 2u);
}
