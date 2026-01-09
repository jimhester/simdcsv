/**
 * @file mmap_util_test.cpp
 * @brief Unit tests for MmapBuffer class.
 */

#include "mmap_util.h"

#include <cstdio>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <sys/stat.h>
#include <unistd.h>

using namespace libvroom;

// Test fixture for MmapBuffer tests
class MmapBufferTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary directory for test files
    char temp_dir[] = "/tmp/mmap_test_XXXXXX";
    ASSERT_NE(mkdtemp(temp_dir), nullptr);
    temp_dir_ = temp_dir;
  }

  void TearDown() override {
    // Clean up temp directory
    if (!temp_dir_.empty()) {
      std::string cmd = "rm -rf " + temp_dir_;
      int result = std::system(cmd.c_str());
      (void)result; // Ignore result
    }
  }

  std::string create_test_file(const std::string& name, const std::string& content) {
    std::string path = temp_dir_ + "/" + name;
    std::ofstream ofs(path, std::ios::binary);
    ofs << content;
    ofs.close();
    return path;
  }

  std::string temp_dir_;
};

// Test opening a valid file
TEST_F(MmapBufferTest, OpenValidFile) {
  std::string content = "Hello, World!";
  std::string path = create_test_file("test.txt", content);

  MmapBuffer buffer;
  EXPECT_FALSE(buffer.valid());

  ASSERT_TRUE(buffer.open(path));
  EXPECT_TRUE(buffer.valid());
  EXPECT_TRUE(buffer); // operator bool
  EXPECT_EQ(buffer.size(), content.size());
  EXPECT_EQ(std::memcmp(buffer.data(), content.data(), content.size()), 0);
  EXPECT_TRUE(buffer.error().empty());
}

// Test opening a non-existent file
TEST_F(MmapBufferTest, OpenNonExistentFile) {
  MmapBuffer buffer;

  EXPECT_FALSE(buffer.open(temp_dir_ + "/nonexistent.txt"));
  EXPECT_FALSE(buffer.valid());
  EXPECT_FALSE(buffer.error().empty());
  EXPECT_NE(buffer.error().find("Failed to open"), std::string::npos);
}

// Test opening an empty file
TEST_F(MmapBufferTest, OpenEmptyFile) {
  std::string path = create_test_file("empty.txt", "");

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path));
  EXPECT_TRUE(buffer.valid()); // Empty file is valid but has null data
  EXPECT_EQ(buffer.size(), 0u);
  EXPECT_EQ(buffer.data(), nullptr); // No mapping for empty file
}

// Test opening a file with read permission issues
TEST_F(MmapBufferTest, OpenFileWithoutReadPermission) {
  std::string path = create_test_file("noperm.txt", "secret");

  // Remove read permission
  chmod(path.c_str(), 0000);

  MmapBuffer buffer;
  EXPECT_FALSE(buffer.open(path));
  EXPECT_FALSE(buffer.valid());
  EXPECT_FALSE(buffer.error().empty());

  // Restore permission for cleanup
  chmod(path.c_str(), 0644);
}

// Test move constructor
TEST_F(MmapBufferTest, MoveConstructor) {
  std::string content = "Move test content";
  std::string path = create_test_file("move.txt", content);

  MmapBuffer buffer1;
  ASSERT_TRUE(buffer1.open(path));
  const uint8_t* original_data = buffer1.data();
  size_t original_size = buffer1.size();

  // Move construct
  MmapBuffer buffer2(std::move(buffer1));

  // buffer2 should have the data
  EXPECT_TRUE(buffer2.valid());
  EXPECT_EQ(buffer2.data(), original_data);
  EXPECT_EQ(buffer2.size(), original_size);

  // buffer1 should be empty
  EXPECT_FALSE(buffer1.valid());
  EXPECT_EQ(buffer1.data(), nullptr);
  EXPECT_EQ(buffer1.size(), 0u);
}

// Test move assignment operator
TEST_F(MmapBufferTest, MoveAssignment) {
  std::string content1 = "First content";
  std::string content2 = "Second content";
  std::string path1 = create_test_file("first.txt", content1);
  std::string path2 = create_test_file("second.txt", content2);

  MmapBuffer buffer1;
  MmapBuffer buffer2;
  ASSERT_TRUE(buffer1.open(path1));
  ASSERT_TRUE(buffer2.open(path2));

  const uint8_t* data2 = buffer2.data();
  size_t size2 = buffer2.size();

  // Move assign buffer2 to buffer1
  buffer1 = std::move(buffer2);

  // buffer1 should have buffer2's data
  EXPECT_TRUE(buffer1.valid());
  EXPECT_EQ(buffer1.data(), data2);
  EXPECT_EQ(buffer1.size(), size2);

  // buffer2 should be empty
  EXPECT_FALSE(buffer2.valid());
  EXPECT_EQ(buffer2.data(), nullptr);
}

// Test close method
TEST_F(MmapBufferTest, Close) {
  std::string path = create_test_file("close.txt", "Close test");

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path));
  EXPECT_TRUE(buffer.valid());

  buffer.close();
  EXPECT_FALSE(buffer.valid());
  EXPECT_EQ(buffer.data(), nullptr);
  EXPECT_EQ(buffer.size(), 0u);

  // Double close should be safe
  buffer.close();
  EXPECT_FALSE(buffer.valid());
}

// Test get_metadata method
TEST_F(MmapBufferTest, GetMetadata) {
  std::string content = "Metadata test content";
  std::string path = create_test_file("meta.txt", content);

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path));

  time_t mtime;
  size_t file_size;
  ASSERT_TRUE(buffer.get_metadata(mtime, file_size));

  EXPECT_GT(mtime, 0);
  EXPECT_EQ(file_size, content.size());
}

// Test get_file_metadata static method
TEST_F(MmapBufferTest, GetFileMetadata) {
  std::string content = "Static metadata test";
  std::string path = create_test_file("static_meta.txt", content);

  time_t mtime;
  size_t file_size;
  ASSERT_TRUE(MmapBuffer::get_file_metadata(path, mtime, file_size));

  EXPECT_GT(mtime, 0);
  EXPECT_EQ(file_size, content.size());
}

// Test get_file_metadata with non-existent file
TEST_F(MmapBufferTest, GetFileMetadataNonExistent) {
  time_t mtime;
  size_t file_size;
  EXPECT_FALSE(MmapBuffer::get_file_metadata(temp_dir_ + "/nonexistent", mtime, file_size));
}

// Test opening with read-only mode (default)
TEST_F(MmapBufferTest, OpenReadOnly) {
  std::string content = "Read-only content";
  std::string path = create_test_file("readonly.txt", content);

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path, true)); // read_only = true

  EXPECT_TRUE(buffer.valid());
  EXPECT_EQ(buffer.size(), content.size());

  // Should be able to read
  EXPECT_EQ(std::memcmp(buffer.data(), content.data(), content.size()), 0);
}

// Test opening with read-write mode
TEST_F(MmapBufferTest, OpenReadWrite) {
  std::string content = "Read-write content";
  std::string path = create_test_file("readwrite.txt", content);

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path, false)); // read_only = false

  EXPECT_TRUE(buffer.valid());
  EXPECT_EQ(buffer.size(), content.size());
}

// Test reopening a file (should close previous mapping)
TEST_F(MmapBufferTest, Reopen) {
  std::string content1 = "First file content";
  std::string content2 = "Second file content";
  std::string path1 = create_test_file("reopen1.txt", content1);
  std::string path2 = create_test_file("reopen2.txt", content2);

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path1));
  EXPECT_EQ(buffer.size(), content1.size());

  // Open second file (should close first)
  ASSERT_TRUE(buffer.open(path2));
  EXPECT_EQ(buffer.size(), content2.size());
  EXPECT_EQ(std::memcmp(buffer.data(), content2.data(), content2.size()), 0);
}

// Test with binary content
TEST_F(MmapBufferTest, BinaryContent) {
  std::string path = temp_dir_ + "/binary.bin";

  // Write binary content with null bytes
  std::ofstream ofs(path, std::ios::binary);
  uint8_t binary_data[] = {0x00, 0xFF, 0x00, 0xFF, 0x42, 0x00, 0x43};
  ofs.write(reinterpret_cast<const char*>(binary_data), sizeof(binary_data));
  ofs.close();

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path));
  EXPECT_EQ(buffer.size(), sizeof(binary_data));
  EXPECT_EQ(std::memcmp(buffer.data(), binary_data, sizeof(binary_data)), 0);
}

// Test large file (to ensure mmap works beyond small buffer sizes)
TEST_F(MmapBufferTest, LargeFile) {
  std::string path = temp_dir_ + "/large.bin";

  // Create a 1MB file
  constexpr size_t size = 1024 * 1024;
  std::vector<uint8_t> data(size);
  for (size_t i = 0; i < size; ++i) {
    data[i] = static_cast<uint8_t>(i & 0xFF);
  }

  std::ofstream ofs(path, std::ios::binary);
  ofs.write(reinterpret_cast<const char*>(data.data()), size);
  ofs.close();

  MmapBuffer buffer;
  ASSERT_TRUE(buffer.open(path));
  EXPECT_EQ(buffer.size(), size);

  // Verify content
  EXPECT_EQ(std::memcmp(buffer.data(), data.data(), size), 0);
}
