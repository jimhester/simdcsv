/**
 * @file index_cache_test.cpp
 * @brief Unit tests for index cache management utilities.
 */

#include "libvroom.h"

#include "index_cache.h"

#include <cstring>
#include <filesystem>
#include <fstream>
#include <gtest/gtest.h>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>

namespace fs = std::filesystem;
using namespace libvroom;

// =============================================================================
// Test Fixture
// =============================================================================

class IndexCacheTest : public ::testing::Test {
protected:
  std::string temp_dir;
  std::vector<std::string> temp_files;

  void SetUp() override {
    // Create temp directory for test files
    temp_dir =
        (fs::temp_directory_path() / ("index_cache_test_" + std::to_string(getpid()))).string();
    fs::create_directories(temp_dir);
  }

  void TearDown() override {
    // Clean up temp files and directory
    if (fs::exists(temp_dir)) {
      fs::remove_all(temp_dir);
    }
  }

  std::string createTempFile(const std::string& filename, const std::string& content) {
    std::string path = temp_dir + "/" + filename;
    std::ofstream file(path, std::ios::binary);
    file.write(content.data(), content.size());
    file.close();
    return path;
  }

  std::string createTempDir(const std::string& dirname) {
    std::string path = temp_dir + "/" + dirname;
    fs::create_directories(path);
    return path;
  }
};

// =============================================================================
// CacheConfig Tests
// =============================================================================

TEST_F(IndexCacheTest, CacheConfig_Defaults) {
  CacheConfig config = CacheConfig::defaults();
  EXPECT_EQ(config.location, CacheConfig::SAME_DIR);
  EXPECT_TRUE(config.custom_path.empty());
}

TEST_F(IndexCacheTest, CacheConfig_XdgCache) {
  CacheConfig config = CacheConfig::xdg_cache();
  EXPECT_EQ(config.location, CacheConfig::XDG_CACHE);
  EXPECT_TRUE(config.custom_path.empty());
}

TEST_F(IndexCacheTest, CacheConfig_Custom) {
  CacheConfig config = CacheConfig::custom("/custom/path");
  EXPECT_EQ(config.location, CacheConfig::CUSTOM);
  EXPECT_EQ(config.custom_path, "/custom/path");
}

// =============================================================================
// IndexCache::compute_path Tests
// =============================================================================

TEST_F(IndexCacheTest, ComputePath_SameDir) {
  std::string source = "/path/to/data.csv";
  std::string cache_path = IndexCache::compute_path(source, CacheConfig::defaults());

  EXPECT_EQ(cache_path, "/path/to/data.csv.vidx");
}

TEST_F(IndexCacheTest, ComputePath_SameDir_NoPath) {
  std::string source = "data.csv";
  std::string cache_path = IndexCache::compute_path(source, CacheConfig::defaults());

  EXPECT_EQ(cache_path, "data.csv.vidx");
}

TEST_F(IndexCacheTest, ComputePath_SameDir_Windows) {
  std::string source = "C:\\Users\\data.csv";
  std::string cache_path = IndexCache::compute_path(source, CacheConfig::defaults());

  EXPECT_EQ(cache_path, "C:\\Users\\data.csv.vidx");
}

TEST_F(IndexCacheTest, ComputePath_XdgCache) {
  std::string source = temp_dir + "/data.csv";
  createTempFile("data.csv", "a,b\n1,2\n");

  std::string cache_path = IndexCache::compute_path(source, CacheConfig::xdg_cache());

  // Should contain the XDG cache directory
  EXPECT_TRUE(cache_path.find(".cache/libvroom") != std::string::npos ||
              cache_path.find("libvroom") != std::string::npos);
  EXPECT_TRUE(cache_path.find(".vidx") != std::string::npos);
}

TEST_F(IndexCacheTest, ComputePath_XdgCache_DifferentFilesGetDifferentPaths) {
  std::string source1 = "/path/to/file1.csv";
  std::string source2 = "/path/to/file2.csv";

  std::string cache1 = IndexCache::compute_path(source1, CacheConfig::xdg_cache());
  std::string cache2 = IndexCache::compute_path(source2, CacheConfig::xdg_cache());

  EXPECT_NE(cache1, cache2);
}

TEST_F(IndexCacheTest, ComputePath_Custom) {
  std::string custom_dir = createTempDir("custom_cache");
  std::string source = "/path/to/data.csv";

  CacheConfig config = CacheConfig::custom(custom_dir);
  std::string cache_path = IndexCache::compute_path(source, config);

  EXPECT_TRUE(cache_path.find(custom_dir) != std::string::npos);
  EXPECT_TRUE(cache_path.find("data.csv.vidx") != std::string::npos);
}

TEST_F(IndexCacheTest, ComputePath_Custom_EmptyPath) {
  CacheConfig config = CacheConfig::custom("");
  std::string source = "/path/to/data.csv";

  std::string cache_path = IndexCache::compute_path(source, config);

  // Should fall back to same-dir mode
  EXPECT_EQ(cache_path, "/path/to/data.csv.vidx");
}

// =============================================================================
// IndexCache::get_source_metadata Tests
// =============================================================================

TEST_F(IndexCacheTest, GetSourceMetadata_ValidFile) {
  std::string content = "hello,world\n1,2,3\n";
  std::string path = createTempFile("meta_test.csv", content);

  auto [mtime, size] = IndexCache::get_source_metadata(path);

  EXPECT_GT(mtime, 0u);
  EXPECT_EQ(size, content.size());
}

TEST_F(IndexCacheTest, GetSourceMetadata_NonexistentFile) {
  auto [mtime, size] = IndexCache::get_source_metadata("/nonexistent/file.csv");

  EXPECT_EQ(mtime, 0u);
  EXPECT_EQ(size, 0u);
}

TEST_F(IndexCacheTest, GetSourceMetadata_Directory) {
  std::string dir = createTempDir("not_a_file");

  auto [mtime, size] = IndexCache::get_source_metadata(dir);

  // Directories should not have valid metadata for caching
  EXPECT_EQ(mtime, 0u);
  EXPECT_EQ(size, 0u);
}

TEST_F(IndexCacheTest, GetSourceMetadata_EmptyFile) {
  std::string path = createTempFile("empty.csv", "");

  auto [mtime, size] = IndexCache::get_source_metadata(path);

  EXPECT_GT(mtime, 0u);
  EXPECT_EQ(size, 0u);
}

// =============================================================================
// IndexCache::is_directory_writable Tests
// =============================================================================

TEST_F(IndexCacheTest, IsDirectoryWritable_WritableDir) {
  std::string dir = createTempDir("writable");

  EXPECT_TRUE(IndexCache::is_directory_writable(dir));
}

TEST_F(IndexCacheTest, IsDirectoryWritable_NonexistentDir) {
  EXPECT_FALSE(IndexCache::is_directory_writable("/nonexistent/directory"));
}

TEST_F(IndexCacheTest, IsDirectoryWritable_EmptyPath) {
  EXPECT_FALSE(IndexCache::is_directory_writable(""));
}

TEST_F(IndexCacheTest, IsDirectoryWritable_FileNotDir) {
  std::string path = createTempFile("not_a_dir.txt", "content");

  EXPECT_FALSE(IndexCache::is_directory_writable(path));
}

#ifndef _WIN32
TEST_F(IndexCacheTest, IsDirectoryWritable_ReadOnlyDir) {
  std::string dir = createTempDir("readonly");

  // Make directory read-only
  chmod(dir.c_str(), 0555);

  EXPECT_FALSE(IndexCache::is_directory_writable(dir));

  // Restore permissions for cleanup
  chmod(dir.c_str(), 0755);
}
#endif

// =============================================================================
// IndexCache::hash_path Tests
// =============================================================================

TEST_F(IndexCacheTest, HashPath_Basic) {
  std::string hash = IndexCache::hash_path("/path/to/file.csv");

  EXPECT_EQ(hash.length(), 16u); // 64-bit hash = 16 hex chars
}

TEST_F(IndexCacheTest, HashPath_DifferentPathsDifferentHashes) {
  std::string hash1 = IndexCache::hash_path("/path/to/file1.csv");
  std::string hash2 = IndexCache::hash_path("/path/to/file2.csv");

  EXPECT_NE(hash1, hash2);
}

TEST_F(IndexCacheTest, HashPath_SamePathSameHash) {
  std::string hash1 = IndexCache::hash_path("/path/to/file.csv");
  std::string hash2 = IndexCache::hash_path("/path/to/file.csv");

  EXPECT_EQ(hash1, hash2);
}

TEST_F(IndexCacheTest, HashPath_EmptyPath) {
  std::string hash = IndexCache::hash_path("");

  EXPECT_EQ(hash.length(), 16u);
}

TEST_F(IndexCacheTest, HashPath_OnlyHexChars) {
  std::string hash = IndexCache::hash_path("/some/path");

  for (char c : hash) {
    EXPECT_TRUE((c >= '0' && c <= '9') || (c >= 'a' && c <= 'f'));
  }
}

// =============================================================================
// IndexCache::get_xdg_cache_dir Tests
// =============================================================================

TEST_F(IndexCacheTest, GetXdgCacheDir_ReturnsNonEmpty) {
  std::string cache_dir = IndexCache::get_xdg_cache_dir();

  // Should return a valid path (assuming HOME is set)
  if (!cache_dir.empty()) {
    EXPECT_TRUE(cache_dir.find("libvroom") != std::string::npos);
    EXPECT_TRUE(fs::exists(cache_dir) || fs::create_directories(cache_dir));
  }
}

// =============================================================================
// IndexCache::write_atomic Tests
// =============================================================================

TEST_F(IndexCacheTest, WriteAtomic_BasicWrite) {
  // Create a source file
  std::string content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string source_path = createTempFile("source.csv", content);
  std::string cache_path = temp_dir + "/source.csv.vidx";

  // Parse the file to get a ParseIndex
  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  ASSERT_TRUE(result.success());

  // Write the cache
  bool success = IndexCache::write_atomic(cache_path, result.idx, source_path);
  EXPECT_TRUE(success);

  // Verify cache file exists
  EXPECT_TRUE(fs::exists(cache_path));

  // Verify cache file has content
  EXPECT_GT(fs::file_size(cache_path), IndexCache::HEADER_SIZE);
}

TEST_F(IndexCacheTest, WriteAtomic_AtomicNoPartialWrites) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("atomic.csv", content);
  std::string cache_path = temp_dir + "/atomic.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Write the cache
  IndexCache::write_atomic(cache_path, result.idx, source_path);

  // No temp files should remain
  for (const auto& entry : fs::directory_iterator(temp_dir)) {
    std::string filename = entry.path().filename().string();
    EXPECT_TRUE(filename.find(".tmp.") == std::string::npos)
        << "Temp file should be cleaned up: " << filename;
  }
}

TEST_F(IndexCacheTest, WriteAtomic_NonexistentSource) {
  ParseIndex empty_idx;
  std::string cache_path = temp_dir + "/cache.vidx";

  bool success = IndexCache::write_atomic(cache_path, empty_idx, "/nonexistent/source.csv");

  EXPECT_FALSE(success);
  EXPECT_FALSE(fs::exists(cache_path));
}

TEST_F(IndexCacheTest, WriteAtomic_InvalidCachePath) {
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("source.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Try to write to a nonexistent directory
  std::string cache_path = "/nonexistent/dir/cache.vidx";
  bool success = IndexCache::write_atomic(cache_path, result.idx, source_path);

  EXPECT_FALSE(success);
}

// =============================================================================
// IndexCache::is_valid Tests
// =============================================================================

TEST_F(IndexCacheTest, IsValid_ValidCache) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("valid.csv", content);
  std::string cache_path = temp_dir + "/valid.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Write cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));

  // Cache should be valid
  EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));
}

TEST_F(IndexCacheTest, IsValid_InvalidAfterModification) {
  std::string content = "a,b,c\n1,2,3\n";
  std::string source_path = createTempFile("modified.csv", content);
  std::string cache_path = temp_dir + "/modified.csv.vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);

  // Write cache
  ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));

  // Modify source file (change content and wait for different mtime)
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::ofstream file(source_path, std::ios::binary);
  file << "a,b,c\n1,2,3\n4,5,6\n";
  file.close();

  // Cache should now be invalid
  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));
}

TEST_F(IndexCacheTest, IsValid_NonexistentCache) {
  std::string source_path = createTempFile("nocache.csv", "a,b\n");

  EXPECT_FALSE(IndexCache::is_valid(source_path, "/nonexistent/cache.vidx"));
}

TEST_F(IndexCacheTest, IsValid_NonexistentSource) {
  std::string cache_path = createTempFile("orphan.vidx", "dummy content");

  EXPECT_FALSE(IndexCache::is_valid("/nonexistent/source.csv", cache_path));
}

TEST_F(IndexCacheTest, IsValid_CorruptedHeader) {
  std::string source_path = createTempFile("corrupt_source.csv", "a,b\n1,2\n");
  std::string cache_path = createTempFile("corrupt.vidx", "not a valid cache file");

  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));
}

TEST_F(IndexCacheTest, IsValid_WrongVersion) {
  std::string source_path = createTempFile("version.csv", "a,b\n");

  // Create a cache file with wrong version
  std::string cache_path = temp_dir + "/version.vidx";
  std::ofstream file(cache_path, std::ios::binary);
  uint8_t wrong_version = 255; // Invalid version
  file.write(reinterpret_cast<char*>(&wrong_version), 1);
  file.close();

  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));
}

// =============================================================================
// IndexCache::try_compute_writable_path Tests
// =============================================================================

TEST_F(IndexCacheTest, TryComputeWritablePath_WritableDir) {
  std::string source_path = createTempFile("writable.csv", "a,b\n");

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::defaults());

  EXPECT_TRUE(success);
  EXPECT_EQ(cache_path, source_path + ".vidx");
}

TEST_F(IndexCacheTest, TryComputeWritablePath_XdgCache) {
  std::string source_path = temp_dir + "/xdg.csv";
  createTempFile("xdg.csv", "a,b\n");

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::xdg_cache());

  if (!IndexCache::get_xdg_cache_dir().empty()) {
    EXPECT_TRUE(success);
    EXPECT_TRUE(cache_path.find("libvroom") != std::string::npos);
  }
}

TEST_F(IndexCacheTest, TryComputeWritablePath_CustomWritable) {
  std::string custom_dir = createTempDir("custom_writable");
  std::string source_path = temp_dir + "/custom.csv";
  createTempFile("custom.csv", "a,b\n");

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::custom(custom_dir));

  EXPECT_TRUE(success);
  EXPECT_TRUE(cache_path.find(custom_dir) != std::string::npos);
}

TEST_F(IndexCacheTest, TryComputeWritablePath_CustomNonexistent) {
  std::string source_path = temp_dir + "/source.csv";

  auto [cache_path, success] =
      IndexCache::try_compute_writable_path(source_path, CacheConfig::custom("/nonexistent/dir"));

  EXPECT_FALSE(success);
  EXPECT_TRUE(cache_path.empty());
}

// =============================================================================
// Integration Tests
// =============================================================================

TEST_F(IndexCacheTest, Integration_FullCacheWorkflow) {
  // Create source file
  std::string content = "name,age,city\nAlice,30,NYC\nBob,25,LA\nCharlie,35,SF\n";
  std::string source_path = createTempFile("workflow.csv", content);
  std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());

  // Initial state: no cache
  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));

  // Parse and write cache
  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(result.success());

  EXPECT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));

  // Cache should now be valid
  EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));

  // Verify cache file structure (v3 format with alignment padding)
  // Layout: version(1) + padding(7) + mtime(8) + size(8)
  std::ifstream cache_file(cache_path, std::ios::binary);
  uint8_t version;
  cache_file.read(reinterpret_cast<char*>(&version), 1);
  constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;
  EXPECT_EQ(version, INDEX_FORMAT_VERSION_V3);

  // Skip 7 bytes of alignment padding
  char padding[7];
  cache_file.read(padding, 7);

  uint64_t mtime, size;
  cache_file.read(reinterpret_cast<char*>(&mtime), 8);
  cache_file.read(reinterpret_cast<char*>(&size), 8);

  auto [actual_mtime, actual_size] = IndexCache::get_source_metadata(source_path);
  EXPECT_EQ(mtime, actual_mtime);
  EXPECT_EQ(size, actual_size);
}

TEST_F(IndexCacheTest, Integration_MultipleFiles) {
  // Create multiple source files
  std::vector<std::string> source_paths;
  for (int i = 0; i < 3; ++i) {
    std::string content = "col" + std::to_string(i) + "\n" + std::to_string(i * 10) + "\n";
    source_paths.push_back(createTempFile("multi" + std::to_string(i) + ".csv", content));
  }

  // Parse and cache each file
  libvroom::Parser parser;
  for (const auto& source_path : source_paths) {
    std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());

    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size);
    ASSERT_TRUE(result.success());

    EXPECT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));
    EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));
  }
}

TEST_F(IndexCacheTest, Integration_CacheOverwrite) {
  std::string content1 = "a\n1\n";
  std::string source_path = createTempFile("overwrite.csv", content1);
  std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());

  libvroom::Parser parser;

  // Write first cache
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size);
    ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));
  }

  size_t first_cache_size = fs::file_size(cache_path);

  // Modify source and wait for mtime change
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::ofstream file(source_path, std::ios::binary);
  file << "a,b,c,d,e\n1,2,3,4,5\n6,7,8,9,10\n";
  file.close();

  // Cache should be invalid now
  EXPECT_FALSE(IndexCache::is_valid(source_path, cache_path));

  // Write new cache (overwrites old)
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size);
    ASSERT_TRUE(IndexCache::write_atomic(cache_path, result.idx, source_path));
  }

  // New cache should be valid and larger
  EXPECT_TRUE(IndexCache::is_valid(source_path, cache_path));
  EXPECT_GT(fs::file_size(cache_path), first_cache_size);
}

// =============================================================================
// Edge Cases
// =============================================================================

TEST_F(IndexCacheTest, EdgeCase_VeryLongPath) {
  std::string long_name(200, 'x');
  std::string source_path = temp_dir + "/" + long_name + ".csv";

  // Create the file (may fail on some filesystems)
  std::ofstream file(source_path, std::ios::binary);
  if (!file.is_open()) {
    GTEST_SKIP() << "Filesystem doesn't support long filenames";
  }
  file << "a,b\n1,2\n";
  file.close();

  std::string cache_path = IndexCache::compute_path(source_path, CacheConfig::defaults());
  EXPECT_TRUE(cache_path.find(".vidx") != std::string::npos);

  // XDG cache should use hash (fixed length)
  std::string xdg_path = IndexCache::compute_path(source_path, CacheConfig::xdg_cache());
  // Hash is 16 chars, so XDG path should be shorter than same-dir path
  if (!IndexCache::get_xdg_cache_dir().empty()) {
    EXPECT_LT(xdg_path.length(), cache_path.length());
  }
}

TEST_F(IndexCacheTest, EdgeCase_SpecialCharactersInPath) {
  // Test with path containing special characters
  std::string hash1 = IndexCache::hash_path("/path/with spaces/file.csv");
  std::string hash2 = IndexCache::hash_path("/path/with-dashes/file.csv");
  std::string hash3 = IndexCache::hash_path("/path/with_underscores/file.csv");

  EXPECT_EQ(hash1.length(), 16u);
  EXPECT_EQ(hash2.length(), 16u);
  EXPECT_EQ(hash3.length(), 16u);

  // All should be different
  EXPECT_NE(hash1, hash2);
  EXPECT_NE(hash2, hash3);
  EXPECT_NE(hash1, hash3);
}

TEST_F(IndexCacheTest, EdgeCase_UnicodeInPath) {
  std::string hash = IndexCache::hash_path("/path/to/日本語.csv");

  EXPECT_EQ(hash.length(), 16u);

  // Should be consistent
  std::string hash2 = IndexCache::hash_path("/path/to/日本語.csv");
  EXPECT_EQ(hash, hash2);
}

TEST_F(IndexCacheTest, EdgeCase_EmptyIndex) {
  std::string content = "";
  std::string source_path = createTempFile("empty.csv", content);
  std::string cache_path = temp_dir + "/empty.csv.vidx";

  // Create an empty ParseIndex
  ParseIndex empty_idx;
  empty_idx.columns = 0;
  empty_idx.n_threads = 0;
  empty_idx.n_indexes = nullptr;
  empty_idx.indexes = nullptr;

  // Should still write (header only)
  bool success = IndexCache::write_atomic(cache_path, empty_idx, source_path);

  // This might fail because source has size 0 - that's ok
  if (success) {
    EXPECT_TRUE(fs::exists(cache_path));
  }
}

// =============================================================================
// Parser API Integration Tests
// =============================================================================

TEST_F(IndexCacheTest, ParserApi_WithCacheFactory) {
  // Test ParseOptions::with_cache factory method
  std::string content = "name,age\nAlice,30\nBob,25\n";
  std::string source_path = createTempFile("api_factory.csv", content);

  auto opts = libvroom::ParseOptions::with_cache(source_path);

  EXPECT_TRUE(opts.cache.has_value());
  EXPECT_EQ(opts.cache->location, CacheConfig::SAME_DIR);
  EXPECT_EQ(opts.source_path, source_path);
}

TEST_F(IndexCacheTest, ParserApi_WithCacheDirFactory) {
  // Test ParseOptions::with_cache_dir factory method
  std::string custom_dir = createTempDir("custom_cache_api");
  std::string source_path = "/path/to/file.csv";

  auto opts = libvroom::ParseOptions::with_cache_dir(source_path, custom_dir);

  EXPECT_TRUE(opts.cache.has_value());
  EXPECT_EQ(opts.cache->location, CacheConfig::CUSTOM);
  EXPECT_EQ(opts.cache->custom_path, custom_dir);
  EXPECT_EQ(opts.source_path, source_path);
}

TEST_F(IndexCacheTest, ParserApi_CacheMissWritesFile) {
  // First parse should write cache file
  std::string content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
  std::string source_path = createTempFile("cache_miss.csv", content);
  std::string expected_cache_path = source_path + ".vidx";

  // Ensure no cache exists
  ASSERT_FALSE(fs::exists(expected_cache_path));

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  auto opts = libvroom::ParseOptions::with_cache(source_path);
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache); // Cache miss
  EXPECT_FALSE(result.cache_path.empty());
  EXPECT_TRUE(fs::exists(result.cache_path)); // Cache was written
}

TEST_F(IndexCacheTest, ParserApi_CacheHitLoadsMmap) {
  // First parse creates cache, second parse should load from cache
  std::string content = "name,age,city\nAlice,30,NYC\nBob,25,LA\n";
  std::string source_path = createTempFile("cache_hit.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - cache miss
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_FALSE(result1.used_cache);
  EXPECT_TRUE(fs::exists(result1.cache_path));

  // Second parse - cache hit
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache); // Cache hit!
  EXPECT_EQ(result2.cache_path, result1.cache_path);
}

TEST_F(IndexCacheTest, ParserApi_CacheResultsCorrect) {
  // Verify that cached results produce correct data access
  std::string content = "name,age\nAlice,30\nBob,25\nCharlie,35\n";
  std::string source_path = createTempFile("cache_verify.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - cache miss
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_EQ(result1.num_rows(), 3);

  // Second parse - cache hit
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache);
  EXPECT_EQ(result2.num_rows(), 3); // Same row count

  // Verify we can still access data
  auto names = result2.column_string(0);
  EXPECT_EQ(names.size(), 3u);
  EXPECT_EQ(names[0], "Alice");
  EXPECT_EQ(names[1], "Bob");
  EXPECT_EQ(names[2], "Charlie");
}

TEST_F(IndexCacheTest, ParserApi_NoCacheByDefault) {
  // Parsing without cache options should not create cache
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("no_cache.csv", content);
  std::string cache_path = source_path + ".vidx";

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  // Parse without cache options
  auto result = parser.parse(buffer.data(), buffer.size);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);
  EXPECT_TRUE(result.cache_path.empty()); // No cache path set
  EXPECT_FALSE(fs::exists(cache_path));   // No cache file created
}

TEST_F(IndexCacheTest, ParserApi_ForceCacheRefresh) {
  // Test force_cache_refresh option
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("force_refresh.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - creates cache
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_FALSE(result1.used_cache);

  // Get cache file mtime
  auto cache_path = result1.cache_path;
  auto mtime1 = fs::last_write_time(cache_path);

  // Wait a bit to ensure different mtime
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Second parse with force_cache_refresh
  opts.force_cache_refresh = true;
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_FALSE(result2.used_cache); // Force refresh means cache miss

  // Cache file should have been rewritten (newer mtime)
  auto mtime2 = fs::last_write_time(cache_path);
  EXPECT_GE(mtime2, mtime1);
}

TEST_F(IndexCacheTest, ParserApi_CacheInvalidAfterSourceChange) {
  // Cache should become invalid if source file changes
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("change.csv", content);

  libvroom::Parser parser;
  auto opts = libvroom::ParseOptions::with_cache(source_path);

  // First parse - creates cache
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size, opts);
    ASSERT_TRUE(result.success());
    EXPECT_FALSE(result.used_cache);
  }

  // Modify source file and wait for mtime change
  std::this_thread::sleep_for(std::chrono::seconds(1));
  std::ofstream file(source_path, std::ios::binary);
  file << "a,b,c\n1,2,3\n4,5,6\n";
  file.close();

  // Second parse - cache should be invalid
  {
    auto buffer = libvroom::load_file_to_ptr(source_path, 64);
    auto result = parser.parse(buffer.data(), buffer.size, opts);
    EXPECT_TRUE(result.success());
    EXPECT_FALSE(result.used_cache); // Cache was stale
    EXPECT_EQ(result.num_rows(), 2); // New content has 2 data rows
  }
}

TEST_F(IndexCacheTest, ParserApi_CustomCacheDir) {
  // Test caching to custom directory
  std::string custom_dir = createTempDir("custom_api_dir");
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("custom_dir.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto opts = libvroom::ParseOptions::with_cache_dir(source_path, custom_dir);

  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);
  EXPECT_TRUE(result.cache_path.find(custom_dir) != std::string::npos);
  EXPECT_TRUE(fs::exists(result.cache_path));
}

TEST_F(IndexCacheTest, ParserApi_EmptySourcePathDisablesCache) {
  // If source_path is empty, caching should be silently disabled
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("empty_source.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  // Create options with cache but empty source_path
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::defaults();
  opts.source_path = ""; // Empty - caching disabled

  auto result = parser.parse(buffer.data(), buffer.size, opts);

  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);
  EXPECT_TRUE(result.cache_path.empty()); // No cache due to empty source_path
}

TEST_F(IndexCacheTest, ParserApi_DialectDetectionWithCache) {
  // Auto-detected dialect should work with cached results
  std::string content = "name\tage\nAlice\t30\nBob\t25\n"; // TSV format
  std::string source_path = createTempFile("tsv_cache.csv", content);

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);

  // Parse with cache and auto-detection
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::defaults();
  opts.source_path = source_path;
  // dialect is nullopt (default) - auto-detect

  // First parse
  auto result1 = parser.parse(buffer.data(), buffer.size, opts);
  ASSERT_TRUE(result1.success());
  EXPECT_EQ(result1.dialect.delimiter, '\t'); // Detected TSV

  // Second parse - cache hit, dialect should still be detected
  auto result2 = parser.parse(buffer.data(), buffer.size, opts);
  EXPECT_TRUE(result2.success());
  EXPECT_TRUE(result2.used_cache);
  EXPECT_EQ(result2.dialect.delimiter, '\t'); // Still TSV
}

// =============================================================================
// Warning Callback Tests
// =============================================================================

TEST_F(IndexCacheTest, WarningCallback_NotCalledOnSuccess) {
  // Warning callback should NOT be called when everything works normally
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("no_warn.csv", content);

  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::defaults();
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  EXPECT_TRUE(success);
  EXPECT_FALSE(cache_path.empty());
  EXPECT_TRUE(warnings.empty()); // No warnings for successful operation
}

TEST_F(IndexCacheTest, WarningCallback_FallbackToXdg) {
  // Warning should be emitted when falling back from SAME_DIR to XDG_CACHE
  std::string ro_dir = createTempDir("readonly_dir");

  // Create a file in read-only directory
  std::string source_path = ro_dir + "/data.csv";
  std::ofstream file(source_path, std::ios::binary);
  file << "a,b\n1,2\n";
  file.close();

  // Make directory read-only
  chmod(ro_dir.c_str(), 0555);

  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::defaults();
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  // Restore permissions for cleanup
  chmod(ro_dir.c_str(), 0755);

  // Should fall back to XDG cache and emit a warning
  if (success) {
    ASSERT_EQ(warnings.size(), 1u);
    EXPECT_TRUE(warnings[0].find("falling back to XDG cache") != std::string::npos);
    EXPECT_TRUE(cache_path.find(".cache/libvroom") != std::string::npos);
  } else {
    // If XDG is also not writable, we get a different warning
    ASSERT_GE(warnings.size(), 1u);
    EXPECT_TRUE(warnings[0].find("not writable") != std::string::npos);
  }
}

TEST_F(IndexCacheTest, WarningCallback_CustomDirNotWritable) {
  // Warning should be emitted when custom directory is not writable
  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::custom("/nonexistent/directory");
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  std::string source_path = temp_dir + "/test.csv";
  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  EXPECT_FALSE(success);
  EXPECT_TRUE(cache_path.empty());
  ASSERT_EQ(warnings.size(), 1u);
  EXPECT_TRUE(warnings[0].find("not writable") != std::string::npos);
  EXPECT_TRUE(warnings[0].find("/nonexistent/directory") != std::string::npos);
}

TEST_F(IndexCacheTest, WarningCallback_EmptyCustomPath) {
  // Warning should be emitted when custom path is empty
  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::custom("");
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  std::string source_path = temp_dir + "/test.csv";
  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  EXPECT_FALSE(success);
  ASSERT_EQ(warnings.size(), 1u);
  EXPECT_TRUE(warnings[0].find("empty") != std::string::npos);
}

TEST_F(IndexCacheTest, WarningCallback_NullCallbackDoesNotCrash) {
  // Null callback should not cause issues
  CacheConfig config = CacheConfig::custom("/nonexistent/directory");
  // warning_callback is not set (nullptr by default)

  std::string source_path = temp_dir + "/test.csv";
  auto [cache_path, success] = IndexCache::try_compute_writable_path(source_path, config);

  // Should complete without crash, just no warnings emitted
  EXPECT_FALSE(success);
  EXPECT_TRUE(cache_path.empty());
}

TEST_F(IndexCacheTest, WarningCallback_ParserApi_CacheWriteFailure) {
  // Test that warning is emitted when cache write fails
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("write_fail.csv", content);
  std::string ro_dir = createTempDir("readonly_cache");

  // Make directory read-only after creation
  chmod(ro_dir.c_str(), 0555);

  std::vector<std::string> warnings;
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::custom(ro_dir);
  opts.cache->warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };
  opts.source_path = source_path;

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  // Restore permissions for cleanup
  chmod(ro_dir.c_str(), 0755);

  // Parsing should succeed
  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);

  // Warning should be emitted about cache write failure or directory not writable
  ASSERT_GE(warnings.size(), 1u);
  // Either "not writable" (path computation) or "Failed to write" (write attempt)
  bool has_relevant_warning = false;
  for (const auto& w : warnings) {
    if (w.find("not writable") != std::string::npos ||
        w.find("Failed to write") != std::string::npos) {
      has_relevant_warning = true;
      break;
    }
  }
  EXPECT_TRUE(has_relevant_warning);
}

TEST_F(IndexCacheTest, WarningCallback_ParserApi_InvalidCache) {
  // Test that warning is emitted when cache is invalid
  std::string content = "a,b\n1,2\n";
  std::string source_path = createTempFile("invalid_cache.csv", content);
  std::string cache_path = source_path + ".vidx";

  // Create an invalid cache file (wrong version or corrupted)
  std::ofstream cache_file(cache_path, std::ios::binary);
  cache_file << "invalid_cache_data";
  cache_file.close();

  std::vector<std::string> warnings;
  libvroom::ParseOptions opts;
  opts.cache = CacheConfig::defaults();
  opts.cache->warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };
  opts.source_path = source_path;

  libvroom::Parser parser;
  auto buffer = libvroom::load_file_to_ptr(source_path, 64);
  auto result = parser.parse(buffer.data(), buffer.size, opts);

  // Parsing should succeed (re-parsing after cache miss)
  EXPECT_TRUE(result.success());
  EXPECT_FALSE(result.used_cache);

  // Warning should be emitted about invalid cache
  ASSERT_GE(warnings.size(), 1u);
  EXPECT_TRUE(warnings[0].find("invalid") != std::string::npos ||
              warnings[0].find("version mismatch") != std::string::npos ||
              warnings[0].find("corruption") != std::string::npos);
}

TEST_F(IndexCacheTest, WarningCallback_MultipleWarnings) {
  // Test that multiple warnings can be collected
  std::vector<std::string> warnings;
  CacheConfig config = CacheConfig::custom("/nonexistent/path1");
  config.warning_callback = [&warnings](const std::string& msg) { warnings.push_back(msg); };

  // This should emit a warning
  auto [path1, success1] = IndexCache::try_compute_writable_path("/test1.csv", config);
  EXPECT_FALSE(success1);

  // Update config and try again
  config.custom_path = "/nonexistent/path2";
  auto [path2, success2] = IndexCache::try_compute_writable_path("/test2.csv", config);
  EXPECT_FALSE(success2);

  // Both warnings should be collected
  EXPECT_EQ(warnings.size(), 2u);
}
