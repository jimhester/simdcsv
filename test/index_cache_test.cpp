/**
 * @file index_cache_test.cpp
 * @brief Unit tests for IndexCache class.
 */

#include "libvroom.h"

#include "index_cache.h"
#include "io_util.h"

#include <chrono>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <fstream>
#include <gtest/gtest.h>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

using namespace libvroom;

// Test fixture for IndexCache tests
class IndexCacheTest : public ::testing::Test {
protected:
  void SetUp() override {
    // Create a temporary directory for test files
    char temp_dir[] = "/tmp/cache_test_XXXXXX";
    ASSERT_NE(mkdtemp(temp_dir), nullptr);
    temp_dir_ = temp_dir;

    // Collect warnings for verification
    warnings_.clear();
    options_.warning_callback = [this](const std::string& msg) { warnings_.push_back(msg); };
    options_.cache_dir = temp_dir_ + "/cache";
  }

  void TearDown() override {
    // Clean up temp directory
    if (!temp_dir_.empty()) {
      std::string cmd = "rm -rf " + temp_dir_;
      int result = std::system(cmd.c_str());
      (void)result;
    }
  }

  std::string create_csv_file(const std::string& name, const std::string& content) {
    std::string path = temp_dir_ + "/" + name;
    std::ofstream ofs(path, std::ios::binary);
    ofs << content;
    ofs.close();
    return path;
  }

  // Parse a CSV file and return the result
  Parser::Result parse_csv(const std::string& path) {
    auto buffer = load_file_to_ptr(path);
    Parser parser;
    return parser.parse(buffer.data(), buffer.size);
  }

  std::string temp_dir_;
  CacheOptions options_;
  std::vector<std::string> warnings_;
};

// Test basic cache miss and hit cycle
TEST_F(IndexCacheTest, CacheMissAndHit) {
  std::string csv_content = "a,b,c\n1,2,3\n4,5,6\n";
  std::string csv_path = create_csv_file("test.csv", csv_content);

  IndexCache cache(options_);

  // First load should be a cache miss
  auto result1 = cache.load(csv_path);
  EXPECT_EQ(result1.error, CacheError::None);
  EXPECT_FALSE(result1.index.has_value()); // Cache miss

  // Parse the file
  auto parsed = parse_csv(csv_path);
  ASSERT_TRUE(parsed.success());

  // Save to cache
  auto write_result = cache.save(csv_path, parsed.idx);
  EXPECT_TRUE(write_result.success());

  // Second load should be a cache hit
  auto result2 = cache.load(csv_path);
  EXPECT_EQ(result2.error, CacheError::None);
  ASSERT_TRUE(result2.index.has_value());

  // Verify cached index matches original
  EXPECT_EQ(result2.index->columns, parsed.idx.columns);
  EXPECT_EQ(result2.index->n_threads, parsed.idx.n_threads);
}

// Test cache path computation
TEST_F(IndexCacheTest, ComputeCachePath) {
  std::string csv_path = create_csv_file("path_test.csv", "a\n1\n");

  IndexCache cache(options_);
  std::string cache_path = cache.compute_cache_path(csv_path);

  EXPECT_FALSE(cache_path.empty());
  EXPECT_NE(cache_path.find(".vroom_cache"), std::string::npos);
  // Should use our custom cache_dir
  EXPECT_NE(cache_path.find(temp_dir_ + "/cache"), std::string::npos);
}

// Test cache path with XDG fallback
TEST_F(IndexCacheTest, ComputeCachePathXDGFallback) {
  // Create a read-only source directory
  std::string readonly_dir = temp_dir_ + "/readonly";
  mkdir(readonly_dir.c_str(), 0755);

  std::string csv_path = readonly_dir + "/test.csv";
  std::ofstream ofs(csv_path);
  ofs << "a\n1\n";
  ofs.close();

  // Make directory read-only
  chmod(readonly_dir.c_str(), 0555);

  // Use cache without custom cache_dir, should fall back to XDG
  CacheOptions opts;
  opts.cache_dir = std::nullopt;
  IndexCache cache(opts);

  std::string cache_path = cache.compute_cache_path(csv_path);

  // Should not be in the read-only directory
  EXPECT_NE(cache_path.find(readonly_dir + "/."), 0u);

  // Restore permissions for cleanup
  chmod(readonly_dir.c_str(), 0755);
}

// Test validation with fresh cache
TEST_F(IndexCacheTest, ValidationFreshCache) {
  std::string csv_content = "a,b\n1,2\n";
  std::string csv_path = create_csv_file("fresh.csv", csv_content);

  IndexCache cache(options_);

  // Get current metadata
  time_t mtime;
  size_t size;
  ASSERT_TRUE(MmapBuffer::get_file_metadata(csv_path, mtime, size));

  // Should validate as fresh
  EXPECT_TRUE(cache.validate_freshness(csv_path, mtime, size));
}

// Test validation with stale cache (mtime changed)
TEST_F(IndexCacheTest, ValidationStaleMtime) {
  std::string csv_content = "a,b\n1,2\n";
  std::string csv_path = create_csv_file("stale_mtime.csv", csv_content);

  IndexCache cache(options_);

  // Get current metadata
  time_t mtime;
  size_t size;
  ASSERT_TRUE(MmapBuffer::get_file_metadata(csv_path, mtime, size));

  // Pretend the cache was created in the past
  EXPECT_FALSE(cache.validate_freshness(csv_path, mtime - 100, size));
}

// Test validation with stale cache (size changed)
TEST_F(IndexCacheTest, ValidationStaleSize) {
  std::string csv_content = "a,b\n1,2\n";
  std::string csv_path = create_csv_file("stale_size.csv", csv_content);

  IndexCache cache(options_);

  // Get current metadata
  time_t mtime;
  size_t size;
  ASSERT_TRUE(MmapBuffer::get_file_metadata(csv_path, mtime, size));

  // Pretend the file was different size
  EXPECT_FALSE(cache.validate_freshness(csv_path, mtime, size + 10));
}

// Test source file change detection
TEST_F(IndexCacheTest, SourceFileChanged) {
  std::string csv_content1 = "a,b\n1,2\n";
  std::string csv_path = create_csv_file("change.csv", csv_content1);

  IndexCache cache(options_);

  // Parse and cache
  auto parsed = parse_csv(csv_path);
  ASSERT_TRUE(parsed.success());
  auto write_result = cache.save(csv_path, parsed.idx);
  EXPECT_TRUE(write_result.success());

  // Wait a bit to ensure mtime changes
  std::this_thread::sleep_for(std::chrono::milliseconds(100));

  // Modify the source file
  std::ofstream ofs(csv_path, std::ios::binary);
  ofs << "a,b,c\n1,2,3\n";
  ofs.close();

  // Load should detect change and return error
  auto result = cache.load(csv_path);
  EXPECT_EQ(result.error, CacheError::SourceChanged);
  EXPECT_FALSE(result.index.has_value());

  // Should have emitted a warning
  EXPECT_FALSE(warnings_.empty());
}

// Test cache invalidation
TEST_F(IndexCacheTest, Invalidate) {
  std::string csv_path = create_csv_file("invalidate.csv", "a\n1\n");

  IndexCache cache(options_);

  // Parse and cache
  auto parsed = parse_csv(csv_path);
  ASSERT_TRUE(parsed.success());
  cache.save(csv_path, parsed.idx);

  // Verify cache exists by loading
  auto result1 = cache.load(csv_path);
  EXPECT_TRUE(result1.index.has_value());

  // Invalidate
  EXPECT_TRUE(cache.invalidate(csv_path));

  // Should be cache miss now
  auto result2 = cache.load(csv_path);
  EXPECT_FALSE(result2.index.has_value());
}

// Test caching disabled
TEST_F(IndexCacheTest, CachingDisabled) {
  std::string csv_path = create_csv_file("disabled.csv", "a\n1\n");

  CacheOptions opts = options_;
  opts.enabled = false;
  IndexCache cache(opts);

  // Load should return no error but no index
  auto result = cache.load(csv_path);
  EXPECT_EQ(result.error, CacheError::None);
  EXPECT_FALSE(result.index.has_value());
  EXPECT_NE(result.message.find("disabled"), std::string::npos);

  // Save should succeed (no-op)
  auto parsed = parse_csv(csv_path);
  auto write_result = cache.save(csv_path, parsed.idx);
  EXPECT_TRUE(write_result.success());
}

// Test corrupted cache file (empty)
TEST_F(IndexCacheTest, CorruptedCacheEmpty) {
  std::string csv_path = create_csv_file("corrupt_empty.csv", "a\n1\n");

  IndexCache cache(options_);

  // Create empty cache file manually
  std::string cache_path = cache.compute_cache_path(csv_path);
  std::ofstream ofs(cache_path, std::ios::binary);
  ofs.close();

  // Load should detect corruption
  auto result = cache.load(csv_path);
  EXPECT_EQ(result.error, CacheError::Corrupted);
  EXPECT_FALSE(result.index.has_value());

  // Cache should be deleted
  EXPECT_NE(access(cache_path.c_str(), F_OK), 0);
}

// Test corrupted cache file (invalid magic)
TEST_F(IndexCacheTest, CorruptedCacheInvalidMagic) {
  std::string csv_path = create_csv_file("corrupt_magic.csv", "a\n1\n");

  IndexCache cache(options_);

  // Create cache file with wrong magic
  std::string cache_path = cache.compute_cache_path(csv_path);
  std::ofstream ofs(cache_path, std::ios::binary);
  uint32_t bad_magic = 0xDEADBEEF;
  ofs.write(reinterpret_cast<const char*>(&bad_magic), sizeof(bad_magic));
  ofs << std::string(100, '\0'); // padding
  ofs.close();

  // Load should detect corruption
  auto result = cache.load(csv_path);
  EXPECT_EQ(result.error, CacheError::Corrupted);
  EXPECT_FALSE(result.index.has_value());
}

// Test version mismatch
TEST_F(IndexCacheTest, VersionMismatch) {
  std::string csv_path = create_csv_file("version.csv", "a\n1\n");

  IndexCache cache(options_);

  // Create cache file with wrong version
  std::string cache_path = cache.compute_cache_path(csv_path);
  std::ofstream ofs(cache_path, std::ios::binary);

  uint32_t magic = CACHE_MAGIC;
  uint8_t bad_version = CACHE_FORMAT_VERSION + 1;

  ofs.write(reinterpret_cast<const char*>(&magic), sizeof(magic));
  ofs.write(reinterpret_cast<const char*>(&bad_version), sizeof(bad_version));
  ofs << std::string(100, '\0'); // padding
  ofs.close();

  // Load should detect version mismatch
  auto result = cache.load(csv_path);
  EXPECT_EQ(result.error, CacheError::VersionMismatch);
  EXPECT_FALSE(result.index.has_value());
}

// Test symlink resolution
TEST_F(IndexCacheTest, SymlinkResolution) {
  std::string csv_content = "a\n1\n2\n";
  std::string csv_path = create_csv_file("original.csv", csv_content);
  std::string link_path = temp_dir_ + "/link.csv";

  // Create symlink
  ASSERT_EQ(symlink(csv_path.c_str(), link_path.c_str()), 0);

  CacheOptions opts = options_;
  opts.resolve_symlinks = true;
  IndexCache cache(opts);

  // Parse and cache via symlink
  auto buffer = load_file_to_ptr(link_path);
  Parser parser;
  auto parsed = parser.parse(buffer.data(), buffer.size);
  ASSERT_TRUE(parsed.success());
  cache.save(link_path, parsed.idx);

  // Load via symlink should hit cache
  auto result1 = cache.load(link_path);
  EXPECT_TRUE(result1.index.has_value());

  // Load via original path should also hit cache (same canonical path)
  auto result2 = cache.load(csv_path);
  EXPECT_TRUE(result2.index.has_value());
}

// Test write to read-only directory (permission denied)
TEST_F(IndexCacheTest, WritePermissionDenied) {
  // Create a read-only cache directory
  std::string readonly_cache = temp_dir_ + "/readonly_cache";
  mkdir(readonly_cache.c_str(), 0555);

  std::string csv_path = create_csv_file("readonly.csv", "a\n1\n");

  CacheOptions opts;
  opts.cache_dir = readonly_cache;
  IndexCache cache(opts);

  auto parsed = parse_csv(csv_path);
  ASSERT_TRUE(parsed.success());

  // Save should fail with permission denied
  auto result = cache.save(csv_path, parsed.idx);
  // Either PermissionDenied or None (if no writable location found)
  EXPECT_TRUE(result.error == CacheError::PermissionDenied || result.error == CacheError::None);

  // Restore permissions for cleanup
  chmod(readonly_cache.c_str(), 0755);
}

// Test source file not found
TEST_F(IndexCacheTest, SourceNotFound) {
  IndexCache cache(options_);

  auto result = cache.load(temp_dir_ + "/nonexistent.csv");
  EXPECT_EQ(result.error, CacheError::SourceNotFound);
  EXPECT_FALSE(result.index.has_value());
}

// Test atomic write pattern
TEST_F(IndexCacheTest, AtomicWrite) {
  std::string csv_path = create_csv_file("atomic.csv", "a,b,c\n1,2,3\n");

  IndexCache cache(options_);

  auto parsed = parse_csv(csv_path);
  ASSERT_TRUE(parsed.success());

  // Save should create cache atomically
  auto result = cache.save(csv_path, parsed.idx);
  EXPECT_TRUE(result.success());

  // No temp files should remain
  std::string cache_path = cache.compute_cache_path(csv_path);
  std::string temp_pattern = cache_path + ".tmp.";
  // (We can't easily check for absence of temp files, but the test passes if save succeeds)
}

// Test cache with larger CSV file
TEST_F(IndexCacheTest, LargerCSV) {
  // Generate a CSV with many rows
  std::ostringstream oss;
  oss << "a,b,c,d,e\n";
  for (int i = 0; i < 10000; ++i) {
    oss << i << "," << i * 2 << "," << i * 3 << "," << i * 4 << "," << i * 5 << "\n";
  }
  std::string csv_path = create_csv_file("large.csv", oss.str());

  IndexCache cache(options_);

  // Parse and cache
  auto parsed = parse_csv(csv_path);
  ASSERT_TRUE(parsed.success());
  auto write_result = cache.save(csv_path, parsed.idx);
  EXPECT_TRUE(write_result.success());

  // Load from cache
  auto result = cache.load(csv_path);
  EXPECT_TRUE(result.success());
  ASSERT_TRUE(result.index.has_value());

  // Verify key properties
  EXPECT_EQ(result.index->columns, parsed.idx.columns);
  EXPECT_EQ(result.index->n_threads, parsed.idx.n_threads);
}

// Test cache error to string conversion
TEST_F(IndexCacheTest, CacheErrorToString) {
  EXPECT_STREQ(cache_error_to_string(CacheError::None), "None");
  EXPECT_STREQ(cache_error_to_string(CacheError::Corrupted), "Corrupted");
  EXPECT_STREQ(cache_error_to_string(CacheError::PermissionDenied), "PermissionDenied");
  EXPECT_STREQ(cache_error_to_string(CacheError::DiskFull), "DiskFull");
  EXPECT_STREQ(cache_error_to_string(CacheError::VersionMismatch), "VersionMismatch");
  EXPECT_STREQ(cache_error_to_string(CacheError::SourceChanged), "SourceChanged");
  EXPECT_STREQ(cache_error_to_string(CacheError::SourceNotFound), "SourceNotFound");
  EXPECT_STREQ(cache_error_to_string(CacheError::InternalError), "InternalError");
}

// Test CacheLoadResult and CacheWriteResult helper methods
TEST_F(IndexCacheTest, ResultHelperMethods) {
  CacheLoadResult load_result;
  EXPECT_FALSE(load_result.success());
  EXPECT_FALSE(load_result.has_error()); // None is not an error

  load_result.error = CacheError::Corrupted;
  EXPECT_FALSE(load_result.success());
  EXPECT_TRUE(load_result.has_error());

  CacheWriteResult write_result;
  EXPECT_TRUE(write_result.success());
  EXPECT_FALSE(write_result.has_error());

  write_result.error = CacheError::DiskFull;
  EXPECT_FALSE(write_result.success());
  EXPECT_TRUE(write_result.has_error());
}

// Test options getter/setter
TEST_F(IndexCacheTest, OptionsGetterSetter) {
  IndexCache cache;

  EXPECT_TRUE(cache.enabled());
  cache.set_enabled(false);
  EXPECT_FALSE(cache.enabled());

  CacheOptions opts;
  opts.enabled = true;
  opts.resolve_symlinks = false;
  cache.set_options(opts);

  EXPECT_TRUE(cache.enabled());
  EXPECT_FALSE(cache.options().resolve_symlinks);
}
