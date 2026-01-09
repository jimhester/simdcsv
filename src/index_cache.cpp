/**
 * @file index_cache.cpp
 * @brief Implementation of IndexCache for persistent CSV index storage.
 */

#include "index_cache.h"

#include <cerrno>
#include <climits>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <filesystem>
#include <fstream>
#include <functional>
#include <sstream>
#include <sys/stat.h>
#include <unistd.h>

namespace libvroom {

namespace fs = std::filesystem;

//-----------------------------------------------------------------------------
// Helper functions
//-----------------------------------------------------------------------------

void IndexCache::warn(const std::string& message) {
  if (options_.warning_callback) {
    options_.warning_callback(message);
  }
}

std::string IndexCache::resolve_path(const std::string& path) {
  if (!options_.resolve_symlinks) {
    return path;
  }

  try {
    return fs::canonical(path).string();
  } catch (const fs::filesystem_error&) {
    // If canonical fails (e.g., file doesn't exist), return original
    return path;
  }
}

bool IndexCache::is_dir_writable(const std::string& dir) {
  return access(dir.c_str(), W_OK) == 0;
}

std::string IndexCache::get_xdg_cache_dir() {
  const char* xdg_cache = std::getenv("XDG_CACHE_HOME");
  if (xdg_cache != nullptr && xdg_cache[0] != '\0') {
    return std::string(xdg_cache) + "/libvroom";
  }

  const char* home = std::getenv("HOME");
  if (home != nullptr && home[0] != '\0') {
    return std::string(home) + "/.cache/libvroom";
  }

  return "";
}

std::string IndexCache::compute_cache_filename(const std::string& source_path) {
  // Use std::hash for a simple, fast hash of the path
  std::hash<std::string> hasher;
  size_t hash = hasher(source_path);

  // Extract just the filename from the source path
  std::string basename;
  size_t last_sep = source_path.find_last_of("/\\");
  if (last_sep != std::string::npos) {
    basename = source_path.substr(last_sep + 1);
  } else {
    basename = source_path;
  }

  // Combine basename with hash for human-readability and uniqueness
  std::ostringstream oss;
  oss << basename << "." << std::hex << hash << ".vroom_cache";
  return oss.str();
}

bool IndexCache::create_directories(const std::string& path) {
  try {
    fs::create_directories(path);
    return true;
  } catch (const fs::filesystem_error&) {
    return false;
  }
}

//-----------------------------------------------------------------------------
// Cache path computation
//-----------------------------------------------------------------------------

std::string IndexCache::compute_cache_path(const std::string& source_path) {
  if (!options_.enabled) {
    return "";
  }

  std::string resolved = resolve_path(source_path);
  std::string cache_filename = compute_cache_filename(resolved);

  // Priority 1: Custom cache directory
  if (options_.cache_dir.has_value() && !options_.cache_dir->empty()) {
    std::string dir = *options_.cache_dir;
    if (create_directories(dir) && is_dir_writable(dir)) {
      return dir + "/" + cache_filename;
    }
    // Fall through if custom dir fails
  }

  // Priority 2: Same directory as source file
  std::string source_dir;
  size_t last_sep = resolved.find_last_of("/\\");
  if (last_sep != std::string::npos) {
    source_dir = resolved.substr(0, last_sep);
  } else {
    source_dir = ".";
  }

  if (is_dir_writable(source_dir)) {
    return source_dir + "/." + cache_filename;
  }

  // Priority 3: XDG cache directory
  std::string xdg_dir = get_xdg_cache_dir();
  if (!xdg_dir.empty()) {
    if (create_directories(xdg_dir) && is_dir_writable(xdg_dir)) {
      return xdg_dir + "/" + cache_filename;
    }
  }

  // No writable cache location found
  return "";
}

//-----------------------------------------------------------------------------
// Cache validation
//-----------------------------------------------------------------------------

bool IndexCache::validate_freshness(const std::string& source_path, time_t cached_mtime,
                                    size_t cached_size) {
  time_t current_mtime;
  size_t current_size;

  if (!MmapBuffer::get_file_metadata(source_path, current_mtime, current_size)) {
    return false;
  }

  return current_mtime == cached_mtime && current_size == cached_size;
}

//-----------------------------------------------------------------------------
// Cache file header
//-----------------------------------------------------------------------------

// Cache file header structure (written in binary):
// - uint32_t magic (CACHE_MAGIC)
// - uint8_t  version (CACHE_FORMAT_VERSION)
// - uint64_t source_mtime
// - uint64_t source_size
// - uint64_t columns
// - uint16_t n_threads
// - uint64_t n_indexes[n_threads]
// - uint64_t indexes[total]

constexpr size_t CACHE_HEADER_BASE_SIZE = sizeof(uint32_t) + // magic
                                          sizeof(uint8_t) +  // version
                                          sizeof(uint64_t) + // source_mtime
                                          sizeof(uint64_t) + // source_size
                                          sizeof(uint64_t) + // columns
                                          sizeof(uint16_t);  // n_threads

CacheError IndexCache::read_header(const MmapBuffer& buffer, uint8_t& version, time_t& mtime,
                                   size_t& size, size_t& header_size) {
  if (buffer.size() < CACHE_HEADER_BASE_SIZE) {
    return CacheError::Corrupted;
  }

  const uint8_t* data = buffer.data();
  size_t offset = 0;

  // Read and verify magic
  uint32_t magic;
  std::memcpy(&magic, data + offset, sizeof(magic));
  offset += sizeof(magic);

  if (magic != CACHE_MAGIC) {
    return CacheError::Corrupted;
  }

  // Read version
  version = data[offset++];
  if (version != CACHE_FORMAT_VERSION) {
    return CacheError::VersionMismatch;
  }

  // Read source metadata
  uint64_t mtime64;
  std::memcpy(&mtime64, data + offset, sizeof(mtime64));
  offset += sizeof(mtime64);
  mtime = static_cast<time_t>(mtime64);

  uint64_t size64;
  std::memcpy(&size64, data + offset, sizeof(size64));
  offset += sizeof(size64);
  size = static_cast<size_t>(size64);

  // Skip columns and n_threads for now (they're validated during load)
  offset += sizeof(uint64_t); // columns
  uint16_t n_threads;
  std::memcpy(&n_threads, data + offset, sizeof(n_threads));
  offset += sizeof(n_threads);

  // Header size includes n_indexes array
  header_size = offset + n_threads * sizeof(uint64_t);

  if (buffer.size() < header_size) {
    return CacheError::Corrupted;
  }

  return CacheError::None;
}

//-----------------------------------------------------------------------------
// Cache loading
//-----------------------------------------------------------------------------

CacheLoadResult IndexCache::load(const std::string& source_path) {
  CacheLoadResult result;

  if (!options_.enabled) {
    result.error = CacheError::None;
    result.message = "Caching disabled";
    return result;
  }

  // Resolve symlinks if configured
  std::string resolved = resolve_path(source_path);

  // Check source file exists
  time_t source_mtime;
  size_t source_size;
  if (!MmapBuffer::get_file_metadata(resolved, source_mtime, source_size)) {
    result.error = CacheError::SourceNotFound;
    result.message = "Source file not found: " + source_path;
    return result;
  }

  // Compute cache path
  std::string cache_path = compute_cache_path(resolved);
  if (cache_path.empty()) {
    result.error = CacheError::None;
    result.message = "No writable cache location";
    return result;
  }

  // Try to open cache file
  MmapBuffer buffer;
  if (!buffer.open(cache_path)) {
    // Cache doesn't exist or can't be opened - not an error, just cache miss
    result.error = CacheError::None;
    result.message = "Cache miss: " + buffer.error();
    return result;
  }

  // Empty cache file is invalid
  if (buffer.size() == 0) {
    warn("Corrupted cache file (empty): " + cache_path);
    invalidate(source_path);
    result.error = CacheError::Corrupted;
    result.message = "Cache file is empty";
    return result;
  }

  // Read and validate header
  uint8_t version;
  time_t cached_mtime;
  size_t cached_size;
  size_t header_size;
  CacheError header_error = read_header(buffer, version, cached_mtime, cached_size, header_size);

  if (header_error == CacheError::VersionMismatch) {
    warn("Cache version mismatch, invalidating: " + cache_path);
    invalidate(source_path);
    result.error = CacheError::VersionMismatch;
    result.message = "Cache version mismatch";
    return result;
  }

  if (header_error == CacheError::Corrupted) {
    warn("Corrupted cache file, deleting: " + cache_path);
    invalidate(source_path);
    result.error = CacheError::Corrupted;
    result.message = "Cache file corrupted";
    return result;
  }

  // Validate freshness
  if (!validate_freshness(resolved, cached_mtime, cached_size)) {
    warn("Source file changed, invalidating cache: " + cache_path);
    invalidate(source_path);
    result.error = CacheError::SourceChanged;
    result.message = "Source file changed since cache was created";
    return result;
  }

  // Parse index data from cache
  const uint8_t* data = buffer.data();
  size_t offset =
      sizeof(uint32_t) + sizeof(uint8_t) + sizeof(uint64_t) * 2; // magic, version, mtime, size

  // Read columns
  uint64_t columns;
  std::memcpy(&columns, data + offset, sizeof(columns));
  offset += sizeof(columns);

  // Read n_threads
  uint16_t n_threads;
  std::memcpy(&n_threads, data + offset, sizeof(n_threads));
  offset += sizeof(n_threads);

  // Validate n_threads
  if (n_threads == 0) {
    warn("Corrupted cache file (n_threads=0): " + cache_path);
    invalidate(source_path);
    result.error = CacheError::Corrupted;
    result.message = "Invalid n_threads in cache";
    return result;
  }

  // Calculate expected size
  size_t n_indexes_size = n_threads * sizeof(uint64_t);
  if (buffer.size() < offset + n_indexes_size) {
    warn("Corrupted cache file (truncated n_indexes): " + cache_path);
    invalidate(source_path);
    result.error = CacheError::Corrupted;
    result.message = "Cache file truncated (n_indexes)";
    return result;
  }

  // Read n_indexes array
  std::vector<uint64_t> n_indexes(n_threads);
  std::memcpy(n_indexes.data(), data + offset, n_indexes_size);
  offset += n_indexes_size;

  // Calculate total index count
  uint64_t total_indexes = 0;
  for (uint16_t i = 0; i < n_threads; ++i) {
    total_indexes += n_indexes[i];
  }

  // Validate total size
  size_t indexes_size = total_indexes * sizeof(uint64_t);
  if (buffer.size() < offset + indexes_size) {
    warn("Corrupted cache file (truncated indexes): " + cache_path);
    invalidate(source_path);
    result.error = CacheError::Corrupted;
    result.message = "Cache file truncated (indexes)";
    return result;
  }

  // Create ParseIndex and copy data
  TwoPass parser;
  ParseIndex idx = parser.init_counted(total_indexes, n_threads);
  idx.columns = columns;

  // Copy n_indexes
  std::memcpy(idx.n_indexes, n_indexes.data(), n_indexes_size);

  // Copy indexes
  std::memcpy(idx.indexes, data + offset, indexes_size);

  result.index = std::move(idx);
  result.error = CacheError::None;
  result.message = "Cache hit";
  return result;
}

//-----------------------------------------------------------------------------
// Cache writing
//-----------------------------------------------------------------------------

CacheWriteResult IndexCache::write_atomic(const std::string& cache_path,
                                          const std::string& source_path, const ParseIndex& index) {
  CacheWriteResult result;

  // Get source file metadata
  time_t source_mtime;
  size_t source_size;
  if (!MmapBuffer::get_file_metadata(source_path, source_mtime, source_size)) {
    result.error = CacheError::SourceNotFound;
    result.message = "Cannot stat source file";
    return result;
  }

  // Create temp file for atomic write
  std::string temp_path = cache_path + ".tmp." + std::to_string(getpid());

  std::FILE* fp = std::fopen(temp_path.c_str(), "wb");
  if (!fp) {
    if (errno == EACCES || errno == EROFS) {
      result.error = CacheError::PermissionDenied;
      result.message = "Permission denied writing cache";
    } else if (errno == ENOSPC || errno == EDQUOT) {
      result.error = CacheError::DiskFull;
      result.message = "Disk full writing cache";
    } else {
      result.error = CacheError::InternalError;
      result.message = std::string("Failed to create temp file: ") + std::strerror(errno);
    }
    return result;
  }

  // Write magic
  uint32_t magic = CACHE_MAGIC;
  if (std::fwrite(&magic, sizeof(magic), 1, fp) != 1) {
    std::fclose(fp);
    std::remove(temp_path.c_str());
    result.error = CacheError::InternalError;
    result.message = "Failed to write magic";
    return result;
  }

  // Write version
  uint8_t version = CACHE_FORMAT_VERSION;
  if (std::fwrite(&version, sizeof(version), 1, fp) != 1) {
    std::fclose(fp);
    std::remove(temp_path.c_str());
    result.error = CacheError::InternalError;
    result.message = "Failed to write version";
    return result;
  }

  // Write source metadata
  uint64_t mtime64 = static_cast<uint64_t>(source_mtime);
  uint64_t size64 = static_cast<uint64_t>(source_size);
  if (std::fwrite(&mtime64, sizeof(mtime64), 1, fp) != 1 ||
      std::fwrite(&size64, sizeof(size64), 1, fp) != 1) {
    std::fclose(fp);
    std::remove(temp_path.c_str());
    result.error = CacheError::InternalError;
    result.message = "Failed to write source metadata";
    return result;
  }

  // Write index header
  if (std::fwrite(&index.columns, sizeof(index.columns), 1, fp) != 1 ||
      std::fwrite(&index.n_threads, sizeof(index.n_threads), 1, fp) != 1) {
    std::fclose(fp);
    std::remove(temp_path.c_str());
    result.error = CacheError::InternalError;
    result.message = "Failed to write index header";
    return result;
  }

  // Write n_indexes array
  if (std::fwrite(index.n_indexes, sizeof(uint64_t), index.n_threads, fp) != index.n_threads) {
    std::fclose(fp);
    std::remove(temp_path.c_str());
    if (errno == ENOSPC || errno == EDQUOT) {
      result.error = CacheError::DiskFull;
      result.message = "Disk full writing cache";
    } else {
      result.error = CacheError::InternalError;
      result.message = "Failed to write n_indexes";
    }
    return result;
  }

  // Calculate total indexes
  uint64_t total_indexes = 0;
  for (uint16_t i = 0; i < index.n_threads; ++i) {
    total_indexes += index.n_indexes[i];
  }

  // Write indexes
  if (total_indexes > 0 &&
      std::fwrite(index.indexes, sizeof(uint64_t), total_indexes, fp) != total_indexes) {
    std::fclose(fp);
    std::remove(temp_path.c_str());
    if (errno == ENOSPC || errno == EDQUOT) {
      result.error = CacheError::DiskFull;
      result.message = "Disk full writing cache";
    } else {
      result.error = CacheError::InternalError;
      result.message = "Failed to write indexes";
    }
    return result;
  }

  // Close file before rename
  if (std::fclose(fp) != 0) {
    std::remove(temp_path.c_str());
    if (errno == ENOSPC || errno == EDQUOT) {
      result.error = CacheError::DiskFull;
      result.message = "Disk full flushing cache";
    } else {
      result.error = CacheError::InternalError;
      result.message = "Failed to close temp file";
    }
    return result;
  }

  // Atomic rename
  if (std::rename(temp_path.c_str(), cache_path.c_str()) != 0) {
    std::remove(temp_path.c_str());
    if (errno == EACCES || errno == EROFS) {
      result.error = CacheError::PermissionDenied;
      result.message = "Permission denied renaming cache";
    } else {
      result.error = CacheError::InternalError;
      result.message = std::string("Failed to rename temp file: ") + std::strerror(errno);
    }
    return result;
  }

  result.error = CacheError::None;
  result.message = "Cache written successfully";
  return result;
}

CacheWriteResult IndexCache::save(const std::string& source_path, const ParseIndex& index) {
  CacheWriteResult result;

  if (!options_.enabled) {
    result.error = CacheError::None;
    result.message = "Caching disabled";
    return result;
  }

  // Resolve symlinks if configured
  std::string resolved = resolve_path(source_path);

  // Validate source still exists and hasn't changed
  time_t current_mtime;
  size_t current_size;
  if (!MmapBuffer::get_file_metadata(resolved, current_mtime, current_size)) {
    result.error = CacheError::SourceNotFound;
    result.message = "Source file not found";
    return result;
  }

  // Compute cache path
  std::string cache_path = compute_cache_path(resolved);
  if (cache_path.empty()) {
    result.error = CacheError::PermissionDenied;
    result.message = "No writable cache location";
    return result;
  }

  // Write atomically
  result = write_atomic(cache_path, resolved, index);

  if (result.error == CacheError::DiskFull) {
    warn("Disk full, continuing without cache: " + result.message);
  } else if (result.error == CacheError::PermissionDenied) {
    // Silent - this is expected for read-only directories
  } else if (result.has_error()) {
    warn("Cache write failed: " + result.message);
  }

  return result;
}

//-----------------------------------------------------------------------------
// Cache invalidation
//-----------------------------------------------------------------------------

bool IndexCache::invalidate(const std::string& source_path) {
  std::string resolved = resolve_path(source_path);
  std::string cache_path = compute_cache_path(resolved);

  if (cache_path.empty()) {
    return true; // No cache location means nothing to invalidate
  }

  if (std::remove(cache_path.c_str()) != 0 && errno != ENOENT) {
    return false;
  }

  return true;
}

} // namespace libvroom
