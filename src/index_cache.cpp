/**
 * @file index_cache.cpp
 * @brief Implementation of cache management utilities.
 */

#include "index_cache.h"

#include "mmap_util.h"

#include <cerrno>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <sys/stat.h>

#ifdef _WIN32
#include <direct.h>
#include <io.h>
#include <windows.h>
#define mkdir(dir, mode) _mkdir(dir)
#define access _access
#define W_OK 2
#define getpid _getpid
#else
#include <unistd.h>
#endif

namespace libvroom {

std::string IndexCache::compute_path(const std::string& source_path, const CacheConfig& config) {
  switch (config.location) {
  case CacheConfig::SAME_DIR:
    return source_path + CacheConfig::CACHE_EXTENSION;

  case CacheConfig::XDG_CACHE: {
    std::string cache_dir = get_xdg_cache_dir();
    if (cache_dir.empty()) {
      // Fallback to same-dir if XDG cache unavailable
      return source_path + CacheConfig::CACHE_EXTENSION;
    }
    return cache_dir + "/" + hash_path(source_path) + CacheConfig::CACHE_EXTENSION;
  }

  case CacheConfig::CUSTOM: {
    if (config.custom_path.empty()) {
      // No custom path specified, fallback to same-dir
      return source_path + CacheConfig::CACHE_EXTENSION;
    }
    // Extract filename from source path
    size_t last_sep = source_path.find_last_of("/\\");
    std::string filename =
        (last_sep == std::string::npos) ? source_path : source_path.substr(last_sep + 1);
    return config.custom_path + "/" + filename + CacheConfig::CACHE_EXTENSION;
  }

  default:
    return source_path + CacheConfig::CACHE_EXTENSION;
  }
}

std::pair<std::string, bool> IndexCache::try_compute_writable_path(const std::string& source_path,
                                                                   const CacheConfig& config) {
  std::string path = compute_path(source_path, config);

  if (config.location == CacheConfig::SAME_DIR) {
    // Check if source directory is writable
    size_t last_sep = source_path.find_last_of("/\\");
    std::string dir = (last_sep == std::string::npos) ? "." : source_path.substr(0, last_sep);

    if (!is_directory_writable(dir)) {
      // Fall back to XDG cache
      std::string xdg_dir = get_xdg_cache_dir();
      if (!xdg_dir.empty()) {
        path = xdg_dir + "/" + hash_path(source_path) + CacheConfig::CACHE_EXTENSION;
        if (is_directory_writable(xdg_dir)) {
          return {path, true};
        }
      }
      return {"", false};
    }
  } else if (config.location == CacheConfig::XDG_CACHE) {
    std::string xdg_dir = get_xdg_cache_dir();
    if (xdg_dir.empty() || !is_directory_writable(xdg_dir)) {
      return {"", false};
    }
  } else if (config.location == CacheConfig::CUSTOM) {
    if (config.custom_path.empty() || !is_directory_writable(config.custom_path)) {
      return {"", false};
    }
  }

  return {path, true};
}

bool IndexCache::is_valid(const std::string& source_path, const std::string& cache_path) {
  // Get current source metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    // Source file doesn't exist or can't be read
    return false;
  }

  // Open cache file
  std::FILE* fp = std::fopen(cache_path.c_str(), "rb");
  if (!fp) {
    return false;
  }

  bool valid = false;

  // Read and validate header - v3 format: version(1) + padding(7) + mtime(8) + size(8)
  // Total: 24 bytes to get to mtime and size
  static constexpr uint8_t INDEX_FORMAT_VERSION_V3 = 3;

  uint8_t version = 0;
  if (std::fread(&version, 1, 1, fp) != 1 || version != INDEX_FORMAT_VERSION_V3) {
    std::fclose(fp);
    return false;
  }

  // Skip 7 bytes of alignment padding
  uint8_t padding[7];
  if (std::fread(padding, 1, 7, fp) != 7) {
    std::fclose(fp);
    return false;
  }

  uint64_t cached_mtime = 0;
  uint64_t cached_size = 0;
  if (std::fread(&cached_mtime, 8, 1, fp) != 1 || std::fread(&cached_size, 8, 1, fp) != 1) {
    std::fclose(fp);
    return false;
  }

  // Compare metadata
  if (cached_mtime == source_meta.mtime && cached_size == source_meta.size) {
    valid = true;
  }

  std::fclose(fp);
  return valid;
}

bool IndexCache::write_atomic(const std::string& path, const ParseIndex& index,
                              const std::string& source_path) {
  // Get source metadata
  SourceMetadata source_meta = SourceMetadata::from_file(source_path);
  if (!source_meta.valid) {
    // Can't get source metadata
    return false;
  }

  // Delegate to ParseIndex::write which writes v3 format with atomic rename
  // Need to const_cast because write() is not const (but doesn't modify data)
  try {
    const_cast<ParseIndex&>(index).write(path, source_meta);
    return true;
  } catch (const std::exception&) {
    return false;
  }
}

std::pair<uint64_t, uint64_t> IndexCache::get_source_metadata(const std::string& source_path) {
  struct stat st;
  if (stat(source_path.c_str(), &st) != 0) {
    return {0, 0};
  }

  // Only regular files have meaningful metadata for caching
  if (!S_ISREG(st.st_mode)) {
    return {0, 0};
  }

  return {static_cast<uint64_t>(st.st_mtime), static_cast<uint64_t>(st.st_size)};
}

bool IndexCache::is_directory_writable(const std::string& dir_path) {
  if (dir_path.empty()) {
    return false;
  }

  // Check if directory exists
  struct stat st;
  if (stat(dir_path.c_str(), &st) != 0) {
    return false;
  }

  if (!S_ISDIR(st.st_mode)) {
    return false;
  }

  // Check write access
  return access(dir_path.c_str(), W_OK) == 0;
}

std::string IndexCache::get_xdg_cache_dir() {
  std::string cache_dir;

  // Try XDG_CACHE_HOME first
  const char* xdg_cache = std::getenv("XDG_CACHE_HOME");
  if (xdg_cache && xdg_cache[0] != '\0') {
    cache_dir = xdg_cache;
  } else {
    // Fall back to ~/.cache
    const char* home = std::getenv("HOME");
#ifdef _WIN32
    if (!home) {
      home = std::getenv("USERPROFILE");
    }
#endif
    if (!home || home[0] == '\0') {
      return "";
    }
    cache_dir = std::string(home) + "/.cache";
  }

  // Append libvroom subdirectory
  cache_dir += "/libvroom";

  // Create directory if it doesn't exist
  struct stat st;
  if (stat(cache_dir.c_str(), &st) != 0) {
    // Directory doesn't exist, try to create it
#ifdef _WIN32
    // On Windows, create parent directories recursively
    std::string parent = cache_dir.substr(0, cache_dir.find_last_of("/\\"));
    if (stat(parent.c_str(), &st) != 0) {
      if (mkdir(parent.c_str(), 0755) != 0 && errno != EEXIST) {
        return "";
      }
    }
#else
    // On Unix, parent directory should exist if ~/.cache exists
    std::string parent = cache_dir.substr(0, cache_dir.find_last_of('/'));
    if (stat(parent.c_str(), &st) != 0) {
      // Create ~/.cache first
      if (mkdir(parent.c_str(), 0755) != 0 && errno != EEXIST) {
        return "";
      }
    }
#endif
    if (mkdir(cache_dir.c_str(), 0755) != 0 && errno != EEXIST) {
      return "";
    }
  }

  return cache_dir;
}

std::string IndexCache::hash_path(const std::string& path) {
  // Simple FNV-1a hash for path hashing
  // This is a well-known, fast hash suitable for filenames
  constexpr uint64_t FNV_PRIME = 0x100000001b3ULL;
  constexpr uint64_t FNV_OFFSET = 0xcbf29ce484222325ULL;

  uint64_t hash = FNV_OFFSET;
  for (char c : path) {
    hash ^= static_cast<uint8_t>(c);
    hash *= FNV_PRIME;
  }

  // Convert to hexadecimal string
  char buf[17]; // 16 hex digits + null terminator
  std::snprintf(buf, sizeof(buf), "%016llx", static_cast<unsigned long long>(hash));
  return std::string(buf);
}

} // namespace libvroom
