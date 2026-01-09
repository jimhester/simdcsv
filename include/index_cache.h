/**
 * @file index_cache.h
 * @brief Index caching system for persistent CSV index storage.
 *
 * This header provides the IndexCache class for caching parsed CSV indexes
 * to disk, enabling fast re-reads of the same files without re-parsing.
 *
 * ## Overview
 *
 * The index caching system stores parsed CSV field positions on disk so that
 * subsequent reads of the same file can skip the expensive parsing step.
 * The cache is automatically invalidated when the source file changes.
 *
 * ## Cache Location
 *
 * Cache files are stored based on the following priority:
 * 1. Same directory as source file (if writable)
 * 2. XDG_CACHE_HOME/libvroom/ (if set)
 * 3. ~/.cache/libvroom/ (fallback)
 *
 * ## Error Handling
 *
 * All edge cases are handled gracefully:
 * - Corrupted cache files are deleted and re-parsed
 * - Permission denied errors fall back to parsing without caching
 * - Disk full conditions log a warning but don't fail the operation
 * - Version mismatches trigger cache invalidation
 * - Source file changes are detected via mtime+size validation
 */

#ifndef LIBVROOM_INDEX_CACHE_H
#define LIBVROOM_INDEX_CACHE_H

#include "mmap_util.h"
#include "two_pass.h"

#include <cstdint>
#include <functional>
#include <optional>
#include <string>

namespace libvroom {

// Forward declarations
class ParseIndex;

/**
 * @brief Error codes for cache operations.
 *
 * These error codes indicate the type of failure encountered during
 * cache read/write operations. All errors are recoverable by falling
 * back to parsing the source file directly.
 */
enum class CacheError {
  /// No error occurred.
  None,

  /// Cache file is corrupted (invalid format, truncated, checksum mismatch).
  Corrupted,

  /// Permission denied when reading or writing the cache file.
  PermissionDenied,

  /// Disk is full, cannot write cache file.
  DiskFull,

  /// Cache file was created by a different version of libvroom.
  VersionMismatch,

  /// Source file changed since cache was created.
  SourceChanged,

  /// Source file not found or cannot be accessed.
  SourceNotFound,

  /// Internal error (should not happen).
  InternalError
};

/**
 * @brief Convert CacheError to human-readable string.
 *
 * @param error The error code to convert.
 * @return String representation of the error.
 */
inline const char* cache_error_to_string(CacheError error) {
  switch (error) {
  case CacheError::None:
    return "None";
  case CacheError::Corrupted:
    return "Corrupted";
  case CacheError::PermissionDenied:
    return "PermissionDenied";
  case CacheError::DiskFull:
    return "DiskFull";
  case CacheError::VersionMismatch:
    return "VersionMismatch";
  case CacheError::SourceChanged:
    return "SourceChanged";
  case CacheError::SourceNotFound:
    return "SourceNotFound";
  case CacheError::InternalError:
    return "InternalError";
  }
  return "Unknown"; // LCOV_EXCL_LINE
}

/**
 * @brief Result of attempting to load a cached index.
 *
 * CacheLoadResult encapsulates the outcome of a cache load operation,
 * including the loaded index (if successful), any error that occurred,
 * and a descriptive message for logging/debugging.
 */
struct CacheLoadResult {
  /// The loaded ParseIndex, if successful. Empty optional on failure.
  std::optional<ParseIndex> index;

  /// Error code indicating what went wrong, or CacheError::None on success.
  CacheError error = CacheError::None;

  /// Human-readable description of the error (for logging).
  std::string message;

  /// @return true if the load was successful.
  bool success() const { return error == CacheError::None && index.has_value(); }

  /// @return true if there was any error.
  bool has_error() const { return error != CacheError::None; }
};

/**
 * @brief Result of attempting to write a cached index.
 *
 * CacheWriteResult encapsulates the outcome of a cache write operation.
 */
struct CacheWriteResult {
  /// Error code indicating what went wrong, or CacheError::None on success.
  CacheError error = CacheError::None;

  /// Human-readable description of the error (for logging).
  std::string message;

  /// @return true if the write was successful.
  bool success() const { return error == CacheError::None; }

  /// @return true if there was any error.
  bool has_error() const { return error != CacheError::None; }
};

/**
 * @brief Cache file format version.
 *
 * Increment this when the cache file format changes in an incompatible way.
 * Cache files with a different version will be invalidated.
 */
constexpr uint8_t CACHE_FORMAT_VERSION = 1;

/**
 * @brief Magic bytes at the start of cache files for identification.
 */
constexpr uint32_t CACHE_MAGIC = 0x56524D43; // "VRMC" (Vroom Cache)

/**
 * @brief Callback type for logging warnings during cache operations.
 *
 * The callback receives a warning message string that can be logged
 * or displayed to the user.
 */
using CacheWarningCallback = std::function<void(const std::string&)>;

/**
 * @brief Options for configuring IndexCache behavior.
 */
struct CacheOptions {
  /// Enable or disable caching entirely.
  bool enabled = true;

  /// Follow symlinks when computing cache paths.
  bool resolve_symlinks = true;

  /// Custom cache directory (overrides XDG and same-dir heuristics).
  std::optional<std::string> cache_dir;

  /// Callback for warning messages (e.g., "cache corrupted, reparsing").
  CacheWarningCallback warning_callback;
};

/**
 * @brief Index caching manager for persistent CSV index storage.
 *
 * IndexCache handles all aspects of caching parsed CSV indexes to disk:
 * - Computing cache file paths from source file paths
 * - Validating cache freshness against source file metadata
 * - Reading cached indexes with error handling
 * - Writing new cache files with atomic rename pattern
 * - Falling back gracefully on any errors
 *
 * ## Thread Safety
 *
 * IndexCache is NOT thread-safe. Each thread should have its own instance,
 * or external synchronization must be used.
 *
 * ## Usage
 *
 * @code
 * IndexCache cache;
 *
 * // Try to load from cache
 * auto result = cache.load("data.csv");
 * if (result.success()) {
 *     // Use cached index
 *     ParseIndex& idx = *result.index;
 * } else {
 *     // Parse the file normally
 *     Parser parser;
 *     auto parsed = parser.parse(buf, len);
 *
 *     // Save to cache for next time
 *     cache.save("data.csv", parsed.idx);
 * }
 * @endcode
 */
class IndexCache {
public:
  /**
   * @brief Construct an IndexCache with default options.
   */
  IndexCache() = default;

  /**
   * @brief Construct an IndexCache with custom options.
   * @param options Configuration options for the cache.
   */
  explicit IndexCache(const CacheOptions& options) : options_(options) {}

  /**
   * @brief Try to load a cached index for a source file.
   *
   * This method attempts to load a cached index for the given source file.
   * It performs the following validations:
   * 1. Check if cache file exists
   * 2. Verify cache file format and version
   * 3. Compare source file mtime and size with cached values
   * 4. Load and validate the index data
   *
   * @param source_path Path to the source CSV file.
   * @return CacheLoadResult containing the index or error information.
   */
  CacheLoadResult load(const std::string& source_path);

  /**
   * @brief Save a parsed index to the cache.
   *
   * This method writes the parsed index to a cache file using an atomic
   * rename pattern to prevent partial writes from being read.
   *
   * @param source_path Path to the source CSV file.
   * @param index The parsed index to cache.
   * @return CacheWriteResult indicating success or failure.
   */
  CacheWriteResult save(const std::string& source_path, const ParseIndex& index);

  /**
   * @brief Validate that a cached index is still fresh.
   *
   * This method checks if the source file has changed since the cache
   * was created by comparing mtime and file size.
   *
   * @param source_path Path to the source CSV file.
   * @param cached_mtime Modification time when cache was created.
   * @param cached_size File size when cache was created.
   * @return true if the cache is still valid, false if source changed.
   */
  bool validate_freshness(const std::string& source_path, time_t cached_mtime, size_t cached_size);

  /**
   * @brief Invalidate (delete) the cache for a source file.
   *
   * @param source_path Path to the source CSV file.
   * @return true if the cache was deleted or didn't exist, false on error.
   */
  bool invalidate(const std::string& source_path);

  /**
   * @brief Compute the cache file path for a source file.
   *
   * The cache path is determined by:
   * 1. Custom cache_dir if set in options
   * 2. Same directory as source file (if writable)
   * 3. XDG_CACHE_HOME/libvroom/ (if set)
   * 4. ~/.cache/libvroom/ (fallback)
   *
   * @param source_path Path to the source CSV file.
   * @return Computed cache file path, or empty string on error.
   */
  std::string compute_cache_path(const std::string& source_path);

  /**
   * @brief Get the current cache options.
   * @return Reference to the current options.
   */
  const CacheOptions& options() const { return options_; }

  /**
   * @brief Set new cache options.
   * @param options New options to use.
   */
  void set_options(const CacheOptions& options) { options_ = options; }

  /**
   * @brief Check if caching is enabled.
   * @return true if caching is enabled.
   */
  bool enabled() const { return options_.enabled; }

  /**
   * @brief Enable or disable caching.
   * @param enable Whether to enable caching.
   */
  void set_enabled(bool enable) { options_.enabled = enable; }

private:
  CacheOptions options_;

  /// Emit a warning message through the configured callback.
  void warn(const std::string& message);

  /// Resolve symlinks to canonical path.
  std::string resolve_path(const std::string& path);

  /// Check if a directory is writable.
  bool is_dir_writable(const std::string& dir);

  /// Get the XDG cache directory.
  std::string get_xdg_cache_dir();

  /// Compute a hash-based filename for cache files.
  std::string compute_cache_filename(const std::string& source_path);

  /// Create a directory and all parent directories.
  bool create_directories(const std::string& path);

  /// Write cache file atomically using rename pattern.
  CacheWriteResult write_atomic(const std::string& cache_path, const std::string& source_path,
                                const ParseIndex& index);

  /// Read and validate cache file header.
  CacheError read_header(const MmapBuffer& buffer, uint8_t& version, time_t& mtime, size_t& size,
                         size_t& header_size);
};

} // namespace libvroom

#endif // LIBVROOM_INDEX_CACHE_H
