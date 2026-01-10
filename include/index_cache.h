/**
 * @file index_cache.h
 * @brief Cache management utilities for index caching.
 *
 * This header provides utilities for computing cache paths, validating cache
 * freshness, and handling atomic writes for persistent index caching. Index
 * caching allows parsed CSV field indexes to be stored on disk and reloaded
 * on subsequent runs, avoiding the cost of re-parsing large files.
 *
 * ## Cache Path Resolution Strategy
 *
 * The cache system supports three location modes:
 * 1. **SAME_DIR** (default): Cache file adjacent to source (e.g., data.csv.vidx)
 * 2. **XDG_CACHE**: Uses ~/.cache/libvroom/<hash>.vidx for read-only source dirs
 * 3. **CUSTOM**: User-specified directory
 *
 * When using SAME_DIR, if the source directory is not writable, the system
 * automatically falls back to XDG_CACHE to avoid permission errors.
 *
 * ## Cache Validation
 *
 * Cache validity is determined by comparing the source file's modification
 * time and size with the values stored in the cache header. If either has
 * changed, the cache is considered stale and must be regenerated.
 *
 * ## Atomic Writes
 *
 * Cache files are written atomically using a temp file + rename pattern.
 * This ensures that readers never see partially-written cache files.
 *
 * @see two_pass.h for ParseIndex structure
 */

#ifndef LIBVROOM_INDEX_CACHE_H
#define LIBVROOM_INDEX_CACHE_H

#include "two_pass.h"

#include <cstdint>
#include <string>
#include <utility>

namespace libvroom {

/// Index cache format version (v1 includes source file metadata for validation)
constexpr uint8_t INDEX_CACHE_VERSION = 1;

/**
 * @brief Configuration for cache location resolution.
 *
 * CacheConfig controls where cache files are stored. The default SAME_DIR
 * mode places cache files adjacent to source files for maximum locality
 * and simplicity. XDG_CACHE mode uses the standard XDG cache directory
 * (~/.cache/libvroom/) which is useful when source directories are read-only.
 */
struct CacheConfig {
  /**
   * @brief Cache location mode.
   */
  enum Location {
    /**
     * @brief Store cache adjacent to source file (e.g., data.csv.vidx).
     *
     * This is the default and preferred mode. Falls back to XDG_CACHE
     * if the source directory is not writable.
     */
    SAME_DIR,

    /**
     * @brief Store cache in XDG cache directory (~/.cache/libvroom/).
     *
     * Uses a hash of the source file's absolute path to generate a unique
     * filename, avoiding collisions between files with the same name in
     * different directories.
     */
    XDG_CACHE,

    /**
     * @brief Store cache in a custom user-specified directory.
     *
     * When this mode is selected, custom_path must be set to a valid
     * directory path.
     */
    CUSTOM
  };

  /// The cache location mode to use.
  Location location = SAME_DIR;

  /// Custom directory path (only used when location == CUSTOM).
  std::string custom_path;

  /// Extension used for cache files.
  static constexpr const char* CACHE_EXTENSION = ".vidx";

  /**
   * @brief Create default configuration (SAME_DIR mode).
   */
  static CacheConfig defaults() { return CacheConfig{}; }

  /**
   * @brief Create configuration for XDG cache directory.
   */
  static CacheConfig xdg_cache() {
    CacheConfig config;
    config.location = XDG_CACHE;
    return config;
  }

  /**
   * @brief Create configuration for a custom directory.
   * @param path Path to the custom cache directory.
   */
  static CacheConfig custom(const std::string& path) {
    CacheConfig config;
    config.location = CUSTOM;
    config.custom_path = path;
    return config;
  }
};

/**
 * @brief Cache management utilities for persistent index storage.
 *
 * IndexCache provides static methods for computing cache paths, validating
 * cache freshness, and performing atomic cache writes. This class is
 * stateless - all methods are static and operate on the paths provided.
 *
 * @example Basic usage
 * @code
 * #include "index_cache.h"
 *
 * // Compute cache path for a source file
 * std::string cache_path = IndexCache::compute_path("data.csv",
 *                                                    CacheConfig::defaults());
 *
 * // Check if existing cache is valid
 * if (IndexCache::is_valid("data.csv", cache_path)) {
 *     // Load from cache (see Phase 4 for loading API)
 * } else {
 *     // Parse file and write cache
 *     auto result = parser.parse(buf, len);
 *     if (IndexCache::write_atomic(cache_path, result.idx, "data.csv")) {
 *         std::cout << "Cache written successfully\n";
 *     }
 * }
 * @endcode
 */
class IndexCache {
public:
  /**
   * @brief Compute cache path for a source file.
   *
   * Resolves the cache path based on the source file path and configuration.
   * For SAME_DIR mode, this simply appends ".vidx" to the source path.
   * For XDG_CACHE mode, this generates a hash-based filename in ~/.cache/libvroom/.
   * For CUSTOM mode, this places the cache file in the configured directory.
   *
   * @param source_path Path to the source CSV file.
   * @param config Cache configuration specifying location mode.
   * @return The computed cache file path.
   *
   * @note For SAME_DIR mode with unwritable source directories, consider
   *       using try_compute_writable_path() instead for automatic fallback.
   */
  static std::string compute_path(const std::string& source_path, const CacheConfig& config);

  /**
   * @brief Compute a writable cache path with automatic fallback.
   *
   * Similar to compute_path(), but for SAME_DIR mode, if the source directory
   * is not writable, automatically falls back to XDG_CACHE mode.
   *
   * @param source_path Path to the source CSV file.
   * @param config Cache configuration specifying location mode.
   * @return A pair of (cache_path, success). If success is false, no writable
   *         location could be found.
   */
  static std::pair<std::string, bool> try_compute_writable_path(const std::string& source_path,
                                                                const CacheConfig& config);

  /**
   * @brief Check if a cache file is valid for the given source file.
   *
   * Reads the cache file header and compares the stored mtime and size
   * with the current source file metadata. The cache is valid only if:
   * 1. The cache file exists and is readable
   * 2. The cache file has a valid header with matching version
   * 3. The stored mtime matches the source file's mtime
   * 4. The stored size matches the source file's size
   *
   * @param source_path Path to the source CSV file.
   * @param cache_path Path to the cache file.
   * @return true if the cache is valid, false otherwise.
   *
   * @note This method does not fully validate the cache contents beyond
   *       the header. A corrupted cache body may still return true.
   */
  static bool is_valid(const std::string& source_path, const std::string& cache_path);

  /**
   * @brief Write a ParseIndex to a cache file atomically.
   *
   * Writes the index to a temporary file, then atomically renames it to
   * the target path. This ensures readers never see partially-written files.
   * The cache header includes the source file's mtime and size for validation.
   *
   * Cache file format:
   * - [version: 1 byte] Cache format version (INDEX_CACHE_VERSION)
   * - [mtime: 8 bytes] Source file modification time (seconds since epoch)
   * - [size: 8 bytes] Source file size in bytes
   * - [columns: 8 bytes] Number of columns in the CSV
   * - [n_threads: 2 bytes] Number of threads used for parsing
   * - [n_indexes: 8 * n_threads bytes] Array of index counts per thread
   * - [indexes: 8 * total_indexes bytes] Array of field separator positions
   *
   * @param path Path to write the cache file.
   * @param index The ParseIndex to serialize.
   * @param source_path Path to the source file (for metadata extraction).
   * @return true if the cache was written successfully, false otherwise.
   *
   * @warning If this returns false, no cache file was created or modified.
   *          Any temporary file is cleaned up automatically.
   */
  static bool write_atomic(const std::string& path, const ParseIndex& index,
                           const std::string& source_path);

  /**
   * @brief Get source file metadata (modification time and size).
   *
   * Retrieves the modification time and size of a file for cache validation.
   *
   * @param source_path Path to the source file.
   * @return A pair of (mtime, size). If the file cannot be stat'd,
   *         returns (0, 0).
   *
   * @note The mtime is in seconds since the Unix epoch (time_t).
   */
  static std::pair<uint64_t, uint64_t> get_source_metadata(const std::string& source_path);

  /**
   * @brief Check if a directory is writable.
   *
   * Attempts to determine if files can be created in the given directory.
   *
   * @param dir_path Path to the directory to check.
   * @return true if the directory exists and is writable, false otherwise.
   */
  static bool is_directory_writable(const std::string& dir_path);

  /**
   * @brief Get the XDG cache directory for libvroom.
   *
   * Returns the path to ~/.cache/libvroom/, creating it if necessary.
   *
   * @return Path to the cache directory, or empty string if it cannot be
   *         created.
   */
  static std::string get_xdg_cache_dir();

  /**
   * @brief Hash a file path to generate a unique cache filename.
   *
   * Uses a simple hash algorithm to convert a file path to a fixed-length
   * hexadecimal string suitable for use as a cache filename.
   *
   * @param path The file path to hash.
   * @return A hexadecimal hash string.
   */
  static std::string hash_path(const std::string& path);

  /**
   * @brief Cache file header size in bytes.
   *
   * Layout: [version:1][mtime:8][size:8][columns:8][n_threads:2] = 27 bytes
   */
  static constexpr size_t HEADER_SIZE = 1 + 8 + 8 + 8 + 2;

  /**
   * @brief Result of a cache load operation.
   *
   * Contains the loaded index (if successful) and status information about
   * why a load might have failed.
   */
  struct LoadResult {
    ParseIndex index;          ///< The loaded index (check is_valid() for success)
    bool was_corrupted{false}; ///< True if cache file was corrupted and deleted
    bool file_deleted{false};  ///< True if corrupted cache file was deleted
    std::string error_message; ///< Description of any error encountered

    /// @return true if the index was loaded successfully
    bool success() const { return index.is_valid(); }
  };

  /**
   * @brief Load a cached index with automatic corruption handling.
   *
   * Attempts to load a cached ParseIndex from disk. If the cache file is
   * corrupted (invalid version, truncated, or fails validation), it is
   * automatically deleted to allow re-caching on the next parse.
   *
   * Corruption conditions that trigger automatic cleanup:
   * - File is too small to contain a valid header
   * - Version byte is not the expected v3 format
   * - File is truncated (indexes extend beyond file boundary)
   * - Stored metadata doesn't match source file (stale, not corrupt)
   *
   * @param cache_path Path to the cache file.
   * @param source_path Path to the source CSV file (for metadata validation).
   * @return LoadResult containing the index and status information.
   *
   * @example
   * @code
   * auto result = IndexCache::load("data.csv.vidx", "data.csv");
   * if (result.success()) {
   *     // Use result.index
   * } else if (result.was_corrupted) {
   *     std::cerr << "Cache was corrupted and deleted: " << result.error_message << "\n";
   *     // Re-parse the file and write new cache
   * } else {
   *     // Cache miss (file doesn't exist or source changed)
   * }
   * @endcode
   *
   * @see is_valid() for validation-only checks without loading
   * @see write_atomic() for writing cache files
   */
  static LoadResult load(const std::string& cache_path, const std::string& source_path);
};

} // namespace libvroom

#endif // LIBVROOM_INDEX_CACHE_H
