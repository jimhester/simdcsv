/**
 * @file mmap_util.h
 * @brief Memory-mapped file utilities for index caching.
 *
 * This header provides the MmapBuffer class for memory-mapped file access,
 * which enables efficient read-only access to cached index files without
 * loading them entirely into memory.
 */

#ifndef LIBVROOM_MMAP_UTIL_H
#define LIBVROOM_MMAP_UTIL_H

#include <cstddef>
#include <cstdint>
#include <fcntl.h>
#include <string>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

namespace libvroom {

/**
 * @brief RAII wrapper for memory-mapped files.
 *
 * MmapBuffer provides safe, RAII-managed access to memory-mapped files.
 * It is designed primarily for read-only access to cached index files,
 * but can also be used for read-write mappings.
 *
 * The buffer is automatically unmapped when the object is destroyed.
 *
 * @note This class is move-only. Copy operations are deleted to prevent
 *       double-unmap issues.
 *
 * @example
 * @code
 * // Open a file for read-only access
 * MmapBuffer buffer;
 * if (buffer.open("cache.idx")) {
 *     const uint8_t* data = buffer.data();
 *     size_t size = buffer.size();
 *     // ... process data ...
 * }
 * // Automatically unmapped when buffer goes out of scope
 * @endcode
 */
class MmapBuffer {
public:
  /// Default constructor. Creates an invalid (unmapped) buffer.
  MmapBuffer() = default;

  /**
   * @brief Move constructor.
   * @param other The MmapBuffer to move from.
   */
  MmapBuffer(MmapBuffer&& other) noexcept : data_(other.data_), size_(other.size_), fd_(other.fd_) {
    other.data_ = nullptr;
    other.size_ = 0;
    other.fd_ = -1;
  }

  /**
   * @brief Move assignment operator.
   * @param other The MmapBuffer to move from.
   * @return Reference to this buffer.
   */
  MmapBuffer& operator=(MmapBuffer&& other) noexcept {
    if (this != &other) {
      close();
      data_ = other.data_;
      size_ = other.size_;
      fd_ = other.fd_;
      other.data_ = nullptr;
      other.size_ = 0;
      other.fd_ = -1;
    }
    return *this;
  }

  // Delete copy operations to prevent double-unmap
  MmapBuffer(const MmapBuffer&) = delete;
  MmapBuffer& operator=(const MmapBuffer&) = delete;

  /// Destructor. Unmaps the file if mapped.
  ~MmapBuffer() { close(); }

  /**
   * @brief Open and memory-map a file for reading.
   *
   * @param path Path to the file to map.
   * @return true if the file was successfully mapped, false otherwise.
   *
   * @note If the buffer is already mapped to another file, it will be
   *       closed first.
   */
  bool open(const std::string& path);

  /**
   * @brief Open and memory-map a file with specified access mode.
   *
   * @param path Path to the file to map.
   * @param read_only If true, map as read-only; otherwise, read-write.
   * @return true if the file was successfully mapped, false otherwise.
   */
  bool open(const std::string& path, bool read_only);

  /**
   * @brief Close the memory mapping and file descriptor.
   *
   * After calling this method, the buffer will be invalid (valid() == false).
   * This method is safe to call multiple times.
   */
  void close();

  /// @return Const pointer to the mapped data, or nullptr if not mapped.
  const uint8_t* data() const { return data_; }

  /// @return Mutable pointer to the mapped data, or nullptr if not mapped.
  uint8_t* data() { return data_; }

  /// @return Size of the mapped data in bytes.
  size_t size() const { return size_; }

  /// @return true if the buffer is valid (file is open).
  /// Note: For empty files, data() will return nullptr but valid() is still true.
  bool valid() const { return fd_ >= 0; }

  /// @return true if the buffer is valid, enabling `if (buffer)` syntax.
  explicit operator bool() const { return valid(); }

  /**
   * @brief Get the last error message.
   *
   * @return Description of the last error, or empty string if no error.
   */
  const std::string& error() const { return error_; }

  /**
   * @brief Get file metadata for validation.
   *
   * @param mtime Output parameter for modification time.
   * @param file_size Output parameter for file size.
   * @return true if metadata was retrieved successfully.
   */
  bool get_metadata(time_t& mtime, size_t& file_size) const;

  /**
   * @brief Get file metadata for a path.
   *
   * @param path Path to the file.
   * @param mtime Output parameter for modification time.
   * @param file_size Output parameter for file size.
   * @return true if metadata was retrieved successfully.
   */
  static bool get_file_metadata(const std::string& path, time_t& mtime, size_t& file_size);

private:
  uint8_t* data_{nullptr};
  size_t size_{0};
  int fd_{-1};
  std::string error_;
};

} // namespace libvroom

#endif // LIBVROOM_MMAP_UTIL_H
