/**
 * @file mmap_util.cpp
 * @brief Implementation of MmapBuffer for memory-mapped file access.
 */

#include "mmap_util.h"

#include <cerrno>
#include <cstring>

namespace libvroom {

bool MmapBuffer::open(const std::string& path) {
  return open(path, true);
}

bool MmapBuffer::open(const std::string& path, bool read_only) {
  // Close any existing mapping
  close();
  error_.clear();

  // Open the file
  int flags = read_only ? O_RDONLY : O_RDWR;
  fd_ = ::open(path.c_str(), flags);
  if (fd_ < 0) {
    error_ = std::string("Failed to open file: ") + std::strerror(errno);
    return false;
  }

  // Get file size
  struct stat st;
  if (fstat(fd_, &st) < 0) {
    error_ = std::string("Failed to stat file: ") + std::strerror(errno);
    ::close(fd_);
    fd_ = -1;
    return false;
  }

  size_ = static_cast<size_t>(st.st_size);

  // Handle empty files
  if (size_ == 0) {
    // Empty file - valid but no mapping needed
    data_ = nullptr;
    return true;
  }

  // Memory-map the file
  int prot = read_only ? PROT_READ : (PROT_READ | PROT_WRITE);
  int mmap_flags = MAP_PRIVATE;

  void* addr = mmap(nullptr, size_, prot, mmap_flags, fd_, 0);
  if (addr == MAP_FAILED) {
    error_ = std::string("Failed to mmap file: ") + std::strerror(errno);
    ::close(fd_);
    fd_ = -1;
    size_ = 0;
    return false;
  }

  data_ = static_cast<uint8_t*>(addr);
  return true;
}

void MmapBuffer::close() {
  if (data_ != nullptr && size_ > 0) {
    munmap(data_, size_);
  }
  data_ = nullptr;
  size_ = 0;

  if (fd_ >= 0) {
    ::close(fd_);
    fd_ = -1;
  }
}

bool MmapBuffer::get_metadata(time_t& mtime, size_t& file_size) const {
  if (fd_ < 0) {
    return false;
  }

  struct stat st;
  if (fstat(fd_, &st) < 0) {
    return false;
  }

  mtime = st.st_mtime;
  file_size = static_cast<size_t>(st.st_size);
  return true;
}

bool MmapBuffer::get_file_metadata(const std::string& path, time_t& mtime, size_t& file_size) {
  struct stat st;
  if (stat(path.c_str(), &st) < 0) {
    return false;
  }

  mtime = st.st_mtime;
  file_size = static_cast<size_t>(st.st_size);
  return true;
}

} // namespace libvroom
