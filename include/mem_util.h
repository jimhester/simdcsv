/**
 * @file mem_util.h
 * @brief Portable aligned memory allocation utilities.
 *
 * This header provides cross-platform aligned memory allocation and deallocation
 * functions. These are used internally by libvroom to ensure buffers are properly
 * aligned for SIMD operations (typically 64-byte cache line alignment).
 *
 * The functions handle platform differences:
 * - POSIX: posix_memalign()
 * - MSVC: _aligned_malloc() / _aligned_free()
 * - MinGW: __mingw_aligned_malloc() / __mingw_aligned_free()
 *
 * @note All memory allocated with aligned_malloc() must be freed with
 *       aligned_free(). Do NOT use standard free() or delete.
 *
 * @see allocate_padded_buffer() in io_util.h for higher-level allocation.
 * @see FileBuffer in libvroom.h for RAII memory management.
 */

#ifndef MEM_UTIL_H
#define MEM_UTIL_H

#include <stdlib.h>

/**
 * @brief Allocate memory with specified alignment.
 *
 * Allocates a block of memory with the specified alignment. This is a
 * portable wrapper around platform-specific aligned allocation functions.
 *
 * @param alignment Required alignment in bytes (must be power of 2).
 *                  Typically 64 for cache line alignment.
 * @param size Number of bytes to allocate.
 *
 * @return Pointer to the allocated memory, or nullptr if allocation fails.
 *
 * @note The returned pointer must be freed using aligned_free(), not free().
 *
 * @example
 * @code
 * // Allocate 1KB aligned to 64-byte cache line
 * void* buffer = aligned_malloc(64, 1024);
 * if (buffer) {
 *     // Use buffer...
 *     aligned_free(buffer);
 * }
 * @endcode
 *
 * @see aligned_free() To deallocate memory from this function.
 */
static inline void *aligned_malloc(size_t alignment, size_t size) {
  void *p;
#ifdef _MSC_VER
  p = _aligned_malloc(size, alignment);
#elif defined(__MINGW32__) || defined(__MINGW64__)
  p = __mingw_aligned_malloc(size, alignment);
#else
  // somehow, if this is used before including "x86intrin.h", it creates an
  // implicit defined warning.
  if (posix_memalign(&p, alignment, size) != 0) { return nullptr; }
#endif
  return p;
}

/**
 * @brief Free memory allocated with aligned_malloc().
 *
 * Frees a block of memory that was allocated with aligned_malloc() or
 * allocate_padded_buffer(). This is a portable wrapper around platform-specific
 * aligned deallocation functions.
 *
 * @param memblock Pointer to the memory block to free. If nullptr, no action
 *                 is taken (safe to call with null pointers).
 *
 * @warning Do NOT use this function to free memory allocated with standard
 *          malloc(), new, or other non-aligned allocation functions.
 *
 * @warning Do NOT use standard free() or delete to free memory allocated
 *          with aligned_malloc() - this may cause undefined behavior on
 *          some platforms (particularly Windows).
 *
 * @example
 * @code
 * void* buffer = aligned_malloc(64, 1024);
 * // ... use buffer ...
 * aligned_free(buffer);  // Safe even if buffer is nullptr
 * @endcode
 *
 * @see aligned_malloc() To allocate memory that this function frees.
 */
static inline void aligned_free(void *memblock) {
    if(memblock == nullptr) { return; }
#ifdef _MSC_VER
    _aligned_free(memblock);
#elif defined(__MINGW32__) || defined(__MINGW64__)
    __mingw_aligned_free(memblock);
#else
    free(memblock);
#endif
}

#endif
