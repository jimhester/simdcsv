/**
 * @file io_util.h
 * @brief File I/O utilities for loading CSV files with SIMD-aligned buffers.
 *
 * This header provides utilities for loading files into memory with proper
 * alignment and padding for efficient SIMD processing. The functions ensure
 * that buffers are cache-line aligned (64 bytes) and include sufficient
 * padding to allow SIMD operations to safely read beyond the actual data
 * length without bounds checking.
 *
 * @note All allocated buffers must be freed using aligned_free() from mem_util.h,
 *       not standard free() or delete.
 *
 * @see mem_util.h for aligned memory allocation and deallocation functions.
 */

#ifndef LIBVROOM_JSONIOUTIL_H
#define LIBVROOM_JSONIOUTIL_H

#include "common_defs.h"
#include "encoding.h"
#include <cstdint>
#include <exception>
#include <fstream>
#include <iostream>
#include <sstream>
#include <string>
#include <string_view>


/**
 * @brief Allocates a memory buffer with padding for safe SIMD operations.
 *
 * Allocates a cache-line aligned (64-byte) buffer of size (length + padding).
 * The padding allows SIMD operations to safely read past the end of the
 * actual data without triggering memory access violations.
 *
 * @param length The number of bytes of actual data that will be stored.
 * @param padding The number of additional bytes to allocate beyond length
 *                for safe SIMD overreads. Typically 32-64 bytes for AVX/AVX2.
 *
 * @return Pointer to the allocated buffer, or nullptr if allocation fails.
 *         The buffer is aligned to 64-byte cache line boundaries.
 *
 * @note The caller is responsible for freeing the returned memory using
 *       aligned_free() from mem_util.h. Do NOT use standard free() or delete.
 *
 * @note The padding bytes are not initialized and may contain garbage values.
 *       SIMD operations should not interpret padding bytes as valid data.
 *
 * @example
 * @code
 * #include "io_util.h"
 * #include "mem_util.h"
 *
 * // Allocate buffer for 1000 bytes with 64 bytes padding for SIMD
 * size_t data_size = 1000;
 * size_t simd_padding = 64;
 * uint8_t* buffer = allocate_padded_buffer(data_size, simd_padding);
 *
 * if (buffer != nullptr) {
 *     // Copy data into buffer
 *     memcpy(buffer, my_data, data_size);
 *
 *     // SIMD operations can safely read up to (data_size + simd_padding) bytes
 *     // ... process buffer with SIMD ...
 *
 *     // Free when done
 *     aligned_free(buffer);
 * }
 * @endcode
 *
 * @see aligned_free() in mem_util.h for proper deallocation.
 */
uint8_t * allocate_padded_buffer(size_t length, size_t padding);


/**
 * @brief Reads all data from stdin into a SIMD-aligned, padded memory buffer.
 *
 * Reads the complete contents of standard input into a newly allocated buffer
 * that is cache-line aligned (64 bytes) with additional padding bytes. This
 * enables efficient SIMD processing of piped data without bounds checking at
 * the end of the buffer.
 *
 * Since stdin has unknown size, this function reads data in chunks and
 * dynamically grows the buffer as needed. The final buffer is reallocated
 * to be properly aligned with the required padding.
 *
 * @param padding The number of extra bytes to allocate beyond the data size
 *                for safe SIMD overreads. Use at least 32 bytes for SSE/NEON,
 *                or 64 bytes for AVX/AVX2 operations.
 *
 * @return A std::basic_string_view<uint8_t> containing the stdin data.
 *         The view's data() points to the allocated buffer, and size()
 *         returns the actual data size (not including padding).
 *
 * @throws std::runtime_error If memory allocation fails ("could not allocate memory").
 * @throws std::runtime_error If reading from stdin fails ("could not read from stdin").
 *
 * @note The caller is responsible for freeing the underlying buffer using
 *       aligned_free((void*)result.data()). Do NOT use free() or delete.
 *
 * @note The padding bytes beyond the content are uninitialized and may
 *       contain arbitrary values. Do not interpret them as valid data.
 *
 * @example
 * @code
 * #include "io_util.h"
 * #include "mem_util.h"
 *
 * // Read piped CSV data: cat data.csv | ./scsv -
 * auto corpus = get_corpus_stdin(64);
 * // ... process corpus ...
 * aligned_free((void*)corpus.data());
 * @endcode
 *
 * @see get_corpus() for loading from a file path.
 * @see aligned_free() in mem_util.h for proper deallocation.
 */
std::basic_string_view<uint8_t> get_corpus_stdin(size_t padding);


/**
 * @brief Loads an entire file into a SIMD-aligned, padded memory buffer.
 *
 * Reads the complete contents of a file into a newly allocated buffer that
 * is cache-line aligned (64 bytes) with additional padding bytes. This
 * enables efficient SIMD processing of the file contents without bounds
 * checking at the end of the buffer.
 *
 * The function performs the following steps:
 * 1. Opens the file in binary mode
 * 2. Determines the file size
 * 3. Allocates an aligned buffer of (file_size + padding) bytes
 * 4. Reads the entire file contents
 * 5. Returns a string_view pointing to the buffer
 *
 * @param filename The path to the file to load.
 * @param padding The number of extra bytes to allocate beyond the file size
 *                for safe SIMD overreads. Use at least 32 bytes for SSE/NEON,
 *                or 64 bytes for AVX/AVX2 operations.
 *
 * @return A std::basic_string_view<uint8_t> containing the file data.
 *         The view's data() points to the allocated buffer, and size()
 *         returns the actual file size (not including padding).
 *
 * @throws std::runtime_error If the file cannot be opened ("could not load corpus").
 * @throws std::runtime_error If memory allocation fails ("could not allocate memory").
 * @throws std::runtime_error If the file cannot be fully read ("could not read the data").
 *
 * @note The caller is responsible for freeing the underlying buffer using
 *       aligned_free((void*)result.data()). Do NOT use free() or delete.
 *
 * @note The padding bytes beyond the file content are uninitialized and may
 *       contain arbitrary values. Do not interpret them as valid file data.
 *
 * @note The returned string_view is NOT null-terminated by default. If you
 *       need null-termination, ensure padding >= 1 and manually set the
 *       byte after the content to '\0'.
 *
 * @example
 * @code
 * #include "io_util.h"
 * #include "mem_util.h"
 * #include <iostream>
 *
 * // Load a CSV file with 64-byte padding for AVX2 SIMD operations
 * std::basic_string_view<uint8_t> corpus;
 * try {
 *     corpus = get_corpus("data.csv", 64);
 *
 *     std::cout << "Loaded " << corpus.size() << " bytes\n";
 *
 *     // Process the CSV data with SIMD
 *     // The buffer has 64 extra bytes that can be safely read
 *     // by SIMD operations without bounds checking
 *
 *     // ... parse CSV using SIMD ...
 *
 *     // Always free the buffer when done
 *     aligned_free((void*)corpus.data());
 *
 * } catch (const std::runtime_error& e) {
 *     std::cerr << "Failed to load file: " << e.what() << "\n";
 *     // Note: If exception is thrown, no memory was allocated
 *     // (or it was already freed), so no cleanup needed
 * }
 * @endcode
 *
 * @example
 * @code
 * // Using RAII wrapper for automatic cleanup
 * #include "io_util.h"
 * #include "mem_util.h"
 * #include <memory>
 *
 * // Custom deleter for aligned memory
 * struct AlignedDeleter {
 *     void operator()(uint8_t* ptr) const {
 *         aligned_free(ptr);
 *     }
 * };
 *
 * void process_file(const std::string& filename) {
 *     auto corpus = get_corpus(filename, 64);
 *
 *     // Wrap in unique_ptr for automatic cleanup
 *     std::unique_ptr<uint8_t, AlignedDeleter> guard(
 *         const_cast<uint8_t*>(corpus.data())
 *     );
 *
 *     // Process corpus...
 *     // Memory automatically freed when guard goes out of scope
 * }
 * @endcode
 *
 * @see allocate_padded_buffer() for the underlying allocation mechanism.
 * @see aligned_free() in mem_util.h for proper deallocation.
 */
std::basic_string_view<uint8_t>  get_corpus(const std::string& filename, size_t padding);


/**
 * @brief Result of loading a file with encoding detection.
 *
 * Contains both the (possibly transcoded) data and information about
 * the detected encoding. If the file was transcoded (e.g., from UTF-16),
 * the data will be in UTF-8 format.
 */
struct LoadResult {
    std::basic_string_view<uint8_t> data;  ///< The loaded/transcoded data
    libvroom::EncodingResult encoding;       ///< Detected encoding information

    /// Returns true if loading was successful
    operator bool() const { return data.data() != nullptr; }
};


/**
 * @brief Loads a file with automatic encoding detection and transcoding.
 *
 * This function detects the encoding of a file (via BOM or heuristics),
 * and automatically transcodes UTF-16 and UTF-32 files to UTF-8. The
 * returned data is always UTF-8 (or ASCII-compatible) for parsing.
 *
 * Encoding detection order:
 * 1. BOM detection (UTF-8, UTF-16 LE/BE, UTF-32 LE/BE)
 * 2. Heuristic analysis (null byte patterns, UTF-8 validation)
 *
 * @param filename The path to the file to load.
 * @param padding The number of extra bytes to allocate beyond the data size
 *                for safe SIMD overreads.
 *
 * @return A LoadResult containing the data and encoding information.
 *
 * @throws std::runtime_error If the file cannot be opened, read, or transcoded.
 *
 * @note The caller is responsible for freeing the underlying buffer using
 *       aligned_free((void*)result.data.data()). Do NOT use free() or delete.
 *
 * @example
 * @code
 * auto result = get_corpus_with_encoding("data.csv", 64);
 * std::cout << "Encoding: " << encoding_to_string(result.encoding.encoding) << "\n";
 * // ... parse result.data ...
 * aligned_free((void*)result.data.data());
 * @endcode
 */
LoadResult get_corpus_with_encoding(const std::string& filename, size_t padding);


/**
 * @brief Reads stdin with automatic encoding detection and transcoding.
 *
 * Similar to get_corpus_with_encoding(), but reads from stdin instead of
 * a file. The data is fully buffered, then encoding is detected and
 * transcoding is performed if necessary.
 *
 * @param padding The number of extra bytes to allocate beyond the data size
 *                for safe SIMD overreads.
 *
 * @return A LoadResult containing the data and encoding information.
 *
 * @throws std::runtime_error If reading fails or transcoding fails.
 *
 * @note The caller is responsible for freeing the underlying buffer using
 *       aligned_free((void*)result.data.data()). Do NOT use free() or delete.
 */
LoadResult get_corpus_stdin_with_encoding(size_t padding);

#endif
