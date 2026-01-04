/**
 * @file simdcsv.h
 * @brief simdcsv - High-performance CSV parser using portable SIMD instructions.
 * @version 0.1.0
 *
 * This is the main public header for the simdcsv library. Include this single
 * header to access all public functionality.
 */

#ifndef SIMDCSV_H
#define SIMDCSV_H

#define SIMDCSV_VERSION_MAJOR 0
#define SIMDCSV_VERSION_MINOR 1
#define SIMDCSV_VERSION_PATCH 0
#define SIMDCSV_VERSION_STRING "0.1.0"

#include "error.h"
#include "dialect.h"
#include "two_pass.h"
#include "io_util.h"
#include "mem_util.h"

namespace simdcsv {

class FileBuffer {
public:
    FileBuffer() : data_(nullptr), size_(0) {}
    FileBuffer(uint8_t* data, size_t size) : data_(data), size_(size) {}
    FileBuffer(FileBuffer&& other) noexcept : data_(other.data_), size_(other.size_) {
        other.data_ = nullptr;
        other.size_ = 0;
    }
    FileBuffer& operator=(FileBuffer&& other) noexcept {
        if (this != &other) {
            free();
            data_ = other.data_;
            size_ = other.size_;
            other.data_ = nullptr;
            other.size_ = 0;
        }
        return *this;
    }
    FileBuffer(const FileBuffer&) = delete;
    FileBuffer& operator=(const FileBuffer&) = delete;
    ~FileBuffer() { free(); }

    const uint8_t* data() const { return data_; }
    uint8_t* data() { return data_; }
    size_t size() const { return size_; }
    bool valid() const { return data_ != nullptr; }
    bool empty() const { return size_ == 0; }
    explicit operator bool() const { return valid(); }

    /**
     * Release ownership of the buffer and return the raw pointer.
     * After calling this method, the FileBuffer no longer owns the memory
     * and the caller is responsible for freeing it using aligned_free().
     * @return The raw pointer to the buffer data, or nullptr if the buffer was empty/invalid.
     */
    uint8_t* release() {
        uint8_t* ptr = data_;
        data_ = nullptr;
        size_ = 0;
        return ptr;
    }

private:
    void free() {
        if (data_) {
            aligned_free(data_);
            data_ = nullptr;
            size_ = 0;
        }
    }
    uint8_t* data_;
    size_t size_;
};

/**
 * @brief Loads a file into a FileBuffer with SIMD-aligned memory.
 *
 * This function loads the entire file contents into a newly allocated buffer
 * that is cache-line aligned with padding for safe SIMD operations. The
 * FileBuffer takes ownership of the allocated memory and will free it when
 * destroyed.
 *
 * @param filename Path to the file to load.
 * @param padding Extra bytes to allocate for SIMD overreads (default: 64).
 * @return FileBuffer containing the file data. Check valid() for success.
 * @throws std::runtime_error if file cannot be opened or read.
 *
 * @note Memory ownership is transferred to FileBuffer - do not manually free.
 */
inline FileBuffer load_file(const std::string& filename, size_t padding = 64) {
    auto corpus = get_corpus(filename, padding);
    // Note: get_corpus() allocates memory via allocate_padded_buffer() which
    // must be freed with aligned_free(). FileBuffer takes ownership and will
    // call aligned_free() in its destructor.
    return FileBuffer(const_cast<uint8_t*>(corpus.data()), corpus.size());
}

inline void free_buffer(std::basic_string_view<uint8_t>& corpus) {
    if (corpus.data()) {
        aligned_free(const_cast<uint8_t*>(corpus.data()));
    }
}

class Parser {
public:
    struct Result {
        index idx;
        bool successful{false};
        Dialect dialect;
        DetectionResult detection;

        Result() = default;
        Result(Result&&) = default;
        Result& operator=(Result&&) = default;
        // Prevent copying - index may contain raw pointers
        Result(const Result&) = delete;
        Result& operator=(const Result&) = delete;

        bool success() const { return successful; }
        size_t num_columns() const { return idx.columns; }
        size_t total_indexes() const {
            if (!idx.n_indexes) return 0;
            size_t total = 0;
            for (uint8_t t = 0; t < idx.n_threads; ++t) {
                total += idx.n_indexes[t];
            }
            return total;
        }
    };

    explicit Parser(size_t num_threads = 1)
        : num_threads_(num_threads > 0 ? num_threads : 1) {}

    Result parse(const uint8_t* buf, size_t len,
                 const Dialect& dialect = Dialect::csv()) {
        Result result;
        result.dialect = dialect;
        result.idx = parser_.init(len, num_threads_);
        result.successful = parser_.parse(buf, result.idx, len, dialect);
        return result;
    }

    Result parse_with_errors(const uint8_t* buf, size_t len,
                             ErrorCollector& errors,
                             const Dialect& dialect = Dialect::csv()) {
        Result result;
        result.dialect = dialect;
        result.idx = parser_.init(len, num_threads_);
        result.successful = parser_.parse_with_errors(buf, result.idx, len, errors, dialect);
        return result;
    }

    Result parse_auto(const uint8_t* buf, size_t len, ErrorCollector& errors) {
        Result result;
        result.idx = parser_.init(len, num_threads_);
        result.successful = parser_.parse_auto(buf, result.idx, len, errors, &result.detection);
        result.dialect = result.detection.dialect;
        return result;
    }

    void set_num_threads(size_t num_threads) {
        num_threads_ = num_threads > 0 ? num_threads : 1;
    }
    size_t num_threads() const { return num_threads_; }

private:
    two_pass parser_;
    size_t num_threads_;
};

inline DetectionResult detect_dialect(const uint8_t* buf, size_t len,
                                       const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect(buf, len);
}

inline DetectionResult detect_dialect_file(const std::string& filename,
                                            const DetectionOptions& options = DetectionOptions()) {
    DialectDetector detector(options);
    return detector.detect_file(filename);
}

} // namespace simdcsv

#endif // SIMDCSV_H
