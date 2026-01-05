#include "io_util.h"
#include "mem_util.h"
#include "encoding.h"
#include <cstring>
#include <cstdlib>
#include <cstdio>
#include <vector>

uint8_t * allocate_padded_buffer(size_t length, size_t padding) {
    // Check for integer overflow before addition
    if (length > SIZE_MAX - padding) {
        return nullptr;
    }
    // we could do a simple malloc
    //return (char *) malloc(length + padding);
    // However, we might as well align to cache lines...
    size_t totalpaddedlength = length + padding;
    uint8_t * padded_buffer = (uint8_t *) aligned_malloc(64, totalpaddedlength);
    return padded_buffer;
}

std::basic_string_view<uint8_t> get_corpus_stdin(size_t padding) {
    // Read stdin in chunks since we don't know the size upfront
    const size_t chunk_size = 64 * 1024;  // 64KB chunks
    std::vector<uint8_t> data;
    data.reserve(chunk_size * 16);  // Reserve ~1MB upfront to reduce reallocations

    uint8_t buffer[chunk_size];
    while (true) {
        size_t bytes_read = std::fread(buffer, 1, chunk_size, stdin);
        if (bytes_read > 0) {
            data.insert(data.end(), buffer, buffer + bytes_read);
        }
        if (bytes_read < chunk_size) {
            if (std::ferror(stdin)) {
                throw std::runtime_error("could not read from stdin");
            }
            break;  // EOF reached
        }
    }

    if (data.empty()) {
        throw std::runtime_error("no data read from stdin");
    }

    // Allocate properly aligned buffer with padding
    uint8_t* buf = allocate_padded_buffer(data.size(), padding);
    if (buf == nullptr) {
        throw std::runtime_error("could not allocate memory");
    }

    // Copy data to aligned buffer
    std::memcpy(buf, data.data(), data.size());

    return std::basic_string_view<uint8_t>(buf, data.size());
}

std::basic_string_view<uint8_t> get_corpus(const std::string& filename, size_t padding) {
  std::FILE *fp = std::fopen(filename.c_str(), "rb");
  if (fp != nullptr) {
    std::fseek(fp, 0, SEEK_END);
    size_t len = std::ftell(fp);
    uint8_t * buf = allocate_padded_buffer(len, padding);
    if(buf == nullptr) {
      std::fclose(fp);
      throw  std::runtime_error("could not allocate memory");
    }
    std::rewind(fp);
    size_t readb = std::fread(buf, 1, len, fp);
    std::fclose(fp);
    if(readb != len) {
      aligned_free(buf);
      throw  std::runtime_error("could not read the data");
    }
    return std::basic_string_view<uint8_t>(buf,len);
  }
  throw  std::runtime_error("could not load corpus");
}

// Helper to transcode data if needed and return result
static LoadResult process_with_encoding(uint8_t* raw_buf, size_t raw_len, size_t padding) {
    LoadResult result;

    // Detect encoding
    result.encoding = simdcsv::detect_encoding(raw_buf, raw_len);

    // If transcoding is needed, transcode and free original buffer
    if (result.encoding.needs_transcoding) {
        auto transcoded = simdcsv::transcode_to_utf8(
            raw_buf, raw_len,
            result.encoding.encoding,
            result.encoding.bom_length,
            padding
        );

        // Free the original buffer
        aligned_free(raw_buf);

        if (!transcoded.success) {
            throw std::runtime_error("Transcoding failed: " + transcoded.error);
        }

        result.data = std::basic_string_view<uint8_t>(transcoded.data, transcoded.length);
    } else if (result.encoding.bom_length > 0) {
        // UTF-8 with BOM - need to strip the BOM
        auto stripped = simdcsv::transcode_to_utf8(
            raw_buf, raw_len,
            result.encoding.encoding,
            result.encoding.bom_length,
            padding
        );

        // Free the original buffer
        aligned_free(raw_buf);

        if (!stripped.success) {
            throw std::runtime_error("Failed to strip BOM: " + stripped.error);
        }

        result.data = std::basic_string_view<uint8_t>(stripped.data, stripped.length);
    } else {
        // Already UTF-8 without BOM - use as-is
        result.data = std::basic_string_view<uint8_t>(raw_buf, raw_len);
    }

    return result;
}

LoadResult get_corpus_with_encoding(const std::string& filename, size_t padding) {
    std::FILE *fp = std::fopen(filename.c_str(), "rb");
    if (fp == nullptr) {
        throw std::runtime_error("could not load corpus");
    }

    std::fseek(fp, 0, SEEK_END);
    size_t len = std::ftell(fp);
    uint8_t* buf = allocate_padded_buffer(len, padding);
    if (buf == nullptr) {
        std::fclose(fp);
        throw std::runtime_error("could not allocate memory");
    }

    std::rewind(fp);
    size_t readb = std::fread(buf, 1, len, fp);
    std::fclose(fp);

    if (readb != len) {
        aligned_free(buf);
        throw std::runtime_error("could not read the data");
    }

    return process_with_encoding(buf, len, padding);
}

LoadResult get_corpus_stdin_with_encoding(size_t padding) {
    // Read stdin in chunks since we don't know the size upfront
    const size_t chunk_size = 64 * 1024;  // 64KB chunks
    std::vector<uint8_t> data;
    data.reserve(chunk_size * 16);  // Reserve ~1MB upfront

    uint8_t buffer[chunk_size];
    while (true) {
        size_t bytes_read = std::fread(buffer, 1, chunk_size, stdin);
        if (bytes_read > 0) {
            data.insert(data.end(), buffer, buffer + bytes_read);
        }
        if (bytes_read < chunk_size) {
            if (std::ferror(stdin)) {
                throw std::runtime_error("could not read from stdin");
            }
            break;  // EOF reached
        }
    }

    if (data.empty()) {
        throw std::runtime_error("no data read from stdin");
    }

    // Allocate properly aligned buffer
    uint8_t* buf = allocate_padded_buffer(data.size(), padding);
    if (buf == nullptr) {
        throw std::runtime_error("could not allocate memory");
    }

    // Copy data to aligned buffer
    std::memcpy(buf, data.data(), data.size());

    return process_with_encoding(buf, data.size(), padding);
}
