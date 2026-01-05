#include "encoding.h"
#include "mem_util.h"
#include <cstring>
#include <algorithm>

namespace simdcsv {

const char* encoding_to_string(Encoding enc) {
    switch (enc) {
        case Encoding::UTF8:     return "UTF-8";
        case Encoding::UTF8_BOM: return "UTF-8 (BOM)";
        case Encoding::UTF16_LE: return "UTF-16LE";
        case Encoding::UTF16_BE: return "UTF-16BE";
        case Encoding::UTF32_LE: return "UTF-32LE";
        case Encoding::UTF32_BE: return "UTF-32BE";
        case Encoding::LATIN1:   return "Latin-1";
        case Encoding::UNKNOWN:  return "Unknown";
    }
    return "Unknown";
}

// BOM (Byte Order Mark) patterns
static constexpr uint8_t UTF8_BOM[] = {0xEF, 0xBB, 0xBF};
static constexpr uint8_t UTF16_LE_BOM[] = {0xFF, 0xFE};
static constexpr uint8_t UTF16_BE_BOM[] = {0xFE, 0xFF};
static constexpr uint8_t UTF32_LE_BOM[] = {0xFF, 0xFE, 0x00, 0x00};
static constexpr uint8_t UTF32_BE_BOM[] = {0x00, 0x00, 0xFE, 0xFF};

// Check if buffer starts with a specific BOM
static bool has_bom(const uint8_t* buf, size_t len,
                    const uint8_t* bom, size_t bom_len) {
    if (len < bom_len) return false;
    return std::memcmp(buf, bom, bom_len) == 0;
}

// Detect encoding via BOM
static EncodingResult detect_bom(const uint8_t* buf, size_t len) {
    EncodingResult result;
    result.confidence = 1.0;  // BOM detection is definitive

    // Check UTF-32 first (4 bytes) since UTF-32 LE BOM starts with FF FE
    // which is the same as UTF-16 LE BOM
    if (has_bom(buf, len, UTF32_LE_BOM, 4)) {
        result.encoding = Encoding::UTF32_LE;
        result.bom_length = 4;
        result.needs_transcoding = true;
        return result;
    }
    if (has_bom(buf, len, UTF32_BE_BOM, 4)) {
        result.encoding = Encoding::UTF32_BE;
        result.bom_length = 4;
        result.needs_transcoding = true;
        return result;
    }

    // Check UTF-16 (2 bytes)
    if (has_bom(buf, len, UTF16_LE_BOM, 2)) {
        result.encoding = Encoding::UTF16_LE;
        result.bom_length = 2;
        result.needs_transcoding = true;
        return result;
    }
    if (has_bom(buf, len, UTF16_BE_BOM, 2)) {
        result.encoding = Encoding::UTF16_BE;
        result.bom_length = 2;
        result.needs_transcoding = true;
        return result;
    }

    // Check UTF-8 BOM (3 bytes)
    if (has_bom(buf, len, UTF8_BOM, 3)) {
        result.encoding = Encoding::UTF8_BOM;
        result.bom_length = 3;
        result.needs_transcoding = false;  // Already UTF-8, just strip BOM
        return result;
    }

    // No BOM found
    result.encoding = Encoding::UNKNOWN;
    result.confidence = 0.0;
    return result;
}

// Heuristic detection when no BOM is present
static EncodingResult detect_heuristic(const uint8_t* buf, size_t len) {
    EncodingResult result;

    if (len == 0) {
        result.encoding = Encoding::UTF8;
        result.confidence = 1.0;
        return result;
    }

    // Sample size for heuristic detection (first 4KB or entire file)
    const size_t sample_size = std::min(len, size_t(4096));

    // Count null bytes and their patterns
    size_t null_count = 0;
    size_t even_nulls = 0;  // Nulls at even byte positions (0, 2, 4, ...)
    size_t odd_nulls = 0;   // Nulls at odd byte positions (1, 3, 5, ...)
    size_t high_bytes = 0;  // Bytes with high bit set (0x80-0xFF)

    for (size_t i = 0; i < sample_size; ++i) {
        if (buf[i] == 0) {
            ++null_count;
            if (i % 2 == 0) ++even_nulls;
            else ++odd_nulls;
        } else if (buf[i] >= 0x80) {
            ++high_bytes;
        }
    }

    // UTF-32 detection: Check for pattern of 3 nulls per 4 bytes
    if (len >= 4 && (len % 4 == 0 || len >= 16)) {
        size_t utf32_le_score = 0;
        size_t utf32_be_score = 0;
        size_t check_count = std::min(sample_size / 4, size_t(256));

        for (size_t i = 0; i < check_count; ++i) {
            size_t offset = i * 4;
            // UTF-32 LE: byte, 0, 0, 0 for ASCII
            if (buf[offset] != 0 && buf[offset + 1] == 0 &&
                buf[offset + 2] == 0 && buf[offset + 3] == 0) {
                ++utf32_le_score;
            }
            // UTF-32 BE: 0, 0, 0, byte for ASCII
            if (buf[offset] == 0 && buf[offset + 1] == 0 &&
                buf[offset + 2] == 0 && buf[offset + 3] != 0) {
                ++utf32_be_score;
            }
        }

        if (check_count > 0) {
            double le_ratio = static_cast<double>(utf32_le_score) / check_count;
            double be_ratio = static_cast<double>(utf32_be_score) / check_count;

            if (le_ratio > 0.5) {
                result.encoding = Encoding::UTF32_LE;
                result.confidence = le_ratio;
                result.needs_transcoding = true;
                return result;
            }
            if (be_ratio > 0.5) {
                result.encoding = Encoding::UTF32_BE;
                result.confidence = be_ratio;
                result.needs_transcoding = true;
                return result;
            }
        }
    }

    // UTF-16 detection: Check for alternating null bytes
    if (len >= 2 && null_count > 0) {
        double null_ratio = static_cast<double>(null_count) / sample_size;

        // UTF-16 typically has ~50% null bytes for ASCII content
        if (null_ratio > 0.2 && null_ratio < 0.7) {
            // UTF-16 LE: nulls at odd positions (ASCII in first byte)
            // UTF-16 BE: nulls at even positions (ASCII in second byte)
            if (odd_nulls > even_nulls * 3) {
                result.encoding = Encoding::UTF16_LE;
                result.confidence = 0.8;
                result.needs_transcoding = true;
                return result;
            }
            if (even_nulls > odd_nulls * 3) {
                result.encoding = Encoding::UTF16_BE;
                result.confidence = 0.8;
                result.needs_transcoding = true;
                return result;
            }
        }
    }

    // No null bytes - could be UTF-8, Latin-1, or ASCII
    if (null_count == 0) {
        // Check for valid UTF-8 sequences
        bool valid_utf8 = true;
        bool has_multibyte = false;

        for (size_t i = 0; i < sample_size && valid_utf8; ) {
            uint8_t b = buf[i];
            size_t seq_len = 1;

            if ((b & 0x80) == 0) {
                // ASCII (0x00-0x7F)
                seq_len = 1;
            } else if ((b & 0xE0) == 0xC0) {
                // 2-byte sequence (0xC0-0xDF)
                seq_len = 2;
                has_multibyte = true;
            } else if ((b & 0xF0) == 0xE0) {
                // 3-byte sequence (0xE0-0xEF)
                seq_len = 3;
                has_multibyte = true;
            } else if ((b & 0xF8) == 0xF0) {
                // 4-byte sequence (0xF0-0xF7)
                seq_len = 4;
                has_multibyte = true;
            } else if (b >= 0x80) {
                // Invalid UTF-8 start byte or Latin-1 character
                valid_utf8 = false;
                break;
            }

            // Verify continuation bytes
            if (seq_len > 1) {
                if (i + seq_len > sample_size) {
                    // Can't verify - assume ok at boundary
                    break;
                }
                for (size_t j = 1; j < seq_len; ++j) {
                    if ((buf[i + j] & 0xC0) != 0x80) {
                        valid_utf8 = false;
                        break;
                    }
                }
            }
            i += seq_len;
        }

        if (valid_utf8) {
            result.encoding = Encoding::UTF8;
            result.confidence = has_multibyte ? 0.95 : 0.9;
            result.needs_transcoding = false;
            return result;
        }

        // If high bytes present but not valid UTF-8, assume Latin-1
        if (high_bytes > 0) {
            result.encoding = Encoding::LATIN1;
            result.confidence = 0.7;
            result.needs_transcoding = false;  // Latin-1 is single-byte, parser handles
            return result;
        }
    }

    // Default to UTF-8 with lower confidence
    result.encoding = Encoding::UTF8;
    result.confidence = 0.5;
    result.needs_transcoding = false;
    return result;
}

EncodingResult detect_encoding(const uint8_t* buf, size_t len) {
    if (buf == nullptr || len == 0) {
        return {Encoding::UTF8, 0, 1.0, false};
    }

    // First try BOM detection (most reliable)
    EncodingResult result = detect_bom(buf, len);
    if (result.encoding != Encoding::UNKNOWN) {
        return result;
    }

    // Fall back to heuristic detection
    return detect_heuristic(buf, len);
}

// Helper: Read a UTF-16 code unit
static inline uint16_t read_utf16(const uint8_t* p, bool is_big_endian) {
    if (is_big_endian) {
        return (static_cast<uint16_t>(p[0]) << 8) | p[1];
    } else {
        return p[0] | (static_cast<uint16_t>(p[1]) << 8);
    }
}

// Helper: Read a UTF-32 code point
static inline uint32_t read_utf32(const uint8_t* p, bool is_big_endian) {
    if (is_big_endian) {
        return (static_cast<uint32_t>(p[0]) << 24) |
               (static_cast<uint32_t>(p[1]) << 16) |
               (static_cast<uint32_t>(p[2]) << 8) |
               p[3];
    } else {
        return p[0] |
               (static_cast<uint32_t>(p[1]) << 8) |
               (static_cast<uint32_t>(p[2]) << 16) |
               (static_cast<uint32_t>(p[3]) << 24);
    }
}

// Helper: Encode a code point as UTF-8, returns number of bytes written
static inline size_t encode_utf8(uint8_t* out, uint32_t cp) {
    if (cp < 0x80) {
        out[0] = static_cast<uint8_t>(cp);
        return 1;
    } else if (cp < 0x800) {
        out[0] = static_cast<uint8_t>(0xC0 | (cp >> 6));
        out[1] = static_cast<uint8_t>(0x80 | (cp & 0x3F));
        return 2;
    } else if (cp < 0x10000) {
        out[0] = static_cast<uint8_t>(0xE0 | (cp >> 12));
        out[1] = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F));
        out[2] = static_cast<uint8_t>(0x80 | (cp & 0x3F));
        return 3;
    } else if (cp <= 0x10FFFF) {
        out[0] = static_cast<uint8_t>(0xF0 | (cp >> 18));
        out[1] = static_cast<uint8_t>(0x80 | ((cp >> 12) & 0x3F));
        out[2] = static_cast<uint8_t>(0x80 | ((cp >> 6) & 0x3F));
        out[3] = static_cast<uint8_t>(0x80 | (cp & 0x3F));
        return 4;
    }
    // Invalid code point - use replacement character
    out[0] = 0xEF;
    out[1] = 0xBF;
    out[2] = 0xBD;  // U+FFFD
    return 3;
}

size_t utf16_to_utf8_length(const uint8_t* buf, size_t len, bool is_big_endian) {
    size_t utf8_len = 0;
    const size_t num_units = len / 2;

    for (size_t i = 0; i < num_units; ) {
        uint16_t cu = read_utf16(buf + i * 2, is_big_endian);

        // Check for surrogate pair
        if (cu >= 0xD800 && cu <= 0xDBFF && i + 1 < num_units) {
            uint16_t cu2 = read_utf16(buf + (i + 1) * 2, is_big_endian);
            if (cu2 >= 0xDC00 && cu2 <= 0xDFFF) {
                // Valid surrogate pair - will encode as 4-byte UTF-8
                utf8_len += 4;
                i += 2;
                continue;
            }
        }

        // Single code unit
        if (cu < 0x80) {
            utf8_len += 1;
        } else if (cu < 0x800) {
            utf8_len += 2;
        } else {
            utf8_len += 3;
        }
        ++i;
    }

    return utf8_len;
}

size_t utf32_to_utf8_length(const uint8_t* buf, size_t len, bool is_big_endian) {
    size_t utf8_len = 0;
    const size_t num_units = len / 4;

    for (size_t i = 0; i < num_units; ++i) {
        uint32_t cp = read_utf32(buf + i * 4, is_big_endian);

        if (cp < 0x80) {
            utf8_len += 1;
        } else if (cp < 0x800) {
            utf8_len += 2;
        } else if (cp < 0x10000) {
            utf8_len += 3;
        } else if (cp <= 0x10FFFF) {
            utf8_len += 4;
        } else {
            utf8_len += 3;  // Replacement character
        }
    }

    return utf8_len;
}

// Transcode UTF-16 to UTF-8
static TranscodeResult transcode_utf16(const uint8_t* buf, size_t len,
                                       bool is_big_endian, size_t padding) {
    TranscodeResult result;

    if (len % 2 != 0) {
        result.error = "Invalid UTF-16 data: odd number of bytes";
        return result;
    }

    // Calculate required output size
    size_t utf8_len = utf16_to_utf8_length(buf, len, is_big_endian);

    // Allocate aligned buffer
    result.data = static_cast<uint8_t*>(aligned_malloc(64, utf8_len + padding));
    if (result.data == nullptr) {
        result.error = "Failed to allocate memory for transcoding";
        return result;
    }

    // Perform transcoding
    uint8_t* out = result.data;
    const size_t num_units = len / 2;

    for (size_t i = 0; i < num_units; ) {
        uint16_t cu = read_utf16(buf + i * 2, is_big_endian);
        uint32_t cp;

        // Check for surrogate pair
        if (cu >= 0xD800 && cu <= 0xDBFF && i + 1 < num_units) {
            uint16_t cu2 = read_utf16(buf + (i + 1) * 2, is_big_endian);
            if (cu2 >= 0xDC00 && cu2 <= 0xDFFF) {
                // Valid surrogate pair
                cp = 0x10000 + ((static_cast<uint32_t>(cu - 0xD800) << 10) |
                                (cu2 - 0xDC00));
                out += encode_utf8(out, cp);
                i += 2;
                continue;
            }
        }

        // Single code unit (or unpaired surrogate)
        cp = cu;
        out += encode_utf8(out, cp);
        ++i;
    }

    result.length = out - result.data;
    result.success = true;
    return result;
}

// Transcode UTF-32 to UTF-8
static TranscodeResult transcode_utf32(const uint8_t* buf, size_t len,
                                       bool is_big_endian, size_t padding) {
    TranscodeResult result;

    if (len % 4 != 0) {
        result.error = "Invalid UTF-32 data: length not divisible by 4";
        return result;
    }

    // Calculate required output size
    size_t utf8_len = utf32_to_utf8_length(buf, len, is_big_endian);

    // Allocate aligned buffer
    result.data = static_cast<uint8_t*>(aligned_malloc(64, utf8_len + padding));
    if (result.data == nullptr) {
        result.error = "Failed to allocate memory for transcoding";
        return result;
    }

    // Perform transcoding
    uint8_t* out = result.data;
    const size_t num_units = len / 4;

    for (size_t i = 0; i < num_units; ++i) {
        uint32_t cp = read_utf32(buf + i * 4, is_big_endian);
        out += encode_utf8(out, cp);
    }

    result.length = out - result.data;
    result.success = true;
    return result;
}

// Copy UTF-8 data, stripping BOM if present
static TranscodeResult copy_utf8(const uint8_t* buf, size_t len,
                                 size_t bom_length, size_t padding) {
    TranscodeResult result;

    // Skip BOM
    const uint8_t* src = buf + bom_length;
    size_t src_len = len - bom_length;

    // Allocate aligned buffer
    result.data = static_cast<uint8_t*>(aligned_malloc(64, src_len + padding));
    if (result.data == nullptr) {
        result.error = "Failed to allocate memory";
        return result;
    }

    std::memcpy(result.data, src, src_len);
    result.length = src_len;
    result.success = true;
    return result;
}

TranscodeResult transcode_to_utf8(const uint8_t* buf, size_t len,
                                   Encoding enc, size_t bom_length,
                                   size_t padding) {
    TranscodeResult result;

    if (buf == nullptr) {
        result.error = "Null buffer";
        return result;
    }

    // Skip BOM for transcoding
    const uint8_t* src = buf + bom_length;
    size_t src_len = len - bom_length;

    switch (enc) {
        case Encoding::UTF8:
        case Encoding::UTF8_BOM:
        case Encoding::LATIN1:  // Latin-1 is single-byte, pass through
            return copy_utf8(buf, len, bom_length, padding);

        case Encoding::UTF16_LE:
            return transcode_utf16(src, src_len, false, padding);

        case Encoding::UTF16_BE:
            return transcode_utf16(src, src_len, true, padding);

        case Encoding::UTF32_LE:
            return transcode_utf32(src, src_len, false, padding);

        case Encoding::UTF32_BE:
            return transcode_utf32(src, src_len, true, padding);

        case Encoding::UNKNOWN:
        default:
            result.error = "Unknown encoding";
            return result;
    }
}

}  // namespace simdcsv
