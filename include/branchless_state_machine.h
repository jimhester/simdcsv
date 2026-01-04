/**
 * @file branchless_state_machine.h
 * @brief Branchless CSV state machine implementation for high-performance parsing.
 *
 * This header provides a branchless implementation of the CSV state machine
 * that eliminates branch mispredictions in the performance-critical parsing paths.
 * The implementation uses:
 *
 * 1. **Lookup Table State Machine**: Pre-computed 5×4 lookup table mapping
 *    current state and character classification to next state.
 *
 * 2. **SIMD Character Classification**: Bitmask operations to classify all
 *    characters in a 64-byte block simultaneously.
 *
 * 3. **Bit Manipulation for State Tracking**: simdjson-inspired approach
 *    encoding state information in bitmasks rather than sequential processing.
 *
 * The goal is to eliminate 90%+ of branches in performance-critical paths and
 * achieve significant IPC (instructions per cycle) improvement.
 *
 * @see two_pass.h For the main parser that uses this state machine
 * @see https://github.com/jimhester/simdcsv/issues/41 For design discussion
 */

#ifndef SIMDCSV_BRANCHLESS_STATE_MACHINE_H
#define SIMDCSV_BRANCHLESS_STATE_MACHINE_H

#include <cassert>
#include <cstdint>
#include <cstddef>
#include "common_defs.h"
#include "simd_highway.h"

namespace simdcsv {

/**
 * @brief Character classification for branchless CSV parsing.
 *
 * Characters are classified into 4 categories that determine state transitions:
 * - DELIMITER (0): Field separator (typically comma)
 * - QUOTE (1): Quote character (typically double-quote)
 * - NEWLINE (2): Line terminator (\n)
 * - OTHER (3): All other characters
 */
enum CharClass : uint8_t {
    CHAR_DELIMITER = 0,
    CHAR_QUOTE = 1,
    CHAR_NEWLINE = 2,
    CHAR_OTHER = 3
};

/**
 * @brief CSV parser state for branchless state machine.
 *
 * Uses numeric values 0-4 for direct indexing into lookup tables.
 */
enum BranchlessState : uint8_t {
    STATE_RECORD_START = 0,    // At the beginning of a new record (row)
    STATE_FIELD_START = 1,     // At the beginning of a new field (after comma)
    STATE_UNQUOTED_FIELD = 2,  // Inside an unquoted field
    STATE_QUOTED_FIELD = 3,    // Inside a quoted field
    STATE_QUOTED_END = 4       // Just saw a quote inside a quoted field
};

/**
 * @brief Error codes for branchless state transitions.
 */
enum BranchlessError : uint8_t {
    ERR_NONE = 0,
    ERR_QUOTE_IN_UNQUOTED = 1,
    ERR_INVALID_AFTER_QUOTE = 2
};

/**
 * @brief Combined state and error result packed into a single byte.
 *
 * Layout: [error (2 bits)][state (3 bits)][is_separator (1 bit)][reserved (2 bits)]
 * This packing allows for efficient table lookups and minimal memory usage.
 */
struct alignas(1) PackedResult {
    uint8_t data;

    really_inline BranchlessState state() const {
        return static_cast<BranchlessState>((data >> 3) & 0x07);
    }

    really_inline BranchlessError error() const {
        return static_cast<BranchlessError>((data >> 6) & 0x03);
    }

    really_inline bool is_separator() const {
        return (data >> 2) & 0x01;
    }

    static really_inline PackedResult make(BranchlessState s, BranchlessError e, bool sep) {
        PackedResult r;
        r.data = (static_cast<uint8_t>(e) << 6) |
                 (static_cast<uint8_t>(s) << 3) |
                 (sep ? 0x04 : 0x00);
        return r;
    }
};

/**
 * @brief Branchless CSV state machine using lookup tables.
 *
 * The state machine processes characters without branches by using:
 * 1. A character classification table (256 bytes) for O(1) character -> class mapping
 * 2. A state transition table (5 states × 4 char classes = 20 bytes) for O(1) transitions
 *
 * This eliminates the switch statements in the original implementation that caused
 * significant branch mispredictions (64+ possible mispredictions per 64-byte block).
 */
class BranchlessStateMachine {
public:
    /**
     * @brief Initialize the state machine with given delimiter and quote characters.
     */
    explicit BranchlessStateMachine(char delimiter = ',', char quote_char = '"') {
        init_char_class_table(delimiter, quote_char);
        init_transition_table();
    }

    /**
     * @brief Reinitialize with new delimiter and quote characters.
     */
    void reinit(char delimiter, char quote_char) {
        init_char_class_table(delimiter, quote_char);
    }

    /**
     * @brief Classify a single character (branchless).
     */
    really_inline CharClass classify(uint8_t c) const {
        return static_cast<CharClass>(char_class_table_[c]);
    }

    /**
     * @brief Get the next state for a given current state and character class (branchless).
     */
    really_inline PackedResult transition(BranchlessState state, CharClass char_class) const {
        return transition_table_[state * 4 + char_class];
    }

    /**
     * @brief Process a single character and return the new state (branchless).
     *
     * This is the main entry point for character-by-character processing.
     * It combines classification and transition in a single call.
     */
    really_inline PackedResult process(BranchlessState state, uint8_t c) const {
        return transition(state, classify(c));
    }

    /**
     * @brief Create 64-bit bitmask for characters matching the delimiter.
     */
    really_inline uint64_t delimiter_mask(const simd_input& in) const {
        return cmp_mask_against_input(in, delimiter_);
    }

    /**
     * @brief Create 64-bit bitmask for characters matching the quote character.
     */
    really_inline uint64_t quote_mask(const simd_input& in) const {
        return cmp_mask_against_input(in, quote_char_);
    }

    /**
     * @brief Create 64-bit bitmask for newline characters.
     */
    really_inline uint64_t newline_mask(const simd_input& in) const {
        return cmp_mask_against_input(in, '\n');
    }

    /**
     * @brief Get current delimiter character.
     */
    really_inline char delimiter() const { return delimiter_; }

    /**
     * @brief Get current quote character.
     */
    really_inline char quote_char() const { return quote_char_; }

private:
    // Character classification table (256 entries for O(1) lookup)
    alignas(64) uint8_t char_class_table_[256];

    // State transition table (5 states × 4 char classes = 20 entries)
    // Packed results for efficient access
    alignas(32) PackedResult transition_table_[20];

    // Store delimiter and quote for SIMD operations
    char delimiter_;
    char quote_char_;

    /**
     * @brief Initialize the character classification table.
     *
     * Default classification is OTHER (3). Only delimiter, quote, and newline
     * get special classifications.
     */
    void init_char_class_table(char delimiter, char quote_char) {
        delimiter_ = delimiter;
        quote_char_ = quote_char;

        // Initialize all characters as OTHER
        for (int i = 0; i < 256; ++i) {
            char_class_table_[i] = CHAR_OTHER;
        }

        // Set special characters
        char_class_table_[static_cast<uint8_t>(delimiter)] = CHAR_DELIMITER;
        char_class_table_[static_cast<uint8_t>(quote_char)] = CHAR_QUOTE;
        char_class_table_[static_cast<uint8_t>('\n')] = CHAR_NEWLINE;
    }

    /**
     * @brief Initialize the state transition table.
     *
     * This table encodes all valid CSV state transitions:
     *
     * State transitions based on RFC 4180:
     *
     * From RECORD_START:
     *   - DELIMITER -> FIELD_START (record separator)
     *   - QUOTE -> QUOTED_FIELD (start quoted field)
     *   - NEWLINE -> RECORD_START (empty row, record separator)
     *   - OTHER -> UNQUOTED_FIELD (start unquoted field)
     *
     * From FIELD_START:
     *   - DELIMITER -> FIELD_START (empty field, field separator)
     *   - QUOTE -> QUOTED_FIELD (start quoted field)
     *   - NEWLINE -> RECORD_START (empty field at end of row, record separator)
     *   - OTHER -> UNQUOTED_FIELD (start unquoted field)
     *
     * From UNQUOTED_FIELD:
     *   - DELIMITER -> FIELD_START (end field, field separator)
     *   - QUOTE -> ERROR (quote in unquoted field)
     *   - NEWLINE -> RECORD_START (end field and row, record separator)
     *   - OTHER -> UNQUOTED_FIELD (continue field)
     *
     * From QUOTED_FIELD:
     *   - DELIMITER -> QUOTED_FIELD (literal comma in field)
     *   - QUOTE -> QUOTED_END (potential end or escape)
     *   - NEWLINE -> QUOTED_FIELD (literal newline in field)
     *   - OTHER -> QUOTED_FIELD (continue field)
     *
     * From QUOTED_END:
     *   - DELIMITER -> FIELD_START (end quoted field, field separator)
     *   - QUOTE -> QUOTED_FIELD (escaped quote, continue)
     *   - NEWLINE -> RECORD_START (end quoted field and row, record separator)
     *   - OTHER -> ERROR (invalid char after closing quote)
     */
    void init_transition_table() {
        // RECORD_START transitions (index 0-3)
        transition_table_[STATE_RECORD_START * 4 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        transition_table_[STATE_RECORD_START * 4 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_RECORD_START * 4 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_RECORD_START * 4 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

        // FIELD_START transitions (index 4-7)
        transition_table_[STATE_FIELD_START * 4 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        transition_table_[STATE_FIELD_START * 4 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_FIELD_START * 4 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_FIELD_START * 4 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

        // UNQUOTED_FIELD transitions (index 8-11)
        transition_table_[STATE_UNQUOTED_FIELD * 4 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        transition_table_[STATE_UNQUOTED_FIELD * 4 + CHAR_QUOTE] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_QUOTE_IN_UNQUOTED, false);
        transition_table_[STATE_UNQUOTED_FIELD * 4 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_UNQUOTED_FIELD * 4 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

        // QUOTED_FIELD transitions (index 12-15)
        transition_table_[STATE_QUOTED_FIELD * 4 + CHAR_DELIMITER] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_QUOTED_FIELD * 4 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_END, ERR_NONE, false);
        transition_table_[STATE_QUOTED_FIELD * 4 + CHAR_NEWLINE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_QUOTED_FIELD * 4 + CHAR_OTHER] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);

        // QUOTED_END transitions (index 16-19)
        transition_table_[STATE_QUOTED_END * 4 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        transition_table_[STATE_QUOTED_END * 4 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_QUOTED_END * 4 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_QUOTED_END * 4 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_INVALID_AFTER_QUOTE, false);
    }
};

/**
 * @brief Process a 64-byte block using branchless state machine.
 *
 * This function processes characters sequentially but uses table lookups
 * instead of switch statements for state transitions. The SIMD operations
 * create bitmasks that can be used for field position extraction.
 *
 * @param sm The branchless state machine
 * @param buf Input buffer (64 bytes)
 * @param len Actual length to process (may be < 64 for final block)
 * @param state Current state (updated in-place)
 * @param indexes Output array for field separator positions
 * @param base Base position in the full buffer
 * @param idx Current index in indexes array
 * @param stride Stride for multi-threaded index storage
 * @return Number of field separators found
 */
really_inline size_t process_block_branchless(
    const BranchlessStateMachine& sm,
    const uint8_t* buf,
    size_t len,
    BranchlessState& state,
    uint64_t* indexes,
    uint64_t base,
    size_t& idx,
    size_t stride
) {
    size_t count = 0;

    for (size_t i = 0; i < len; ++i) {
        PackedResult result = sm.process(state, buf[i]);
        state = result.state();

        // Write separator position if this is a field/record separator
        // This is still a branch but it's highly predictable since
        // separators are relatively rare
        if (result.is_separator()) {
            indexes[idx * stride] = base + i;
            idx++;
            count++;
        }
    }

    return count;
}

/**
 * @brief SIMD-accelerated block processing with branchless state extraction.
 *
 * This function uses SIMD to find potential separator positions, then
 * uses the branchless state machine to validate which separators are
 * actually field boundaries (not inside quoted fields).
 *
 * The approach:
 * 1. Use SIMD to find all delimiter, quote, and newline positions (bitmasks)
 * 2. Compute quote mask to identify positions inside quoted strings
 * 3. Extract valid separator positions using bitwise operations
 * 4. Update state machine only at quote boundaries
 *
 * @param sm The branchless state machine
 * @param in SIMD input block (64 bytes)
 * @param len Actual length to process
 * @param prev_quote_state Previous iteration's inside-quote state (all 0s or 1s)
 * @param indexes Output array for field separator positions
 * @param base Base position in the full buffer
 * @param idx Current index in indexes array
 * @param stride Stride for multi-threaded index storage
 * @return Number of field separators found
 */
really_inline size_t process_block_simd_branchless(
    const BranchlessStateMachine& sm,
    const simd_input& in,
    size_t len,
    uint64_t& prev_quote_state,
    uint64_t* indexes,
    uint64_t base,
    uint64_t& idx,
    int stride
) {
    // Create mask for valid bytes (handle partial final block)
    uint64_t valid_mask = (len < 64) ? blsmsk_u64(1ULL << len) : ~0ULL;

    // Get bitmasks for special characters using SIMD
    uint64_t quotes = sm.quote_mask(in) & valid_mask;
    uint64_t delimiters = sm.delimiter_mask(in) & valid_mask;
    uint64_t newlines = sm.newline_mask(in) & valid_mask;

    // Compute quote mask: positions that are inside quotes
    // Uses XOR prefix sum to track quote parity
    uint64_t inside_quote = find_quote_mask2(in, quotes, prev_quote_state);

    // Field separators are delimiters/newlines that are NOT inside quotes
    uint64_t field_seps = (delimiters | newlines) & ~inside_quote & valid_mask;

    // Write separator positions
    return write(indexes, idx, base, stride, field_seps);
}

/**
 * @brief Second pass using branchless state machine (scalar fallback).
 *
 * This function processes the buffer using the branchless state machine
 * for character classification and state transitions. It's used when
 * error collection is needed or for debugging.
 *
 * @param sm The branchless state machine
 * @param buf Input buffer
 * @param start Start position
 * @param end End position
 * @param indexes Output array
 * @param thread_id Thread ID for interleaved storage
 * @param n_threads Total number of threads
 * @return Number of field separators found
 */
inline uint64_t second_pass_branchless(
    const BranchlessStateMachine& sm,
    const uint8_t* buf,
    size_t start,
    size_t end,
    uint64_t* indexes,
    size_t thread_id,
    size_t n_threads
) {
    BranchlessState state = STATE_RECORD_START;
    size_t idx = thread_id;
    uint64_t count = 0;

    for (size_t pos = start; pos < end; ++pos) {
        PackedResult result = sm.process(state, buf[pos]);
        state = result.state();

        if (result.is_separator()) {
            indexes[idx] = pos;
            idx += n_threads;
            count++;
        }
    }

    return count;
}

/**
 * @brief Second pass using SIMD-accelerated branchless processing.
 *
 * This is the main performance-optimized function that combines SIMD
 * character detection with branchless state tracking.
 *
 * @param sm The branchless state machine
 * @param buf Input buffer
 * @param start Start position
 * @param end End position
 * @param indexes Output array
 * @param thread_id Thread ID for interleaved storage
 * @param n_threads Total number of threads
 * @return Number of field separators found
 */
inline uint64_t second_pass_simd_branchless(
    const BranchlessStateMachine& sm,
    const uint8_t* buf,
    size_t start,
    size_t end,
    uint64_t* indexes,
    size_t thread_id,
    int n_threads
) {
    assert(end >= start && "Invalid range: end must be >= start");
    size_t len = end - start;
    size_t pos = 0;
    uint64_t idx = thread_id;
    uint64_t prev_quote_state = 0ULL;
    uint64_t count = 0;
    const uint8_t* data = buf + start;

    // Process 64-byte blocks
    for (; pos + 64 <= len; pos += 64) {
        __builtin_prefetch(data + pos + 128);

        simd_input in = fill_input(data + pos);
        count += process_block_simd_branchless(
            sm, in, 64, prev_quote_state,
            indexes, start + pos, idx, n_threads
        );
    }

    // Handle remaining bytes (< 64)
    if (pos < len) {
        simd_input in = fill_input(data + pos);
        count += process_block_simd_branchless(
            sm, in, len - pos, prev_quote_state,
            indexes, start + pos, idx, n_threads
        );
    }

    return count;
}

}  // namespace simdcsv

#endif  // SIMDCSV_BRANCHLESS_STATE_MACHINE_H
