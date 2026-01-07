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
 * @see https://github.com/jimhester/libvroom/issues/41 For design discussion
 */

#ifndef LIBVROOM_BRANCHLESS_STATE_MACHINE_H
#define LIBVROOM_BRANCHLESS_STATE_MACHINE_H

#include <cassert>
#include <cstdint>
#include <cstddef>
#include "common_defs.h"
#include "simd_highway.h"
#include "error.h"

namespace libvroom {

/**
 * @brief Character classification for branchless CSV parsing.
 *
 * Characters are classified into 5 categories that determine state transitions:
 * - DELIMITER (0): Field separator (typically comma)
 * - QUOTE (1): Quote character (typically double-quote)
 * - NEWLINE (2): Line terminator (\n)
 * - OTHER (3): All other characters
 * - ESCAPE (4): Escape character (typically backslash when not using double-quote escaping)
 */
enum CharClass : uint8_t {
    CHAR_DELIMITER = 0,
    CHAR_QUOTE = 1,
    CHAR_NEWLINE = 2,
    CHAR_OTHER = 3,
    CHAR_ESCAPE = 4
};

/**
 * @brief CSV parser state for branchless state machine.
 *
 * Uses numeric values 0-5 for direct indexing into lookup tables.
 */
enum BranchlessState : uint8_t {
    STATE_RECORD_START = 0,    // At the beginning of a new record (row)
    STATE_FIELD_START = 1,     // At the beginning of a new field (after comma)
    STATE_UNQUOTED_FIELD = 2,  // Inside an unquoted field
    STATE_QUOTED_FIELD = 3,    // Inside a quoted field
    STATE_QUOTED_END = 4,      // Just saw a quote inside a quoted field
    STATE_ESCAPED = 5          // Just saw an escape character (next char is literal)
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
 * 2. A state transition table (6 states × 5 char classes = 30 bytes) for O(1) transitions
 *
 * This eliminates the switch statements in the original implementation that caused
 * significant branch mispredictions (64+ possible mispredictions per 64-byte block).
 *
 * Escape character handling:
 * - When double_quote=true (RFC 4180): escape_char is ignored, "" escapes to "
 * - When double_quote=false: escape_char (e.g., backslash) escapes the next character
 *   - Inside quotes: \" becomes literal "
 *   - Escape char can also escape delimiters, newlines, itself
 */
class BranchlessStateMachine {
public:
    /**
     * @brief Initialize the state machine with given delimiter, quote, and escape characters.
     *
     * @param delimiter Field separator character (default: comma)
     * @param quote_char Quote character (default: double-quote)
     * @param escape_char Escape character (default: same as quote_char for RFC 4180)
     * @param double_quote If true, use RFC 4180 double-quote escaping; if false, use escape_char
     */
    explicit BranchlessStateMachine(char delimiter = ',', char quote_char = '"',
                                     char escape_char = '"', bool double_quote = true) {
        init_char_class_table(delimiter, quote_char, escape_char, double_quote);
        init_transition_table(double_quote);
    }

    /**
     * @brief Reinitialize with new delimiter, quote, and escape characters.
     */
    void reinit(char delimiter, char quote_char, char escape_char = '"', bool double_quote = true) {
        init_char_class_table(delimiter, quote_char, escape_char, double_quote);
        init_transition_table(double_quote);
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
        return transition_table_[state * 5 + char_class];
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
     * @brief Create 64-bit bitmask for line ending characters.
     *
     * Supports LF (\n), CRLF (\r\n), and CR-only (\r) line endings:
     * - LF positions are always included
     * - CR positions are included only if NOT immediately followed by LF
     *
     * For CRLF sequences, only the LF is marked as the line ending.
     * The CR in CRLF is handled during value extraction (stripped from field end).
     */
    really_inline uint64_t newline_mask(const simd_input& in) const {
        return compute_line_ending_mask_simple(in, ~0ULL);
    }

    /**
     * @brief Create 64-bit bitmask for line endings with validity mask.
     */
    really_inline uint64_t newline_mask(const simd_input& in, uint64_t valid_mask) const {
        return compute_line_ending_mask_simple(in, valid_mask);
    }

    /**
     * @brief Get current delimiter character.
     */
    really_inline char delimiter() const { return delimiter_; }

    /**
     * @brief Get current quote character.
     */
    really_inline char quote_char() const { return quote_char_; }

    /**
     * @brief Get current escape character.
     */
    really_inline char escape_char() const { return escape_char_; }

    /**
     * @brief Check if using double-quote escaping (RFC 4180).
     */
    really_inline bool uses_double_quote() const { return double_quote_; }

    /**
     * @brief Create 64-bit bitmask for characters matching the escape character.
     * Only meaningful when not using double-quote mode.
     */
    really_inline uint64_t escape_mask(const simd_input& in) const {
        return cmp_mask_against_input(in, static_cast<uint8_t>(escape_char_));
    }

private:
    // Character classification table (256 entries for O(1) lookup)
    alignas(64) uint8_t char_class_table_[256];

    // State transition table (6 states × 5 char classes = 30 entries)
    // Packed results for efficient access
    alignas(32) PackedResult transition_table_[30];

    // Store delimiter, quote, and escape for SIMD operations
    char delimiter_;
    char quote_char_;
    char escape_char_;
    bool double_quote_;

    /**
     * @brief Initialize the character classification table.
     *
     * Default classification is OTHER (3). Special characters get their own
     * classifications: delimiter, quote, newline, and optionally escape.
     *
     * When double_quote=true (RFC 4180 mode), escape_char is not classified
     * as ESCAPE since escaping is handled by quote doubling.
     *
     * When double_quote=false (escape char mode), escape_char is classified
     * as ESCAPE so the state machine can handle backslash escaping.
     */
    void init_char_class_table(char delimiter, char quote_char,
                                char escape_char, bool double_quote) {
        delimiter_ = delimiter;
        quote_char_ = quote_char;
        escape_char_ = escape_char;
        double_quote_ = double_quote;

        // Initialize all characters as OTHER
        for (int i = 0; i < 256; ++i) {
            char_class_table_[i] = CHAR_OTHER;
        }

        // Set special characters
        char_class_table_[static_cast<uint8_t>(delimiter)] = CHAR_DELIMITER;
        char_class_table_[static_cast<uint8_t>(quote_char)] = CHAR_QUOTE;
        char_class_table_[static_cast<uint8_t>('\n')] = CHAR_NEWLINE;

        // Only classify escape character as ESCAPE when not using double-quote mode
        // and escape_char is different from quote_char
        if (!double_quote && escape_char != quote_char && escape_char != '\0') {
            char_class_table_[static_cast<uint8_t>(escape_char)] = CHAR_ESCAPE;
        }
    }

    /**
     * @brief Initialize the state transition table.
     *
     * This table encodes all valid CSV state transitions.
     *
     * For RFC 4180 mode (double_quote=true):
     * - Escaping is done by doubling quotes: "" -> "
     * - ESCAPE char class is never used (escape char not classified)
     *
     * For escape char mode (double_quote=false):
     * - Escaping is done with escape char: \" -> "
     * - ESCAPE transitions to STATE_ESCAPED, next char is literal
     *
     * State transitions:
     *
     * From RECORD_START:
     *   - DELIMITER -> FIELD_START (record separator)
     *   - QUOTE -> QUOTED_FIELD (start quoted field)
     *   - NEWLINE -> RECORD_START (empty row, record separator)
     *   - OTHER -> UNQUOTED_FIELD (start unquoted field)
     *   - ESCAPE -> UNQUOTED_FIELD (escape at field start, treat as content)
     *
     * From FIELD_START:
     *   - DELIMITER -> FIELD_START (empty field, field separator)
     *   - QUOTE -> QUOTED_FIELD (start quoted field)
     *   - NEWLINE -> RECORD_START (empty field at end of row, record separator)
     *   - OTHER -> UNQUOTED_FIELD (start unquoted field)
     *   - ESCAPE -> UNQUOTED_FIELD (escape at field start, treat as content)
     *
     * From UNQUOTED_FIELD:
     *   - DELIMITER -> FIELD_START (end field, field separator)
     *   - QUOTE -> ERROR (quote in unquoted field) [RFC 4180 mode]
     *            or UNQUOTED_FIELD (literal quote in escape mode if no escape)
     *   - NEWLINE -> RECORD_START (end field and row, record separator)
     *   - OTHER -> UNQUOTED_FIELD (continue field)
     *   - ESCAPE -> UNQUOTED_FIELD (in escape mode, next char literal but stay unquoted)
     *
     * From QUOTED_FIELD:
     *   - DELIMITER -> QUOTED_FIELD (literal comma in field)
     *   - QUOTE -> QUOTED_END (potential end or RFC 4180 escape)
     *   - NEWLINE -> QUOTED_FIELD (literal newline in field)
     *   - OTHER -> QUOTED_FIELD (continue field)
     *   - ESCAPE -> STATE_ESCAPED (in escape mode, next char is literal)
     *
     * From QUOTED_END:
     *   - DELIMITER -> FIELD_START (end quoted field, field separator)
     *   - QUOTE -> QUOTED_FIELD (RFC 4180 escaped quote, continue)
     *   - NEWLINE -> RECORD_START (end quoted field and row, record separator)
     *   - OTHER -> ERROR (invalid char after closing quote)
     *   - ESCAPE -> ERROR (invalid, escape after closing quote)
     *
     * From STATE_ESCAPED (only used in escape char mode):
     *   - Any char -> QUOTED_FIELD (literal char, continue quoted field)
     *   Note: The escaped character is consumed as a literal, regardless of what it is
     */
    void init_transition_table(bool double_quote) {
        // RECORD_START transitions (index 0-4)
        transition_table_[STATE_RECORD_START * 5 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        transition_table_[STATE_RECORD_START * 5 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_RECORD_START * 5 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_RECORD_START * 5 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);
        // ESCAPE at record start: start unquoted field (escape is just content)
        transition_table_[STATE_RECORD_START * 5 + CHAR_ESCAPE] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

        // FIELD_START transitions (index 5-9)
        transition_table_[STATE_FIELD_START * 5 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        transition_table_[STATE_FIELD_START * 5 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_FIELD_START * 5 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_FIELD_START * 5 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);
        // ESCAPE at field start: start unquoted field (escape is just content)
        transition_table_[STATE_FIELD_START * 5 + CHAR_ESCAPE] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

        // UNQUOTED_FIELD transitions (index 10-14)
        transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        // In double-quote mode, quote in unquoted field is an error
        // In escape mode, quote in unquoted field is also an error (should be preceded by escape)
        transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_QUOTE] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_QUOTE_IN_UNQUOTED, false);
        transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);
        // ESCAPE in unquoted field: stay in unquoted field (escape is content in unquoted)
        // We don't support escaping in unquoted fields - the escape is just literal content
        transition_table_[STATE_UNQUOTED_FIELD * 5 + CHAR_ESCAPE] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_NONE, false);

        // QUOTED_FIELD transitions (index 15-19)
        transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_DELIMITER] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_END, ERR_NONE, false);
        transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_NEWLINE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_OTHER] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        // ESCAPE in quoted field: go to escaped state (next char is literal)
        // Note: In double_quote mode, ESCAPE char class is never assigned, so this won't be reached
        transition_table_[STATE_QUOTED_FIELD * 5 + CHAR_ESCAPE] =
            PackedResult::make(STATE_ESCAPED, ERR_NONE, false);

        // QUOTED_END transitions (index 20-24)
        transition_table_[STATE_QUOTED_END * 5 + CHAR_DELIMITER] =
            PackedResult::make(STATE_FIELD_START, ERR_NONE, true);
        // In double_quote mode: quote after quote = escaped quote, back to quoted field
        // In escape mode: this state means quote closed the field, another quote is an error
        if (double_quote) {
            transition_table_[STATE_QUOTED_END * 5 + CHAR_QUOTE] =
                PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        } else {
            // In escape mode, we shouldn't see quote after closing quote
            // This would be ""  which in escape mode means empty closing followed by opening
            // But we're already past the closing quote, so this is an error
            transition_table_[STATE_QUOTED_END * 5 + CHAR_QUOTE] =
                PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);  // Allow for compatibility
        }
        transition_table_[STATE_QUOTED_END * 5 + CHAR_NEWLINE] =
            PackedResult::make(STATE_RECORD_START, ERR_NONE, true);
        transition_table_[STATE_QUOTED_END * 5 + CHAR_OTHER] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_INVALID_AFTER_QUOTE, false);
        // ESCAPE after closing quote: error (should not have escape after closing quote)
        transition_table_[STATE_QUOTED_END * 5 + CHAR_ESCAPE] =
            PackedResult::make(STATE_UNQUOTED_FIELD, ERR_INVALID_AFTER_QUOTE, false);

        // STATE_ESCAPED transitions (index 25-29)
        // After escape char, any character is literal and we return to quoted field
        // This is the key for backslash escaping: \" becomes literal "
        transition_table_[STATE_ESCAPED * 5 + CHAR_DELIMITER] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_ESCAPED * 5 + CHAR_QUOTE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_ESCAPED * 5 + CHAR_NEWLINE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_ESCAPED * 5 + CHAR_OTHER] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);
        transition_table_[STATE_ESCAPED * 5 + CHAR_ESCAPE] =
            PackedResult::make(STATE_QUOTED_FIELD, ERR_NONE, false);  // \\ is escaped backslash
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
 * 3. For escape char mode: mask out escaped quotes before computing quote parity
 * 4. Extract valid separator positions using bitwise operations
 * 5. Update state machine only at quote boundaries
 *
 * @param sm The branchless state machine
 * @param in SIMD input block (64 bytes)
 * @param len Actual length to process
 * @param prev_quote_state Previous iteration's inside-quote state (all 0s or 1s)
 * @param prev_escape_carry For escape char mode: whether previous block ended with unmatched escape
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
    uint64_t& prev_escape_carry,
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
    // Use newline_mask with valid_mask for proper CR/CRLF handling
    uint64_t newlines = sm.newline_mask(in, valid_mask);

    // Handle escape character mode (e.g., backslash escaping)
    // In escape mode, we need to ignore quotes that are preceded by an escape char
    uint64_t escaped_positions = 0;
    if (!sm.uses_double_quote()) {
        uint64_t escapes = sm.escape_mask(in) & valid_mask;
        escaped_positions = compute_escaped_mask(escapes, prev_escape_carry);

        // Remove escaped quotes from the quote mask
        // An escaped quote doesn't toggle quote state
        quotes &= ~escaped_positions;
        // Also remove escaped delimiters and newlines (they're literal content)
        delimiters &= ~escaped_positions;
        newlines &= ~escaped_positions;
    }

    // Compute quote mask: positions that are inside quotes
    // Uses XOR prefix sum to track quote parity
    uint64_t inside_quote = find_quote_mask2(quotes, prev_quote_state);

    // Debug output for escape mode
    (void)escaped_positions;  // Silence unused warning when debug disabled
#if 0
    if (!sm.uses_double_quote() && base == 0) {
        fprintf(stderr, "DEBUG SIMD escape processing:\n");
        fprintf(stderr, "  valid_mask=0x%016llx\n", (unsigned long long)valid_mask);
        fprintf(stderr, "  escapes=0x%016llx\n", (unsigned long long)(sm.escape_mask(in) & valid_mask));
        fprintf(stderr, "  escaped_positions=0x%016llx\n", (unsigned long long)escaped_positions);
        fprintf(stderr, "  quotes (after masking)=0x%016llx\n", (unsigned long long)quotes);
        fprintf(stderr, "  inside_quote=0x%016llx\n", (unsigned long long)inside_quote);
        fprintf(stderr, "  prev_quote_state=0x%016llx\n", (unsigned long long)prev_quote_state);
    }
#endif

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
 * Supports both RFC 4180 double-quote escaping and custom escape character
 * modes (e.g., backslash escaping).
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
    uint64_t idx = 0;  // Start at 0; thread offset handled by base pointer
    uint64_t prev_quote_state = 0ULL;
    uint64_t prev_escape_carry = 0ULL;  // For escape char mode
    uint64_t count = 0;
    const uint8_t* data = buf + start;

    // Process 64-byte blocks
    // Pass indexes + thread_id so each thread writes to its own interleaved slots:
    // thread 0 -> indexes[0], indexes[n_threads], indexes[2*n_threads], ...
    // thread 1 -> indexes[1], indexes[n_threads+1], indexes[2*n_threads+1], ...
    for (; pos + 64 <= len; pos += 64) {
        __builtin_prefetch(data + pos + 128);

        simd_input in = fill_input(data + pos);
        count += process_block_simd_branchless(
            sm, in, 64, prev_quote_state, prev_escape_carry,
            indexes + thread_id, start + pos, idx, n_threads
        );
    }

    // Handle remaining bytes (< 64)
    if (pos < len) {
        simd_input in = fill_input(data + pos);
        count += process_block_simd_branchless(
            sm, in, len - pos, prev_quote_state, prev_escape_carry,
            indexes + thread_id, start + pos, idx, n_threads
        );
    }

    return count;
}

/**
 * @brief Convert BranchlessError to ErrorCode.
 *
 * Maps the compact branchless error codes to the full ErrorCode enum for
 * compatibility with the error collection framework.
 */
really_inline ErrorCode branchless_error_to_error_code(BranchlessError err) {
    switch (err) {
        case ERR_NONE:
            return ErrorCode::NONE;
        case ERR_QUOTE_IN_UNQUOTED:
            return ErrorCode::QUOTE_IN_UNQUOTED_FIELD;
        case ERR_INVALID_AFTER_QUOTE:
            return ErrorCode::INVALID_QUOTE_ESCAPE;
        default:
            return ErrorCode::INTERNAL_ERROR;
    }
}

/**
 * @brief Helper to get context around an error position.
 *
 * Returns a string representation of the buffer content near the given position.
 */
inline std::string get_error_context(const uint8_t* buf, size_t len, size_t pos,
                                     size_t context_size = 20) {
    if (len == 0 || buf == nullptr) return "";
    size_t safe_pos = pos < len ? pos : len - 1;
    size_t ctx_start = safe_pos > context_size ? safe_pos - context_size : 0;
    size_t ctx_end = std::min(safe_pos + context_size, len);

    std::string ctx;
    ctx.reserve((ctx_end - ctx_start) * 2);

    for (size_t i = ctx_start; i < ctx_end; ++i) {
        char c = static_cast<char>(buf[i]);
        if (c == '\n') ctx += "\\n";
        else if (c == '\r') ctx += "\\r";
        else if (c == '\0') ctx += "\\0";
        else if (c >= 32 && c < 127) ctx += c;
        else ctx += "?";
    }
    return ctx;
}

/**
 * @brief Helper to calculate line and column from byte offset.
 */
inline void get_error_line_column(const uint8_t* buf, size_t buf_len, size_t offset,
                                  size_t& line, size_t& column) {
    line = 1;
    column = 1;
    size_t safe_offset = offset < buf_len ? offset : buf_len;
    for (size_t i = 0; i < safe_offset; ++i) {
        if (buf[i] == '\n') {
            ++line;
            column = 1;
        } else if (buf[i] != '\r') {
            ++column;
        }
    }
}

/**
 * @brief Second pass using branchless state machine with error collection.
 *
 * This function processes the buffer using the branchless state machine
 * for character classification and state transitions, while collecting
 * errors in the provided ErrorCollector.
 *
 * @param sm The branchless state machine
 * @param buf Input buffer
 * @param start Start position
 * @param end End position
 * @param indexes Output array
 * @param thread_id Thread ID for interleaved storage
 * @param n_threads Total number of threads
 * @param errors ErrorCollector to accumulate errors (may be nullptr)
 * @param total_len Total buffer length for bounds checking
 * @return Number of field separators found
 */
inline uint64_t second_pass_branchless_with_errors(
    const BranchlessStateMachine& sm,
    const uint8_t* buf,
    size_t start,
    size_t end,
    uint64_t* indexes,
    size_t thread_id,
    size_t n_threads,
    ErrorCollector* errors,
    size_t total_len
) {
    BranchlessState state = STATE_RECORD_START;
    size_t idx = thread_id;
    uint64_t count = 0;

    // Use effective buffer length for bounds checking
    size_t buf_len = total_len > 0 ? total_len : end;
    char quote_char = sm.quote_char();

    for (size_t pos = start; pos < end; ++pos) {
        uint8_t value = buf[pos];

        // Check for null bytes
        if (value == '\0' && errors) {
            size_t line, col;
            get_error_line_column(buf, buf_len, pos, line, col);
            errors->add_error(ErrorCode::NULL_BYTE, ErrorSeverity::ERROR,
                              line, col, pos, "Null byte in data",
                              get_error_context(buf, buf_len, pos));
            if (errors->should_stop()) return count;
            continue;
        }

        PackedResult result = sm.process(state, value);
        BranchlessState new_state = result.state();
        BranchlessError err = result.error();

        // Handle errors
        if (err != ERR_NONE && errors) {
            size_t line, col;
            get_error_line_column(buf, buf_len, pos, line, col);
            ErrorCode error_code = branchless_error_to_error_code(err);

            std::string msg;
            if (err == ERR_QUOTE_IN_UNQUOTED) {
                msg = "Quote character '";
                msg += quote_char;
                msg += "' in unquoted field";
            } else if (err == ERR_INVALID_AFTER_QUOTE) {
                msg = "Invalid character after closing quote '";
                msg += quote_char;
                msg += "'";
            }

            errors->add_error(error_code, ErrorSeverity::ERROR,
                              line, col, pos, msg,
                              get_error_context(buf, buf_len, pos));
            if (errors->should_stop()) return count;
        }

        // Handle CR specially for CRLF sequences
        if (value == '\r') {
            // CR is a line ending only if not followed by LF
            // Check both end and buf_len bounds to prevent out-of-bounds read
            bool is_line_ending = (pos + 1 >= end || pos + 1 >= buf_len || buf[pos + 1] != '\n');
            if (is_line_ending && state != STATE_QUOTED_FIELD) {
                indexes[idx] = pos;
                idx += n_threads;
                count++;
                state = STATE_RECORD_START;
                continue;
            }
            // If CR is followed by LF (CRLF), treat CR as regular character
            // The LF will be the line ending
            state = new_state;
            continue;
        }

        state = new_state;

        if (result.is_separator()) {
            indexes[idx] = pos;
            idx += n_threads;
            count++;
        }
    }

    // Check for unclosed quote at end of chunk
    if (state == STATE_QUOTED_FIELD && errors && end == buf_len) {
        size_t line, col;
        size_t error_pos = end > 0 ? end - 1 : 0;
        get_error_line_column(buf, buf_len, error_pos, line, col);
        std::string msg = "Unclosed quote '";
        msg += quote_char;
        msg += "' at end of file";
        errors->add_error(ErrorCode::UNCLOSED_QUOTE, ErrorSeverity::FATAL,
                          line, col, end, msg,
                          get_error_context(buf, buf_len, error_pos > 20 ? error_pos - 20 : 0));
    }

    return count;
}

}  // namespace libvroom

#endif  // LIBVROOM_BRANCHLESS_STATE_MACHINE_H
