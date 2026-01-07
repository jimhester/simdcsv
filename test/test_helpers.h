/**
 * Test helpers for libvroom unit tests.
 *
 * Provides RAII wrappers and utilities to prevent memory leaks in test code.
 */

#ifndef LIBVROOM_TEST_HELPERS_H
#define LIBVROOM_TEST_HELPERS_H

#include "io_util.h"
#include "mem_util.h"

#include <string>

/**
 * RAII wrapper for exception-safe memory management of corpus data.
 *
 * Automatically frees the memory allocated by get_corpus() when the
 * guard goes out of scope, preventing memory leaks even when tests
 * throw exceptions or use early returns.
 *
 * Usage:
 *   CorpusGuard corpus("path/to/file.csv");
 *   parser.parse(corpus.data.data(), idx, corpus.data.size());
 *   // No need to call free_buffer - automatically freed on scope exit
 */
struct CorpusGuard {
  std::basic_string_view<uint8_t> data;

  explicit CorpusGuard(const std::string& path) : data(get_corpus(path, LIBVROOM_PADDING)) {}

  ~CorpusGuard() {
    if (data.data()) {
      aligned_free(const_cast<uint8_t*>(data.data()));
    }
  }

  // Non-copyable, non-movable
  CorpusGuard(const CorpusGuard&) = delete;
  CorpusGuard& operator=(const CorpusGuard&) = delete;
  CorpusGuard(CorpusGuard&&) = delete;
  CorpusGuard& operator=(CorpusGuard&&) = delete;
};

#endif // LIBVROOM_TEST_HELPERS_H
