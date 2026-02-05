#include "libvroom/arrow_column_builder.h"
#include "libvroom/encoding.h"
#include "libvroom/parse_utils.h"
#include "libvroom/parsed_chunk_queue.h"
#include "libvroom/vroom.h"

#include "BS_thread_pool.hpp"

#include <algorithm>
#include <cstring>
#include <thread>
#include <vector>

namespace libvroom {

// ============================================================================
// Helper: skip leading comment lines
// ============================================================================
static size_t skip_leading_comment_lines_fwf(const char* data, size_t size, char comment_char) {
  if (comment_char == '\0' || size == 0)
    return 0;

  size_t offset = 0;
  while (offset < size) {
    if (data[offset] != comment_char)
      break;
    while (offset < size && data[offset] != '\n' && data[offset] != '\r')
      offset++;
    if (offset < size && data[offset] == '\r') {
      offset++;
      if (offset < size && data[offset] == '\n')
        offset++;
    } else if (offset < size && data[offset] == '\n') {
      offset++;
    }
  }
  return offset;
}

// ============================================================================
// Helper: skip N lines
// ============================================================================
static size_t skip_n_lines(const char* data, size_t size, size_t n) {
  size_t offset = 0;
  for (size_t i = 0; i < n && offset < size; ++i) {
    while (offset < size && data[offset] != '\n' && data[offset] != '\r')
      offset++;
    if (offset < size && data[offset] == '\r') {
      offset++;
      if (offset < size && data[offset] == '\n')
        offset++;
    } else if (offset < size && data[offset] == '\n') {
      offset++;
    }
  }
  return offset;
}

// ============================================================================
// Core: parse a chunk of FWF data
// ============================================================================
static size_t parse_fwf_chunk(const char* data, size_t size, const FwfOptions& options,
                              const NullChecker& null_checker,
                              std::vector<std::unique_ptr<ArrowColumnBuilder>>& columns,
                              int64_t max_rows = -1) {
  if (size == 0 || columns.empty())
    return 0;

  std::vector<FastArrowContext> fast_contexts;
  fast_contexts.reserve(columns.size());
  for (auto& col : columns)
    fast_contexts.push_back(col->create_context());

  const size_t num_cols = columns.size();
  const bool trim = options.trim_ws;
  const char comment = options.comment;
  const bool skip_empty = options.skip_empty_rows;

  size_t offset = 0;
  size_t row_count = 0;

  while (offset < size) {
    if (max_rows >= 0 && static_cast<int64_t>(row_count) >= max_rows)
      break;

    const char* line_start = data + offset;
    const char* newline = static_cast<const char*>(memchr(line_start, '\n', size - offset));
    size_t line_end_offset;
    if (newline) {
      line_end_offset = static_cast<size_t>(newline - data) + 1;
    } else {
      line_end_offset = size;
    }

    size_t raw_line_len = (newline ? static_cast<size_t>(newline - line_start) : size - offset);
    size_t line_len = raw_line_len;
    if (line_len > 0 && line_start[line_len - 1] == '\r')
      line_len--;

    if (skip_empty && line_len == 0) {
      offset = line_end_offset;
      continue;
    }

    if (comment != '\0' && line_len > 0 && line_start[0] == comment) {
      offset = line_end_offset;
      continue;
    }

    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
      size_t cs = static_cast<size_t>(options.col_starts[col_idx]);
      int col_end = options.col_ends[col_idx];

      std::string_view field;
      if (cs >= line_len) {
        field = std::string_view();
      } else if (col_end == -1) {
        field = std::string_view(line_start + cs, line_len - cs);
      } else {
        size_t end = std::min(static_cast<size_t>(col_end), line_len);
        field = (end > cs) ? std::string_view(line_start + cs, end - cs) : std::string_view();
      }

      if (trim && !field.empty()) {
        size_t first = field.find_first_not_of(" \t");
        if (first == std::string_view::npos) {
          field = std::string_view();
        } else {
          size_t last = field.find_last_not_of(" \t");
          field = field.substr(first, last - first + 1);
        }
      }

      if (null_checker.is_null(field)) {
        fast_contexts[col_idx].append_null();
      } else {
        fast_contexts[col_idx].append(field);
      }
    }

    row_count++;
    offset = line_end_offset;
  }

  return row_count;
}

// ============================================================================
// Type inference for FWF
// ============================================================================
static std::vector<DataType> infer_fwf_types(const char* data, size_t size,
                                             const FwfOptions& options, size_t max_rows) {
  const size_t num_cols = options.col_starts.size();
  std::vector<DataType> types(num_cols, DataType::UNKNOWN);

  CsvOptions csv_opts;
  csv_opts.null_values = options.null_values;
  csv_opts.true_values = options.true_values;
  csv_opts.false_values = options.false_values;
  TypeInference inference(csv_opts);

  const bool trim = options.trim_ws;
  const char comment = options.comment;
  const bool skip_empty = options.skip_empty_rows;

  size_t offset = 0;
  size_t rows_sampled = 0;

  while (offset < size && rows_sampled < max_rows) {
    const char* line_start = data + offset;
    const char* newline = static_cast<const char*>(memchr(line_start, '\n', size - offset));
    size_t line_end_offset;
    if (newline) {
      line_end_offset = static_cast<size_t>(newline - data) + 1;
    } else {
      line_end_offset = size;
    }

    size_t raw_line_len = (newline ? static_cast<size_t>(newline - line_start) : size - offset);
    size_t line_len = raw_line_len;
    if (line_len > 0 && line_start[line_len - 1] == '\r')
      line_len--;

    if (skip_empty && line_len == 0) {
      offset = line_end_offset;
      continue;
    }
    if (comment != '\0' && line_len > 0 && line_start[0] == comment) {
      offset = line_end_offset;
      continue;
    }

    for (size_t col_idx = 0; col_idx < num_cols; ++col_idx) {
      size_t cs = static_cast<size_t>(options.col_starts[col_idx]);
      int col_end = options.col_ends[col_idx];

      std::string_view field;
      if (cs >= line_len) {
        field = std::string_view();
      } else if (col_end == -1) {
        field = std::string_view(line_start + cs, line_len - cs);
      } else {
        size_t end = std::min(static_cast<size_t>(col_end), line_len);
        field = (end > cs) ? std::string_view(line_start + cs, end - cs) : std::string_view();
      }

      if (trim && !field.empty()) {
        size_t first = field.find_first_not_of(" \t");
        if (first == std::string_view::npos) {
          field = std::string_view();
        } else {
          size_t last = field.find_last_not_of(" \t");
          field = field.substr(first, last - first + 1);
        }
      }

      DataType field_type = inference.infer_field(field);
      types[col_idx] = wider_type(types[col_idx], field_type);
    }

    rows_sampled++;
    offset = line_end_offset;
  }

  for (auto& t : types) {
    if (t == DataType::UNKNOWN)
      t = DataType::STRING;
  }

  return types;
}

// ============================================================================
// FwfReader::Impl
// ============================================================================
struct FwfReader::Impl {
  FwfOptions options;
  MmapSource source;
  AlignedBuffer owned_buffer;
  const char* data_ptr = nullptr;
  size_t data_size = 0;
  std::vector<ColumnSchema> schema;
  size_t row_count = 0;
  size_t data_start_offset = 0;
  size_t num_threads = 0;
  EncodingResult detected_encoding;

  std::unique_ptr<ParsedChunkQueue> streaming_queue;
  std::unique_ptr<BS::thread_pool> streaming_pool;
  bool streaming_active = false;

  ~Impl() {
    if (streaming_queue)
      streaming_queue->close();
    streaming_pool.reset();
  }

  Impl(const FwfOptions& opts) : options(opts) {
    if (options.num_threads > 0) {
      num_threads = options.num_threads;
    } else {
      num_threads = std::thread::hardware_concurrency();
      if (num_threads == 0)
        num_threads = 4;
    }
  }
};

FwfReader::FwfReader(const FwfOptions& options) : impl_(std::make_unique<Impl>(options)) {}
FwfReader::~FwfReader() = default;

// ============================================================================
// Shared initialization
// ============================================================================
Result<bool> FwfReader::initialize_data() {
  if (impl_->data_size == 0)
    return Result<bool>::failure("Empty file");

  // Validate column specifications
  if (impl_->options.col_starts.empty())
    return Result<bool>::failure("col_starts must not be empty");
  if (impl_->options.col_starts.size() != impl_->options.col_ends.size())
    return Result<bool>::failure("col_starts and col_ends must have the same length");
  for (size_t i = 0; i < impl_->options.col_starts.size(); ++i) {
    if (impl_->options.col_starts[i] < 0)
      return Result<bool>::failure("col_starts values must be non-negative");
  }

  // Encoding detection and transcoding
  {
    const auto* raw = reinterpret_cast<const uint8_t*>(impl_->data_ptr);
    size_t raw_size = impl_->data_size;

    if (impl_->options.encoding.has_value()) {
      impl_->detected_encoding.encoding = *impl_->options.encoding;
      auto bom_result = detect_encoding(raw, raw_size);
      if (bom_result.encoding == *impl_->options.encoding ||
          (*impl_->options.encoding == CharEncoding::UTF8 &&
           bom_result.encoding == CharEncoding::UTF8_BOM)) {
        impl_->detected_encoding.bom_length = bom_result.bom_length;
      }
      impl_->detected_encoding.confidence = 1.0;
      impl_->detected_encoding.needs_transcoding =
          (*impl_->options.encoding != CharEncoding::UTF8 &&
           *impl_->options.encoding != CharEncoding::UTF8_BOM);
    } else {
      impl_->detected_encoding = detect_encoding(raw, raw_size);
    }

    if (impl_->detected_encoding.needs_transcoding) {
      impl_->owned_buffer = transcode_to_utf8(raw, raw_size, impl_->detected_encoding.encoding,
                                              impl_->detected_encoding.bom_length);
      impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
      impl_->data_size = impl_->owned_buffer.size();
    } else if (impl_->detected_encoding.bom_length > 0) {
      impl_->data_ptr += impl_->detected_encoding.bom_length;
      impl_->data_size -= impl_->detected_encoding.bom_length;
    }
  }

  const char* data = impl_->data_ptr;
  size_t size = impl_->data_size;

  // Skip leading comment lines
  size_t comment_offset = skip_leading_comment_lines_fwf(data, size, impl_->options.comment);
  data += comment_offset;
  size -= comment_offset;

  // Skip N lines
  if (impl_->options.skip > 0) {
    size_t skip_offset = skip_n_lines(data, size, impl_->options.skip);
    data += skip_offset;
    size -= skip_offset;
  }

  impl_->data_start_offset = static_cast<size_t>(data - impl_->data_ptr);

  if (size == 0) {
    for (size_t i = 0; i < impl_->options.col_names.size(); ++i) {
      ColumnSchema col;
      col.name = impl_->options.col_names[i];
      col.index = i;
      col.type = DataType::STRING;
      impl_->schema.push_back(std::move(col));
    }
    return Result<bool>::success(true);
  }

  // Type inference
  auto inferred_types = infer_fwf_types(data, size, impl_->options, impl_->options.sample_rows);

  // Build schema
  size_t num_cols = impl_->options.col_starts.size();
  for (size_t i = 0; i < num_cols; ++i) {
    ColumnSchema col;
    col.name = (i < impl_->options.col_names.size()) ? impl_->options.col_names[i]
                                                     : "V" + std::to_string(i + 1);
    col.index = i;
    col.type = (i < inferred_types.size()) ? inferred_types[i] : DataType::STRING;
    impl_->schema.push_back(std::move(col));
  }

  return Result<bool>::success(true);
}

Result<bool> FwfReader::open(const std::string& path) {
  auto result = impl_->source.open(path);
  if (!result)
    return result;

  impl_->data_ptr = impl_->source.data();
  impl_->data_size = impl_->source.size();
  return initialize_data();
}

Result<bool> FwfReader::open_from_buffer(AlignedBuffer buffer) {
  impl_->owned_buffer = std::move(buffer);
  impl_->data_ptr = reinterpret_cast<const char*>(impl_->owned_buffer.data());
  impl_->data_size = impl_->owned_buffer.size();
  return initialize_data();
}

const std::vector<ColumnSchema>& FwfReader::schema() const {
  return impl_->schema;
}

size_t FwfReader::row_count() const {
  return impl_->row_count;
}

const EncodingResult& FwfReader::encoding() const {
  return impl_->detected_encoding;
}

// ============================================================================
// Serial read
// ============================================================================
Result<ParsedChunks> FwfReader::read_all_serial() {
  ParsedChunks result;

  if (impl_->schema.empty())
    return Result<ParsedChunks>::success(std::move(result));

  std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
  for (const auto& col_schema : impl_->schema) {
    columns.push_back(ArrowColumnBuilder::create(col_schema.type));
  }

  const char* data = impl_->data_ptr + impl_->data_start_offset;
  size_t size = impl_->data_size - impl_->data_start_offset;

  NullChecker null_checker(impl_->options);
  size_t rows =
      parse_fwf_chunk(data, size, impl_->options, null_checker, columns, impl_->options.max_rows);

  result.total_rows = rows;
  impl_->row_count = rows;
  result.chunks.push_back(std::move(columns));
  return Result<ParsedChunks>::success(std::move(result));
}

// ============================================================================
// Streaming API
// ============================================================================
Result<bool> FwfReader::start_streaming() {
  if (impl_->schema.empty())
    return Result<bool>::failure("No schema - call open() first");
  if (impl_->streaming_active)
    return Result<bool>::failure("Streaming already started");

  const char* data = impl_->data_ptr;
  size_t total_size = impl_->data_size;
  size_t data_start = impl_->data_start_offset;
  size_t data_size = total_size - data_start;

  // For small files or row-limited reads, use serial path
  constexpr size_t PARALLEL_THRESHOLD = 1024 * 1024; // 1MB
  if (data_size < PARALLEL_THRESHOLD || impl_->options.max_rows >= 0) {
    auto serial_result = read_all_serial();
    if (!serial_result.ok)
      return Result<bool>::failure(serial_result.error);

    size_t num_chunks = serial_result.value.chunks.size();
    impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, 4);
    for (size_t i = 0; i < num_chunks; ++i) {
      impl_->streaming_queue->push(i, std::move(serial_result.value.chunks[i]));
    }
    impl_->streaming_active = true;
    return Result<bool>::success(true);
  }

  // Calculate chunk boundaries — simple newline scanning
  size_t n_cols = impl_->schema.size();
  size_t chunk_size = calculate_chunk_size(data_size, n_cols, impl_->num_threads);

  std::vector<std::pair<size_t, size_t>> chunk_ranges;
  size_t offset = data_start;

  while (offset < total_size) {
    size_t target_end = std::min(offset + chunk_size, total_size);
    size_t chunk_end;
    if (target_end >= total_size) {
      chunk_end = total_size;
    } else {
      chunk_end = target_end;
      while (chunk_end < total_size && data[chunk_end] != '\n')
        chunk_end++;
      if (chunk_end < total_size)
        chunk_end++; // Include the newline
    }
    chunk_ranges.emplace_back(offset, chunk_end);
    offset = chunk_end;
  }

  size_t num_chunks = chunk_ranges.size();
  if (num_chunks <= 1) {
    auto serial_result = read_all_serial();
    if (!serial_result.ok)
      return Result<bool>::failure(serial_result.error);

    size_t n = serial_result.value.chunks.size();
    impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(n, 4);
    for (size_t i = 0; i < n; ++i) {
      impl_->streaming_queue->push(i, std::move(serial_result.value.chunks[i]));
    }
    impl_->streaming_active = true;
    return Result<bool>::success(true);
  }

  // Dispatch parallel parse tasks
  size_t pool_threads = std::min(impl_->num_threads, num_chunks);
  impl_->streaming_pool = std::make_unique<BS::thread_pool>(pool_threads);
  impl_->streaming_queue = std::make_unique<ParsedChunkQueue>(num_chunks, /*max_buffered=*/4);

  const FwfOptions options = impl_->options;
  const std::vector<ColumnSchema> schema = impl_->schema;
  auto* queue_ptr = impl_->streaming_queue.get();

  for (size_t chunk_idx = 0; chunk_idx < num_chunks; ++chunk_idx) {
    size_t start_offset = chunk_ranges[chunk_idx].first;
    size_t end_offset = chunk_ranges[chunk_idx].second;

    impl_->streaming_pool->detach_task(
        [queue_ptr, data, total_size, chunk_idx, start_offset, end_offset, options, schema]() {
          if (start_offset >= total_size || end_offset > total_size || start_offset >= end_offset) {
            std::vector<std::unique_ptr<ArrowColumnBuilder>> empty;
            queue_ptr->push(chunk_idx, std::move(empty));
            return;
          }

          NullChecker null_checker(options);
          std::vector<std::unique_ptr<ArrowColumnBuilder>> columns;
          for (const auto& col_schema : schema) {
            columns.push_back(ArrowColumnBuilder::create(col_schema.type));
          }

          parse_fwf_chunk(data + start_offset, end_offset - start_offset, options, null_checker,
                          columns);

          queue_ptr->push(chunk_idx, std::move(columns));
        });
  }

  impl_->streaming_active = true;
  return Result<bool>::success(true);
}

std::optional<std::vector<std::unique_ptr<ArrowColumnBuilder>>> FwfReader::next_chunk() {
  if (!impl_->streaming_active || !impl_->streaming_queue)
    return std::nullopt;

  auto result = impl_->streaming_queue->pop();

  if (!result.has_value()) {
    impl_->streaming_pool.reset();
    impl_->streaming_queue.reset();
    impl_->streaming_active = false;
  } else if (!result->empty()) {
    impl_->row_count += (*result)[0]->size();
  }

  return result;
}

} // namespace libvroom
