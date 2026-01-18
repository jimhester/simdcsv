/**
 * @file libvroom.h
 * @brief libvroom - High-performance CSV parser and Parquet writer
 * @version 2.0.0
 *
 * This is the main public header for the libvroom library.
 * Migrated from libvroom2 for improved performance.
 */

#ifndef LIBVROOM_H
#define LIBVROOM_H

#define LIBVROOM_VERSION_MAJOR 2
#define LIBVROOM_VERSION_MINOR 0
#define LIBVROOM_VERSION_PATCH 0
#define LIBVROOM_VERSION_STRING "2.0.0"

// Core headers
#include "vroom/options.h"
#include "vroom/types.h"
#include "vroom/vroom.h"

// Column builders
#include "vroom/arrow_column_builder.h"

// Parsing
#include "vroom/quote_parity.h"
#include "vroom/split_fields.h"

// Statistics and dictionary
#include "vroom/dictionary.h"
#include "vroom/statistics.h"

// Output formats
#include "vroom/arrow_ipc_writer.h"

// Re-export vroom namespace as libvroom for compatibility
namespace libvroom = vroom;

#endif // LIBVROOM_H
