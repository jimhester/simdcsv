#ifndef LIBVROOM_SIMD_INFO_H
#define LIBVROOM_SIMD_INFO_H

#include <string>
#include <vector>

namespace libvroom {

/// Returns the name of the best SIMD target selected by Highway at runtime.
/// Examples: "AVX2", "AVX3", "AVX3_DL", "AVX3_ZEN4", "NEON", "SCALAR"
std::string simd_best_target();

/// Returns names of all SIMD targets supported by this CPU.
std::vector<std::string> simd_supported_targets();

} // namespace libvroom

#endif // LIBVROOM_SIMD_INFO_H
