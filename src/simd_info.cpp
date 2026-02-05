#include "libvroom/simd_info.h"

#include "hwy/targets.h"

#include <algorithm>

namespace libvroom {

std::string simd_best_target() {
  int64_t targets = hwy::SupportedTargets();
  int64_t best = 0;
  while (targets != 0) {
    best = targets;
    targets &= targets - 1; // clear lowest set bit
  }
  // best now holds only the highest set bit
  return hwy::TargetName(best);
}

std::vector<std::string> simd_supported_targets() {
  std::vector<std::string> result;
  int64_t targets = hwy::SupportedTargets();
  while (targets != 0) {
    int64_t lowest = targets & -targets; // isolate lowest set bit
    result.push_back(hwy::TargetName(lowest));
    targets &= targets - 1; // clear lowest set bit
  }
  // Reverse so best target is first
  std::reverse(result.begin(), result.end());
  return result;
}

} // namespace libvroom
