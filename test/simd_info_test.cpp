#include "libvroom/simd_info.h"

#include <gtest/gtest.h>
#include <string>

TEST(SimdInfo, BestTargetReturnsNonEmptyString) {
  std::string target = libvroom::simd_best_target();
  EXPECT_FALSE(target.empty());
}

TEST(SimdInfo, SupportedTargetsReturnsAtLeastOne) {
  auto targets = libvroom::simd_supported_targets();
  EXPECT_GE(targets.size(), 1u);
}

TEST(SimdInfo, BestTargetIsInSupportedList) {
  auto best = libvroom::simd_best_target();
  auto targets = libvroom::simd_supported_targets();
  bool found = false;
  for (const auto& t : targets) {
    if (t == best) {
      found = true;
      break;
    }
  }
  EXPECT_TRUE(found) << "Best target '" << best << "' not in supported list";
}
