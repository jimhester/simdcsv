#include "vroom/vroom.h"

#include <algorithm>
#include <cmath>
#include <cstring>
#include <limits>

namespace vroom {
namespace writer {

// Merge two statistics objects (for combining page stats into column stats)
ColumnStatistics merge_statistics(const ColumnStatistics& a, const ColumnStatistics& b) {
  ColumnStatistics result;

  result.has_null = a.has_null || b.has_null;
  result.null_count = a.null_count + b.null_count;

  // Merge min/max based on type
  auto merge_variant = [](const auto& av, const auto& bv, bool is_min) -> decltype(av) {
    // Handle monostate (no value)
    if (std::holds_alternative<std::monostate>(av))
      return bv;
    if (std::holds_alternative<std::monostate>(bv))
      return av;

    // Both have values - compare based on type
    return std::visit(
        [&](const auto& a_val) -> decltype(av) {
          using T = std::decay_t<decltype(a_val)>;
          if constexpr (std::is_same_v<T, std::monostate>) {
            return bv;
          } else {
            if (auto* b_val = std::get_if<T>(&bv)) {
              if (is_min) {
                return a_val < *b_val ? av : bv;
              } else {
                return a_val > *b_val ? av : bv;
              }
            }
            return av; // Type mismatch - keep first
          }
        },
        av);
  };

  result.min_value = merge_variant(a.min_value, b.min_value, true);
  result.max_value = merge_variant(a.max_value, b.max_value, false);

  return result;
}

// Serialize min/max value to bytes for Parquet statistics
std::vector<uint8_t> serialize_statistic(
    const std::variant<std::monostate, bool, int32_t, int64_t, double, std::string>& value,
    DataType type) {
  std::vector<uint8_t> result;

  std::visit(
      [&](const auto& v) {
        using T = std::decay_t<decltype(v)>;

        if constexpr (std::is_same_v<T, std::monostate>) {
          // No value - empty result
        } else if constexpr (std::is_same_v<T, bool>) {
          result.push_back(v ? 1 : 0);
        } else if constexpr (std::is_same_v<T, int32_t>) {
          result.resize(4);
          std::memcpy(result.data(), &v, 4);
        } else if constexpr (std::is_same_v<T, int64_t>) {
          result.resize(8);
          std::memcpy(result.data(), &v, 8);
        } else if constexpr (std::is_same_v<T, double>) {
          result.resize(8);
          std::memcpy(result.data(), &v, 8);
        } else if constexpr (std::is_same_v<T, std::string>) {
          result.assign(v.begin(), v.end());
        }
      },
      value);

  return result;
}

// Compute statistics incrementally (useful for streaming)
class StatisticsAccumulator {
public:
  explicit StatisticsAccumulator(DataType type) : type_(type) {}

  void add_int32(int32_t value, bool is_null) {
    if (is_null) {
      stats_.has_null = true;
      ++stats_.null_count;
      return;
    }

    if (!has_value_) {
      stats_.min_value = value;
      stats_.max_value = value;
      has_value_ = true;
    } else {
      auto* min_val = std::get_if<int32_t>(&stats_.min_value);
      auto* max_val = std::get_if<int32_t>(&stats_.max_value);
      if (min_val && value < *min_val)
        stats_.min_value = value;
      if (max_val && value > *max_val)
        stats_.max_value = value;
    }
  }

  void add_int64(int64_t value, bool is_null) {
    if (is_null) {
      stats_.has_null = true;
      ++stats_.null_count;
      return;
    }

    if (!has_value_) {
      stats_.min_value = value;
      stats_.max_value = value;
      has_value_ = true;
    } else {
      auto* min_val = std::get_if<int64_t>(&stats_.min_value);
      auto* max_val = std::get_if<int64_t>(&stats_.max_value);
      if (min_val && value < *min_val)
        stats_.min_value = value;
      if (max_val && value > *max_val)
        stats_.max_value = value;
    }
  }

  void add_double(double value, bool is_null) {
    if (is_null || std::isnan(value)) {
      stats_.has_null = true;
      ++stats_.null_count;
      return;
    }

    if (!has_value_) {
      stats_.min_value = value;
      stats_.max_value = value;
      has_value_ = true;
    } else {
      auto* min_val = std::get_if<double>(&stats_.min_value);
      auto* max_val = std::get_if<double>(&stats_.max_value);
      if (min_val && value < *min_val)
        stats_.min_value = value;
      if (max_val && value > *max_val)
        stats_.max_value = value;
    }
  }

  void add_string(const std::string& value, bool is_null) {
    if (is_null) {
      stats_.has_null = true;
      ++stats_.null_count;
      return;
    }

    if (!has_value_) {
      stats_.min_value = value;
      stats_.max_value = value;
      has_value_ = true;
    } else {
      auto* min_val = std::get_if<std::string>(&stats_.min_value);
      auto* max_val = std::get_if<std::string>(&stats_.max_value);
      if (min_val && value < *min_val)
        stats_.min_value = value;
      if (max_val && value > *max_val)
        stats_.max_value = value;
    }
  }

  void add_bool(bool value, bool is_null) {
    if (is_null) {
      stats_.has_null = true;
      ++stats_.null_count;
      return;
    }

    if (!has_value_) {
      stats_.min_value = value;
      stats_.max_value = value;
      has_value_ = true;
    } else {
      auto* min_val = std::get_if<bool>(&stats_.min_value);
      auto* max_val = std::get_if<bool>(&stats_.max_value);
      if (min_val && !value)
        stats_.min_value = false;
      if (max_val && value)
        stats_.max_value = true;
    }
  }

  const ColumnStatistics& statistics() const { return stats_; }

private:
  DataType type_;
  ColumnStatistics stats_;
  bool has_value_ = false;
};

} // namespace writer
} // namespace vroom
