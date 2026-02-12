#pragma once

#include <array>
#include <cctype>
#include <cstdint>
#include <memory>
#include <string>
#include <string_view>

namespace libvroom {

// Locale data for format-string datetime parsing (month/day names, AM/PM)
struct FormatLocale {
  std::array<std::string, 12> month_names;  // Full: January..December
  std::array<std::string, 12> month_abbrev; // Abbreviated: Jan..Dec
  std::array<std::string, 7> day_names;     // Full: Sunday..Saturday
  std::array<std::string, 7> day_abbrev;    // Abbreviated: Sun..Sat
  std::string am = "AM";
  std::string pm = "PM";
  std::string date_format = "%Y-%m-%d"; // Used by %x
  std::string time_format = "%H:%M:%S"; // Used by %X

  static FormatLocale english();
};

// Intermediate result of format-string parsing
struct ParsedDateTime {
  int year = 1970;
  int month = 1;                   // 1-12
  int day = 1;                     // 1-31
  int hour = 0;                    // 0-23 (or unlimited for durations via %h)
  int minute = 0;                  // 0-59
  int second = 0;                  // 0-59
  double fractional_seconds = 0.0; // 0.0 - 0.999999
  int tz_offset_minutes = 0;       // Timezone offset in minutes from UTC
  bool is_negative = false;        // For negative durations (e.g., "-1:23:45")

  // Convert to days since Unix epoch (1970-01-01)
  int32_t to_epoch_days() const;

  // Convert to microseconds since Unix epoch (UTC)
  int64_t to_epoch_micros() const;

  // Convert to microseconds since midnight (for TIME / durations)
  int64_t to_seconds_since_midnight_micros() const;
};

// Format-string datetime parser (strptime-compatible)
// Thread-safe after construction — parse() is const.
class FormatParser {
public:
  // Construct with a format string and locale.
  // Format specifiers: %Y %y %m %d %e %b %B %a %A %H %h %I %M %S %OS %p %z %Z
  // %% %D %F %T %R %x %X %. %AD %AT
  FormatParser(std::string_view format, const FormatLocale& locale);

  // Parse a value according to the format string.
  // Returns true on success, populating dt. On failure, dt is left in a partial
  // state.
  bool parse(std::string_view value, ParsedDateTime& dt) const;

  // Get the format string
  const std::string& format() const { return format_; }

private:
  std::string format_;
  FormatLocale locale_;
};

} // namespace libvroom
