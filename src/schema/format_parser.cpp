#include "libvroom/format_parser.h"

#include <algorithm>
#include <cctype>

namespace libvroom {

FormatLocale FormatLocale::english() {
  FormatLocale loc;
  loc.month_names = {"January", "February", "March",     "April",   "May",      "June",
                     "July",    "August",   "September", "October", "November", "December"};
  loc.month_abbrev = {"Jan", "Feb", "Mar", "Apr", "May", "Jun",
                      "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"};
  loc.day_names = {"Sunday", "Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday"};
  loc.day_abbrev = {"Sun", "Mon", "Tue", "Wed", "Thu", "Fri", "Sat"};
  loc.am = "AM";
  loc.pm = "PM";
  return loc;
}

// Leap year / epoch math (same formulas as type_parsers.cpp)
static inline bool is_leap_year(int year) {
  return (year % 4 == 0 && year % 100 != 0) || (year % 400 == 0);
}

static const int days_in_month_table[] = {0, 31, 28, 31, 30, 31, 30, 31, 31, 30, 31, 30, 31};

static inline int get_days_in_month(int year, int month) {
  if (month == 2 && is_leap_year(year))
    return 29;
  return days_in_month_table[month];
}

static inline int leap_years_before(int year) {
  if (year <= 1)
    return 0;
  int y = year - 1;
  return y / 4 - y / 100 + y / 400;
}

static inline int32_t days_from_epoch_to_year(int year) {
  constexpr int LEAP_YEARS_BEFORE_1970 = 477;
  int leap_years_diff = leap_years_before(year) - LEAP_YEARS_BEFORE_1970;
  return static_cast<int32_t>(year - 1970) * 365 + leap_years_diff;
}

int32_t ParsedDateTime::to_epoch_days() const {
  int32_t days = days_from_epoch_to_year(year);
  for (int m = 1; m < month; ++m)
    days += get_days_in_month(year, m);
  days += day - 1;
  return days;
}

int64_t ParsedDateTime::to_epoch_micros() const {
  int64_t days = to_epoch_days();
  int64_t micros = days * 86400LL * 1000000LL + static_cast<int64_t>(hour) * 3600LL * 1000000LL +
                   static_cast<int64_t>(minute) * 60LL * 1000000LL +
                   static_cast<int64_t>(second) * 1000000LL +
                   static_cast<int64_t>(fractional_seconds * 1000000.0);
  micros -= static_cast<int64_t>(tz_offset_minutes) * 60LL * 1000000LL;
  return micros;
}

int64_t ParsedDateTime::to_seconds_since_midnight_micros() const {
  return static_cast<int64_t>(hour) * 3600LL * 1000000LL +
         static_cast<int64_t>(minute) * 60LL * 1000000LL +
         static_cast<int64_t>(second) * 1000000LL +
         static_cast<int64_t>(fractional_seconds * 1000000.0);
}

// Case-insensitive string prefix match. Returns length matched or 0.
static size_t match_string_ci(const char* pos, const char* end, const std::string& target) {
  size_t len = target.size();
  if (static_cast<size_t>(end - pos) < len)
    return 0;
  for (size_t i = 0; i < len; ++i) {
    if (std::tolower(static_cast<unsigned char>(pos[i])) !=
        std::tolower(static_cast<unsigned char>(target[i])))
      return 0;
  }
  return len;
}

// Parse up to max_digits digits into result. Returns number of digits parsed.
static int parse_digits(const char*& pos, const char* end, int max_digits, int& result) {
  result = 0;
  int count = 0;
  while (count < max_digits && pos < end && *pos >= '0' && *pos <= '9') {
    result = result * 10 + (*pos - '0');
    pos++;
    count++;
  }
  return count;
}

FormatParser::FormatParser(std::string_view format, const FormatLocale& locale)
    : format_(format), locale_(locale) {}

bool FormatParser::parse(std::string_view value, ParsedDateTime& dt) const {
  dt = ParsedDateTime{};
  const char* pos = value.data();
  const char* end = value.data() + value.size();
  const char* fmt = format_.data();
  const char* fmt_end = format_.data() + format_.size();
  int am_pm = -1; // -1 = not set, 0 = AM, 1 = PM

  while (fmt < fmt_end) {
    if (pos > end)
      return false;

    // Whitespace in format matches zero or more whitespace in input
    if (std::isspace(static_cast<unsigned char>(*fmt))) {
      while (pos < end && std::isspace(static_cast<unsigned char>(*pos)))
        pos++;
      fmt++;
      continue;
    }

    if (*fmt != '%') {
      if (pos >= end || *pos != *fmt)
        return false;
      pos++;
      fmt++;
      continue;
    }

    fmt++; // skip '%'
    if (fmt >= fmt_end)
      return false;

    char spec = *fmt++;

    switch (spec) {
    case 'Y': {
      int val;
      if (parse_digits(pos, end, 4, val) != 4)
        return false;
      dt.year = val;
      break;
    }
    case 'y': {
      int val;
      if (parse_digits(pos, end, 2, val) != 2)
        return false;
      dt.year = val < 69 ? 2000 + val : 1900 + val;
      break;
    }
    case 'm': {
      int val;
      if (parse_digits(pos, end, 2, val) == 0)
        return false;
      dt.month = val;
      break;
    }
    case 'd': {
      int val;
      if (parse_digits(pos, end, 2, val) == 0)
        return false;
      dt.day = val;
      break;
    }
    case 'e': {
      if (pos < end && *pos == ' ')
        pos++;
      int val;
      if (parse_digits(pos, end, 2, val) == 0)
        return false;
      dt.day = val;
      break;
    }
    case 'H': {
      int val;
      if (parse_digits(pos, end, 2, val) == 0 || val > 23)
        return false;
      dt.hour = val;
      break;
    }
    case 'I': {
      int val;
      if (parse_digits(pos, end, 2, val) == 0 || val < 1 || val > 12)
        return false;
      dt.hour = val % 12;
      break;
    }
    case 'M': {
      int val;
      if (parse_digits(pos, end, 2, val) == 0 || val > 59)
        return false;
      dt.minute = val;
      break;
    }
    case 'S': {
      int val;
      if (parse_digits(pos, end, 2, val) == 0 || val > 59)
        return false;
      dt.second = val;
      break;
    }
    case 'O': {
      if (fmt >= fmt_end || *fmt != 'S')
        return false;
      fmt++;
      int val;
      if (parse_digits(pos, end, 2, val) == 0 || val > 59)
        return false;
      dt.second = val;
      if (pos < end && *pos == '.') {
        pos++;
        double frac = 0.0;
        double place = 0.1;
        int frac_digits = 0;
        while (pos < end && *pos >= '0' && *pos <= '9' && frac_digits < 6) {
          frac += (*pos - '0') * place;
          place *= 0.1;
          pos++;
          frac_digits++;
        }
        // Consume any remaining fractional digits beyond 6
        while (pos < end && *pos >= '0' && *pos <= '9')
          pos++;
        dt.fractional_seconds = frac;
      }
      break;
    }
    case 'p': {
      size_t am_len = match_string_ci(pos, end, locale_.am);
      if (am_len > 0) {
        am_pm = 0;
        pos += am_len;
        break;
      }
      size_t pm_len = match_string_ci(pos, end, locale_.pm);
      if (pm_len > 0) {
        am_pm = 1;
        pos += pm_len;
        break;
      }
      return false;
    }
    case 'b': {
      bool found = false;
      for (int i = 0; i < 12; ++i) {
        size_t len = match_string_ci(pos, end, locale_.month_abbrev[i]);
        if (len > 0) {
          dt.month = i + 1;
          pos += len;
          found = true;
          break;
        }
      }
      if (!found)
        return false;
      break;
    }
    case 'B': {
      bool found = false;
      for (int i = 0; i < 12; ++i) {
        size_t len = match_string_ci(pos, end, locale_.month_names[i]);
        if (len > 0) {
          dt.month = i + 1;
          pos += len;
          found = true;
          break;
        }
      }
      if (!found)
        return false;
      break;
    }
    case 'a': {
      bool found = false;
      for (int i = 0; i < 7; ++i) {
        size_t len = match_string_ci(pos, end, locale_.day_abbrev[i]);
        if (len > 0) {
          pos += len;
          found = true;
          break;
        }
      }
      if (!found)
        return false;
      break;
    }
    case 'A': {
      bool found = false;
      for (int i = 0; i < 7; ++i) {
        size_t len = match_string_ci(pos, end, locale_.day_names[i]);
        if (len > 0) {
          pos += len;
          found = true;
          break;
        }
      }
      if (!found)
        return false;
      break;
    }
    case 'z': {
      if (pos < end && *pos == 'Z') {
        dt.tz_offset_minutes = 0;
        pos++;
        break;
      }
      if (pos >= end || (*pos != '+' && *pos != '-'))
        return false;
      bool neg = (*pos == '-');
      pos++;
      int tz_hour;
      if (parse_digits(pos, end, 2, tz_hour) != 2)
        return false;
      if (pos < end && *pos == ':')
        pos++;
      int tz_min = 0;
      if (pos < end && *pos >= '0' && *pos <= '9') {
        if (parse_digits(pos, end, 2, tz_min) != 2)
          return false;
      }
      dt.tz_offset_minutes = tz_hour * 60 + tz_min;
      if (neg)
        dt.tz_offset_minutes = -dt.tz_offset_minutes;
      break;
    }
    case 'Z': {
      // Named timezone — consume non-whitespace characters (not interpreted)
      while (pos < end && !std::isspace(static_cast<unsigned char>(*pos)))
        pos++;
      break;
    }
    case '%': {
      if (pos >= end || *pos != '%')
        return false;
      pos++;
      break;
    }
    case 'D': {
      int month_val;
      if (parse_digits(pos, end, 2, month_val) == 0)
        return false;
      dt.month = month_val;
      if (pos >= end || *pos != '/')
        return false;
      pos++;
      int day_val;
      if (parse_digits(pos, end, 2, day_val) == 0)
        return false;
      dt.day = day_val;
      if (pos >= end || *pos != '/')
        return false;
      pos++;
      int year_val;
      if (parse_digits(pos, end, 2, year_val) != 2)
        return false;
      dt.year = year_val < 69 ? 2000 + year_val : 1900 + year_val;
      break;
    }
    case 'F': {
      int year_val;
      if (parse_digits(pos, end, 4, year_val) != 4)
        return false;
      dt.year = year_val;
      if (pos >= end || *pos != '-')
        return false;
      pos++;
      int month_val;
      if (parse_digits(pos, end, 2, month_val) == 0)
        return false;
      dt.month = month_val;
      if (pos >= end || *pos != '-')
        return false;
      pos++;
      int day_val;
      if (parse_digits(pos, end, 2, day_val) == 0)
        return false;
      dt.day = day_val;
      break;
    }
    case 'T': {
      int h, m, s;
      if (parse_digits(pos, end, 2, h) == 0 || h > 23)
        return false;
      if (pos >= end || *pos != ':')
        return false;
      pos++;
      if (parse_digits(pos, end, 2, m) == 0 || m > 59)
        return false;
      if (pos >= end || *pos != ':')
        return false;
      pos++;
      if (parse_digits(pos, end, 2, s) == 0 || s > 59)
        return false;
      dt.hour = h;
      dt.minute = m;
      dt.second = s;
      break;
    }
    case 'R': {
      int h, m;
      if (parse_digits(pos, end, 2, h) == 0 || h > 23)
        return false;
      if (pos >= end || *pos != ':')
        return false;
      pos++;
      if (parse_digits(pos, end, 2, m) == 0 || m > 59)
        return false;
      dt.hour = h;
      dt.minute = m;
      break;
    }
    default:
      return false;
    }
  }

  // Apply AM/PM
  if (am_pm == 1) {
    if (dt.hour != 12)
      dt.hour += 12;
  } else if (am_pm == 0) {
    if (dt.hour == 12)
      dt.hour = 0;
  }

  // Must consume all input
  if (pos != end)
    return false;

  // Validate date components
  if (dt.month < 1 || dt.month > 12)
    return false;
  if (dt.day < 1 || dt.day > get_days_in_month(dt.year, dt.month))
    return false;

  return true;
}

} // namespace libvroom
