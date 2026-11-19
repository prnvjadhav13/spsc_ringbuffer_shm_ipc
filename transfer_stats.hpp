#ifndef TRANSFER_STATS_HPP
#define TRANSFER_STATS_HPP

#include <array>
#include <cstddef>
#include <cstdint>
#include <iomanip>
#include <sstream>
#include <string>
#include <string_view>

inline std::string format_byte_size(std::uint64_t bytes) {
  constexpr std::array<std::string_view, 7> units{
      "B", "kB", "MB", "GB", "TB", "PB", "EB"};
  double value = static_cast<double>(bytes);
  std::size_t unit = 0;
  while (value >= 1000.0 && unit + 1 < units.size()) {
    value /= 1000.0;
    ++unit;
  }

  std::ostringstream formatted;
  formatted << std::fixed << std::setprecision(2) << value << ' ' << units[unit];
  return formatted.str();
}

inline std::string format_byte_rate(double bytes_per_second) {
  constexpr std::array<std::string_view, 7> units{
      "B/s", "kB/s", "MB/s", "GB/s", "TB/s", "PB/s", "EB/s"};
  std::size_t unit = 0;
  while (bytes_per_second >= 1000.0 && unit + 1 < units.size()) {
    bytes_per_second /= 1000.0;
    ++unit;
  }

  std::ostringstream formatted;
  formatted << std::fixed << std::setprecision(1) << bytes_per_second << ' ' << units[unit];
  return formatted.str();
}

#endif
