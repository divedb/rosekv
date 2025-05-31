#pragma once

#include <chrono>
#include <string>

namespace rosekv {

inline constexpr const char* kDefSegFileExtension = ".seg";

struct Options {
  /// This is the base path where WAL segment files will be written and loaded
  /// from.
  std::string wal_dir;

  /// The file extension used for segment files (e.g. ".seg").
  std::string file_extension_ = kDefSegFileExtension;

  /// The maximum allowed size for a single segment file, in bytes.
  int64_t max_segment_sz = 64 * 1024 * 1024;

  /// The number of bytes written before triggering a sync operation.
  /// If zero, syncing is disabled based on byte thresholds.
  int64_t sync_bytes_threshold = 0;

  /// The interval between background syncs, if enabled.
  /// If set to 0ms, periodic syncing is disabled.
  std::chrono::milliseconds sync_interval_ms = std::chrono::milliseconds(0);

  /// Whether to call Sync after every write.
  /// Useful for durability guarantees, but may hurt performance.
  bool sync_per_write = false;

  /// Whether to enable compression for completed WAL segments.
  /// Only cold (read-only) segments will be compressed.
  bool compression_enabled = false;

  /// Whether to enable verbose logging for debugging purposes.
  bool verbose_logging = false;
};

}  // namespace rosekv