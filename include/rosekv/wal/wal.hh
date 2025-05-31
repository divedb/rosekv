#pragma once

#include <condition_variable>
#include <kiwi/common/error_or.hh>
#include <map>
#include <mutex>
#include <shared_mutex>

#include "rosekv/wal/error_code.hh"
#include "rosekv/wal/options.hh"
#include "rosekv/wal/segment.hh"

namespace rosekv {

class WAL {
  struct IOStats {
    /// The number of bytes successfully written.
    int64_t total_bytes_written;
    /// The number of write operations performed.
    int64_t total_write_op_count;

    int64_t cur_bytes_written;
    int64_t cur_write_op_count;

    /// The number of times data was flushed or synced.
    int64_t sync_op_count;
  };

 public:
  explicit WAL(const Options& options);

  /// Synchronize the data to the disk.
  void Sync();

  void Write(Slice data, std::error_code& ec);

 private:
  Segment* GetActiveSegment() const;
  Segment* NewSegment();
  void UpdateIOStat(std::size_t nbytes);
  bool NeedSync() const;
  void StartSyncThread();

  Options options_;
  std::map<std::string, std::unique_ptr<Segment>> segments_;
  IOStats io_stats_;
  kiwi::File::Error error_;

  std::shared_mutex wal_rw_mtx_;
  int next_segment_id_;

  bool stop_sync_thread_;
  std::mutex sync_mtx_;
  std::condition_variable sync_cv_;
};

}  // namespace rosekv