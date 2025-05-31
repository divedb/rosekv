#include "rosekv/wal/wal.hh"

#include <kiwi/io/file_enumerator.hh>
#include <kiwi/io/file_util.hh>

namespace rosekv {

WAL::WAL(const Options& options) : options_{options} {
  auto path = kiwi::FilePath::FromASCII(options.wal_dir);

  if (kiwi::CreateDirectoryAndGetError(path, &error_)) {
    DLOG(INFO) << "Directory: " << path << " has existed!";
  }

  if (error_ != kiwi::File::kFileOk) {
    return;
  }

  // We only traverse the files and skip directories.
  kiwi::FileEnumerator file_iter(path, false, kiwi::FileEnumerator::kFiles);

  // Load all the segments from the disk.
  for (auto fp = file_iter.Next(); !fp.empty(); fp = file_iter.Next()) {
    auto ext = fp.Extension();

    if (ext == kDefSegFileExtension) {
      segments_.emplace(fp.BaseName(), std::make_unique<Segment>(fp));
    } else {
      LOG(INFO) << "File: " << fp << " with unsupported extension: " << ext;
    }
  }
}

void WAL::Sync() {}

void WAL::Write(Slice data, std::error_code& ec) {
  std::lock_guard<std::shared_mutex> lk_guard{wal_rw_mtx_};

  if (data.size() > options_.max_segment_sz - Segment::kChunkHeaderSize) {
    ec = rosekv::make_error_code(WALError::kTooLargeData);
    return;
  }

  auto seg = GetActiveSegment();
  auto req_space = Segment::ComputeRequiredSpace(data);

  if (req_space > options_.max_segment_sz - seg->Size()) {
    seg = NewSegment();
  }

  seg->Append(data);
  UpdateIOStat(data.size());

  if (NeedSync()) {
    seg->Sync();
  }
}

Segment* WAL::GetActiveSegment() const {
  auto it = segments_.rbegin();

  return it->second.get();
}

Segment* WAL::NewSegment() {
  kiwi::FilePath dir = kiwi::FilePath::FromASCII(options_.wal_dir);
  auto basename = std::to_string(++next_segment_id_) + kDefSegFileExtension;
  auto path = dir.Append(basename);
  segments_.emplace(std::move(basename), std::make_unique<Segment>(path));

  return GetActiveSegment();
}

void WAL::UpdateIOStat(std::size_t nbytes) {
  io_stats_.total_bytes_written += nbytes;
  io_stats_.total_write_op_count += 1;
  io_stats_.cur_bytes_written += nbytes;
  io_stats_.cur_write_op_count += 1;
}

bool WAL::NeedSync() const {
  if (options_.sync_per_write) {
    return true;
  }

  if (options_.sync_bytes_threshold != 0 &&
      io_stats_.cur_write_op_count >= options_.sync_bytes_threshold) {
    return true;
  }

  return false;
}

void WAL::StartSyncThread() {
  while (true) {
    std::unique_lock<std::mutex> lk_guard{sync_mtx_};
    sync_cv_.wait_for(lk_guard, options_.sync_interval_ms);

    if (stop_sync_thread_) {
      break;
    }

    Sync();
  }
}

}  // namespace rosekv
