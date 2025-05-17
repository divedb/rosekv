#pragma once

#include <glog/logging.h>

#include <array>
#include <kiwi/io/file.hh>
#include <kiwi/io/iobuf.hh>
#include <kiwi/metrics/crc32.hh>
#include <kiwi/util/byte_order.hh>

namespace rosekv {

using Slice = kiwi::span<uint8_t>;

/// Chunk Format:
/// ------------------------------------------------------------------------
/// | CRC (4 bytes) | Length (2 bytes) | Type (1 byte) | Data              |
/// ------------------------------------------------------------------------
enum ChunkType : uint8_t { kFull, kFirst, kMiddle, kLast };

#pragma pack(push, 1)
struct ChunkHeader {
  uint32_t crc32;
  uint16_t len;
  ChunkType type;
};
#pragma pack(pop)

inline constexpr std::size_t kCrcOffset = offsetof(ChunkHeader, crc32);
inline constexpr std::size_t kLenOffset = offsetof(ChunkHeader, len);
inline constexpr std::size_t kTypeOffset = offsetof(ChunkHeader, type);
inline constexpr int kChunkHeaderSize = sizeof(ChunkHeader);
inline constexpr int kMaxBlockSize = 32 * 1024 * 1024;

using Offset = int64_t;

class Segment {
 public:
  explicit Segment(FilePath filepath) : file_{filepath} {}

  Offset Write(Slice data) {
    CHECK(!IsClosed());

    constexpr int kMaxPayLoad = kMaxBlockSize - kChunkHeaderSize;

    auto ret = offset_;
    std::unique_ptr<kiwi::IOBuf> io_buf;

    if (data.size() <= kMaxPayLoad) {
      io_buf = CreateChunk(data, ChunkType::kFull);
    } else {
      auto [first, subspan] = data.split_at(kMaxPayLoad);
      io_buf = CreateChunk(first, ChunkType::kFirst);

      while (subspan.size() > kMaxPayLoad) {
        auto [middle, rest] = subspan.split_at(kMaxPayLoad);
        io_buf->AppendChain(CreateChunk(middle, ChunkType::kMiddle));
        subspan = rest;
      }

      io_buf->AppendChain(CreateChunk(subspan, ChunkType::kLast));
    }

    auto nbytes = file_.WriteIOBufAtCurrentPos(*io_buf.get());

    if (file_.Flush()) {
      offset_ += nbytes;
    }

    return ret;
  }

  void Read(ChunkRef position) const;
  void Flush();

  constexpr bool IsClosed() const { return is_closed_; }

 private:
  std::unique_ptr<kiwi::IOBuf> CreateChunk(Slice data, ChunkType type) {
    auto header = kiwi::IOBuf::Create(kChunkHeaderSize);

    kiwi::LittleEndian::PutUint16(header->data() + kLenOffset, data.size());
    header->data()[kTypeOffset] = type;

    uint32_t crc = 0;
    crc = kiwi::Crc32(crc, kiwi::span{header->data() + kLenOffset,
                                      kChunkHeaderSize - kLenOffset});
    crc = kiwi::Crc32(crc, data);
    kiwi::LittleEndian::PutUint32(header->data(), crc);
    header->AppendChain(kiwi::IOBuf::WrapBuffer(data));

    return header;
  }

  kiwi::File file_;
  Offset offset_ = 0;
  bool is_closed_ = false;
};

}  // namespace rosekv