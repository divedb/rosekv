#pragma once

#include <algorithm>
#include <cassert>
#include <vector>

#include "kiwi/containers/span.hh"
#include "kiwi/metrics/crc32.hh"
#include "kiwi/util/byte_order.hh"

namespace rosekv {

using Slice = kiwi::span<uint8_t>;

/// Chunk Format:
/// ------------------------------------------------------------------------
/// | CRC (4 bytes) | Length (2 bytes) | Type (1 byte) | Data              |
/// ------------------------------------------------------------------------
enum ChunkType : uint8_t { kFull, kFirst, kMiddle, kLast };

struct ChunkHeader {
  uint32_t crc32;
  uint16_t len;
  ChunkType type;
};

inline constexpr int kChunkHeaderSize = sizeof(ChunkHeader);
inline constexpr int kMaxBlockSize = 32 * 1024 * 1024;

class Chunk {
 public:
  Chunk(ChunkType type, PayLoad data) : type_{type}, data_{data} {}

  constexpr ChunkType Type() const { return type_; }
  constexpr PayLoad Data() const { return data_; }

  /// Write the chunk header together with the payload into the buffer.
  ///
  /// @param buffer
  uint8_t* WriteTo(uint8_t* buffer) const {
    uint8_t* first = buffer + sizeof(uint32_t);
    uint8_t* last = first;

    last = kiwi::LittleEndian::AppendUint16(
        last, static_cast<uint16_t>(data_.size()));
    last = kiwi::LittleEndian::AppendUint8(last, type_);
  }

 private:
  //   uint32_t CheckSum() const {
  //     uint32_t crc = 0;
  //     uint16_t len = data_.size();

  //     crc = kiwi::Crc32(crc, );
  //   }

  ChunkType type_;
  PayLoad data_;
};

class Block {
 public:
  static constexpr int kMaxSize = 32 * 1024 * 1024;

  auto begin() noexcept { return chunks_.begin(); }
  auto begin() const noexcept { return chunks_.begin(); }
  auto end() noexcept { return chunks_.end(); }
  auto end() const noexcept { return chunks_.end(); }

  constexpr bool IsFull() const { return AvailableSpace() == 0; }
  constexpr int AvailableSpace() const { return kMaxSize - used_space_; }

  void Append(Chunk chunk) {
    // The number of bytes required to store the chunk.
    auto size = kChunkHeaderSize + chunk.Data().size();

    assert(AvailableSpace() >= size);

    chunks_.emplace_back(chunk);
    used_space_ += size;
  }

 private:
  int used_space_ = 0;
  std::vector<Chunk> chunks_;
};

}  // namespace rosekv