#pragma once

#include <glog/logging.h>

#include <array>
#include <cstdlib>
#include <iostream>
#include <kiwi/io/file.hh>
#include <kiwi/io/iobuf.hh>
#include <kiwi/metrics/crc32.hh>
#include <kiwi/util/byte_order.hh>

namespace rosekv {

using Slice = kiwi::span<const char>;

class Segment {
  /// Defines the structure of a data chunk stored within the segment file.
  /// Each chunk includes a CRC for integrity, its length, a type indicating
  /// its position in a multi-chunk record, and the actual data.
  ///
  /// Chunk Format:
  /// ------------------------------------------------------------------------
  /// | CRC (4 bytes) | Length (2 bytes) | Type (1 byte) | Data              |
  /// ------------------------------------------------------------------------
  enum ChunkType : uint8_t {
    kFull,    ///< Represents a complete record contained within a single chunk.
    kFirst,   ///< The first chunk of a multi-chunk record.
    kMiddle,  ///< A middle chunk of a multi-chunk record.
    kLast     ///< The last chunk of a multi-chunk record.
  };

#pragma pack(push, 1)
  struct ChunkHeader {
    uint32_t crc32;
    uint16_t len;
    ChunkType type;
  };
#pragma pack(pop)

  struct Chunk {
    ChunkHeader header;
    Slice data;
  };

  static constexpr std::size_t kLenOffset = offsetof(ChunkHeader, len);
  static constexpr std::size_t kCrcOffset = offsetof(ChunkHeader, crc32);
  static constexpr std::size_t kTypeOffset = offsetof(ChunkHeader, type);

 public:
  using Offset = int64_t;

  static constexpr int kChunkHeaderSize = sizeof(ChunkHeader);
  static constexpr int kMaxBlockSize = 32768;
  static constexpr int kMaxPayLoad = kMaxBlockSize - kChunkHeaderSize;

  static int64_t ComputeRequiredSpace(Slice data) {
    // TODO(gc): add checked_cast.
    auto [nblocks, remain] = std::div(data.size(), kMaxPayLoad);

    return nblocks * kMaxBlockSize + remain +
           (remain == 0 ? 0 : kChunkHeaderSize);
  }

  /// Constructs a Segment object, opening the specified file.
  ///
  /// \note: The file is opened in a mode that allows reading, writing, and
  ///        appending, and will be created if it does not exist.
  ///
  /// \param filepath The path to the segment file.
  explicit Segment(kiwi::FilePath filepath)
      : file_{filepath, kiwi::File::kFlagOpenAlways | kiwi::File::kFlagRead |
                            kiwi::File::kFlagWrite | kiwi::File::kFlagAppend} {}

  /// Appends a data slice to the segment file as one or more chunks.
  ///
  /// \param data The data slice to append.
  /// \return The offset in the file where the first chunk of the data was
  ///         written.
  /// \note This method asserts if the segment is closed or invalid.
  Offset Append(Slice data) {
    DCHECK(!IsClosed() && IsValid());

    auto saved_offset = offset_;
    std::unique_ptr<kiwi::IOBuf> io_buf;
    auto avail = AvailableSpaceInCurrentBlock() - kChunkHeaderSize;

    if (KIWI_LIKELY(data.size() <= avail)) {
      io_buf = Encode(data, ChunkType::kFull);
    } else {
      auto [first, subspan] = data.split_at(static_cast<size_t>(avail));
      io_buf = Encode(first, ChunkType::kFirst);

      while (subspan.size() >
             AvailableSpaceInCurrentBlock() - kChunkHeaderSize) {
        auto [middle, rest] = subspan.split_at(static_cast<size_t>(
            AvailableSpaceInCurrentBlock() - kChunkHeaderSize));
        io_buf->AppendToChain(Encode(middle, ChunkType::kMiddle));
        subspan = rest;
      }

      io_buf->AppendToChain(Encode(subspan, ChunkType::kLast));
    }

    auto nbytes = file_.WriteIOBufAtCurrentPos(*io_buf.get());

    DCHECK_EQ(nbytes, io_buf->ComputeChainDataLength());

    return saved_offset;
  }

  /// Reads data starting from a specific offset in the segment file.
  ///
  /// \param offset The file offset from which to start reading.
  /// \return A `std::string` containing the reconstructed data.
  std::string ReadAt(Offset offset) {
    DCHECK(!IsClosed() && IsValid());

    std::string data;

    while (true) {
      offset = GetAlignedReadOffset(offset);

      Chunk chunk = Decode(offset);
      auto type = chunk.header.type;

      data.append(chunk.data.begin(), chunk.data.end());

      if (type == ChunkType::kLast || type == ChunkType::kFull) break;

      offset += kChunkHeaderSize + chunk.data.size();
    }

    return std::move(data);
  }

  /// Synchronizes the segment file's data to disk.
  ///
  /// \return `true` if the flush operation was successful, `false` otherwise.
  bool Sync() { return file_.Flush(); }

  /// Flushes any pending writes to disk and then closes the file handle.
  void Close() {
    Sync();
    file_.Close();
    is_closed_ = true;
  }

  /// \return `true` if the file is closed, `false` otherwise.
  constexpr bool IsClosed() const { return is_closed_; }

  /// \return `true` if the file handle is valid, `false` otherwise.
  bool IsValid() const { return file_.IsValid(); }

  /// \return A string containing details about the last file operation error.
  std::string GetErrorDetail() const {
    return kiwi::File::ErrorToString(file_.ErrorDetails());
  }

  /// \return The current size of the segment in bytes.
  constexpr std::size_t Size() const { return offset_; }

 private:
  static Offset GetAlignedReadOffset(Offset offset) {
    auto remain = kMaxBlockSize - offset % kMaxBlockSize;

    if (remain <= kChunkHeaderSize) {
      offset += remain;
    }

    return offset;
  }

  /// Encodes data and chunk type into a `kiwi::IOBuf` representing a single
  /// chunk.
  ///
  /// This helper function creates a chunk header, calculates its CRC, and
  /// wraps the data and header into an `IOBuf` chain. It does not handle
  /// block padding or offset management.
  ///
  /// \param data The data slice to encode.
  /// \param type The type of the chunk.
  /// \return A `std::unique_ptr<kiwi::IOBuf>` containing the encoded chunk.
  static std::unique_ptr<kiwi::IOBuf> EncodeDataToChunk(Slice data,
                                                        ChunkType type) {
    auto header = kiwi::IOBuf::Create(kChunkHeaderSize);
    auto ptr = header->WritableData();

    kiwi::LittleEndian::PutUint16(ptr + kLenOffset, data.size());
    ptr[kTypeOffset] = type;

    uint32_t crc = 0;
    crc = kiwi::Crc32(
        crc, kiwi::span{ptr + kLenOffset, kChunkHeaderSize - kLenOffset});
    crc = kiwi::Crc32(crc, kiwi::as_byte_span(data));
    kiwi::LittleEndian::PutUint32(ptr, crc);

    header->Append(kChunkHeaderSize);
    header->AppendToChain(kiwi::IOBuf::WrapBuffer(data.data(), data.size()));

    return header;
  }

  /// \return The number of bytes available in the current block.
  int64_t AvailableSpaceInCurrentBlock() const {
    return kMaxBlockSize - offset_ % kMaxBlockSize;
  }

  /// Encodes a data slice into one or more chunks, handling block alignment and
  /// padding.
  ///
  /// This method takes a data slice and its chunk type, creates the necessary
  /// chunk(s), and manages the `offset_` to reflect the new write position.
  /// It also inserts padding if the current write would cross a block boundary
  /// and the remaining space in the block is too small for a header.
  ///
  /// \param data The data slice to encode.
  /// \param type The type of the chunk.
  /// \return A `std::unique_ptr<kiwi::IOBuf>` representing the encoded
  ///         chunk(s), potentially including padding.
  std::unique_ptr<kiwi::IOBuf> Encode(Slice data, ChunkType type) {
    DCHECK_LE(data.size() + kChunkHeaderSize, AvailableSpaceInCurrentBlock());

    auto chunk = EncodeDataToChunk(data, type);
    offset_ += chunk->ComputeChainDataLength();
    auto sz = AvailableSpaceInCurrentBlock();

    if (sz <= kChunkHeaderSize) {
      auto padding = kiwi::IOBuf::Create(sz);
      padding->Append(sz);
      offset_ += sz;
      chunk->AppendToChain(std::move(padding));

      LOG(INFO) << "Padding size: " << sz;
    }

    return chunk;
  }

  Chunk Decode(Offset offset) {
    ChunkHeader header;
    char buf[kChunkHeaderSize];

    // Read the chunk header.
    CHECK_EQ(kChunkHeaderSize, file_.Read(offset, buf, sizeof(buf)));

    header.crc32 = kiwi::LittleEndian::Uint32(reinterpret_cast<uint8_t*>(buf));
    header.len = kiwi::LittleEndian::Uint16(
        reinterpret_cast<uint8_t*>(buf + kLenOffset));
    header.type = static_cast<ChunkType>(*(buf + kTypeOffset));

    CHECK_EQ(header.len,
             file_.Read(offset + kChunkHeaderSize, buffer_.data(), header.len))
        << " at offset: " << offset;

    auto data = kiwi::span(buffer_.data(), header.len);

    return {header, data};
  }

  std::array<char, kMaxBlockSize> buffer_;
  kiwi::File file_;
  Offset offset_ = 0;
  bool is_closed_ = false;
};

}  // namespace rosekv