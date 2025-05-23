#include "rosekv/wal/segment.hh"

#include <gtest/gtest.h>

#include <iostream>
#include <kiwi/containers/span.hh>
#include <kiwi/io/file.hh>
#include <kiwi/io/scoped_temp_file.hh>
#include <random>

using namespace rosekv;

TEST(Segment, WriteFullInSingleBlock) {
  kiwi::ScopedTempFile temp_file;

  ASSERT_TRUE(temp_file.Create());

  Segment segment{temp_file.Path()};

  EXPECT_TRUE(segment.IsValid());
  EXPECT_FALSE(segment.IsClosed());

  constexpr std::string_view kTestData = "hello";
  constexpr int kNumIterations = 100;

  // Tests writing multiple small records that fit entirely within a single
  // block. Verifies that each record is correctly appended and can be read
  // back.
  for (int i = 0; i < kNumIterations; ++i) {
    const auto offset = segment.Append(kiwi::span(kTestData));
    const std::string data = segment.ReadAt(offset);

    EXPECT_EQ(kTestData, data) << "Failed at iteration: " << i;
  }
}

TEST(Segment, WriteFullInMultipleBlocks) {
  kiwi::ScopedTempFile temp_file;

  ASSERT_TRUE(temp_file.Create());

  Segment segment{temp_file.Path()};

  EXPECT_TRUE(segment.IsValid());
  EXPECT_FALSE(segment.IsClosed());

  // Tests writing enough small records to span multiple blocks.
  // Verifies that records are correctly stored across block boundaries.
  constexpr std::string_view kTestData = "world";
  constexpr int kChunkSize = kTestData.size() + Segment::kChunkHeaderSize;
  constexpr int kChunksPerBlock = Segment::kMaxBlockSize / kChunkSize;

  // Write one more record than fits in a single block to force a new block.
  constexpr int kNumIterations = kChunksPerBlock + 1;

  for (int i = 0; i < kNumIterations; ++i) {
    const auto offset = segment.Append(kiwi::span(kTestData));
    const std::string data = segment.ReadAt(offset);

    EXPECT_EQ(kTestData, data) << "Failed at iteration: " << i;
  }
}

TEST(Segment, WriteLargeData) {
  kiwi::ScopedTempFile temp_file;

  ASSERT_TRUE(temp_file.Create());

  Segment segment{temp_file.Path()};

  EXPECT_TRUE(segment.IsValid());
  EXPECT_FALSE(segment.IsClosed());

  std::string expect(Segment::kMaxBlockSize * 3, 'S');

  auto offset =
      segment.Append(kiwi::span(static_cast<std::string_view>(expect)));
  auto actual = segment.ReadAt(offset);

  EXPECT_EQ(expect, actual);
}

static std::string GenerateRandomString(size_t n) {
  static std::mt19937 rng{42};
  static std::uniform_int_distribution<size_t> len_dist(1, n);
  static std::uniform_int_distribution<int> char_dist(32, 126);

  size_t len = len_dist(rng);
  std::string s(len, '\0');

  for (size_t j = 0; j < len; ++j) {
    s[j] = char_dist(rng);
  }

  return s;
}

TEST(Segment, WriteRandomData) {
  kiwi::ScopedTempFile temp_file;
  ASSERT_TRUE(temp_file.Create());

  Segment segment{temp_file.Path()};

  EXPECT_TRUE(segment.IsValid());
  EXPECT_FALSE(segment.IsClosed());

  struct Record {
    int i;
    std::string data;
    Segment::Offset offset;
  };

  constexpr int kNum = 10000;
  std::vector<Record> records;
  records.reserve(kNum);

  for (int i = 0; i < kNum; ++i) {
    auto s = GenerateRandomString(4096);
    auto offset = segment.Append(kiwi::span(static_cast<std::string_view>(s)));
    records.push_back({i, std::move(s), offset});
  }

  for (const auto& record : records) {
    auto read_back = segment.ReadAt(record.offset);

    EXPECT_EQ(read_back, record.data)
        << " at iteration: " << record.i << ": offset: " << record.offset;
  }
}