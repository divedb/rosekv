#include "rosekv/wal/segment.hh"

#include <gtest/gtest.h>

#include <iostream>
#include <kiwi/containers/span.hh>
#include <kiwi/io/file.hh>
#include <kiwi/io/scoped_temp_file.hh>

using namespace rosekv;

TEST(Segment, WriteFullInSingleBlock) {
  kiwi::ScopedTempFile temp_file;
  ASSERT_TRUE(temp_file.Create());

  Segment segment{kiwi::FilePath::FromASCII("/tmp/b.txt")};

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

  Segment segment{kiwi::FilePath::FromASCII("/tmp/a.txt")};

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