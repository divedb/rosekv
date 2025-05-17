#include "rosekv/wal/segment.hh"

#include <gtest/gtest.h>

#include <iostream>
#include <kiwi/io/file_util.hh>

TEST(Segment, Write) {
  kiwi::ScopedTempFile temp_file;

  ASSERT_TRUE(temp_file.Create());

  std::cout << temp_file.Path() << std::endl;
}