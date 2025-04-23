Segment Structure

For improving read efficiency, the log file is partitioned into blocks, with each block having a size of 32KB. Each block comprises several complete chunks.

A single log record consists of one or more chunks. Each chunk includes a 7-byte header: the first 4 bytes are the checksum for that chunk, followed by 2 bytes for the length of the chunk's data, and the last byte for the chunk's type. The scope of the checksum calculation includes the chunk's type and the subsequent data.

There are 4 types of chunks: 
- FULL
- FIRST
- MIDDLE
- LAST

If a log record consists of only one chunk, that chunk's type is 'FULL'. If a log record comprises multiple chunks, the first chunk among them is of type 'FIRST', the last one is of type 'LAST', and the ones in between (zero or more) are of type 'MIDDLE'.

Due to the block size of 32KB, when a single log record is too large to fit within a single block, it will be split into multiple chunks. The first chunk (containing the initial part of the data) will be written into the first block it occupies, and its type will be 'first'. If the remaining data still exceeds a block's size, the subsequent chunks will be written into further blocks, and these chunks will be of type 'middle'. Finally, the last chunk containing the remaining data for the record will be written into the last block it occupies, and its type will be 'last'.