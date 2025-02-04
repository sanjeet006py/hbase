package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;

public class RowKeyPrefixIndexedBlockWriter extends HFileBlock.Writer {

  private long sectionStartOffset = 0;

  public RowKeyPrefixIndexedBlockWriter(Configuration conf, HFileDataBlockEncoder dataBlockEncoder,
    HFileContext fileContext, ByteBuffAllocator allocator, int maxSizeUnCompressed) {
    super(conf, dataBlockEncoder, fileContext, allocator, maxSizeUnCompressed);
  }

  public void setSectionStartOffset(long sectionStartOffset) {
    this.sectionStartOffset = sectionStartOffset;
  }

  public long getSectionStartOffset() {
    return sectionStartOffset;
  }
}
