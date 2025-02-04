package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.regionserver.BloomType;

public class RowKeyPrefixIndexedBloomFilterWriter extends CompoundBloomFilterWriter {

  private long sectionStartOffset = 0;

  public RowKeyPrefixIndexedBloomFilterWriter(int chunkByteSizeHint, float errorRate, int hashType,
    int maxFold, boolean cacheOnWrite, CellComparator comparator, BloomType bloomType) {
    super(chunkByteSizeHint, errorRate, hashType, maxFold, cacheOnWrite, comparator, bloomType);
  }

  @Override
  protected void finishInit() {
    bloomBlockIndexWriter = new RowKeyPrefixIndexedBlockIndexWriter();
  }

  @Override
  public void setSectionStartOffset(long sectionStartOffset) {
    this.sectionStartOffset = sectionStartOffset;
  }

  @Override
  public long getSectionStartOffset() {
    return sectionStartOffset;
  }

  @Override
  public void blockWritten(long offset, int onDiskSize, int uncompressedSize) {
    super.blockWritten(offset - sectionStartOffset, onDiskSize, uncompressedSize);
  }
}
