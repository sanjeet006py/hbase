package org.apache.hadoop.hbase.io.hfile;

public class RowKeyPrefixIndexedBlockIndexWriter extends HFileBlockIndex.BlockIndexWriter {

  private long sectionStartOffset = 0;

  public RowKeyPrefixIndexedBlockIndexWriter() {
  }

  public RowKeyPrefixIndexedBlockIndexWriter(HFileBlock.Writer blockWriter, CacheConfig cacheConf,
    String nameForCaching, HFileIndexBlockEncoder indexBlockEncoder) {
    super(blockWriter, cacheConf, nameForCaching, indexBlockEncoder);
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
  public void addEntry(byte[] firstKey, long blockOffset, int blockDataSize) {
    super.addEntry(firstKey, blockOffset - sectionStartOffset, blockDataSize);
  }

  @Override
  public void blockWritten(long offset, int onDiskSize, int uncompressedSize) {
    super.blockWritten(offset - sectionStartOffset, onDiskSize, uncompressedSize);
  }
}
