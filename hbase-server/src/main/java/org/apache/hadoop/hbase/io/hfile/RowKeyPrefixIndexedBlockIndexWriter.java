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
  protected BlockIndexChunk newChunk() {
    return new RowKeyPrefixIndexedBlockIndexChunk(this);
  }

  @Override
  public void setSectionStartOffset(long sectionStartOffset) {
    this.sectionStartOffset = sectionStartOffset;
  }

  @Override
  public long getSectionStartOffset() {
    return sectionStartOffset;
  }

  static class RowKeyPrefixIndexedBlockIndexChunk extends HFileBlockIndex.BlockIndexChunkImpl {

    private final RowKeyPrefixIndexedBlockIndexWriter indexWriter;

    public RowKeyPrefixIndexedBlockIndexChunk(RowKeyPrefixIndexedBlockIndexWriter indexWriter) {
      super();
      this.indexWriter = indexWriter;
    }

    @Override
    public void add(byte[] firstKey, long blockOffset, int onDiskDataSize,
      long curTotalNumSubEntries) {
      super.add(firstKey, blockOffset - indexWriter.getSectionStartOffset(),
        onDiskDataSize);
    }
  }
}
