package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.regionserver.StoreFileWriter.SingleStoreFileWriter;
import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.
        MAX_BLOCK_SIZE_UNCOMPRESSED;

import java.io.IOException;

@InterfaceAudience.Private
public class RowKeyPrefixIndexedHFileWriter extends HFileWriterImpl {
  private static final Logger LOG = LoggerFactory.getLogger(RowKeyPrefixIndexedHFileWriter.class);

  private static final long SECTION_START_OFFSET_ON_WRITER_INIT = 0;

  private final int rowKeyPrefixLength;

  private long sectionStartOffset = SECTION_START_OFFSET_ON_WRITER_INIT;

  private HFileBlockIndex.BlockIndexWriter sectionIndexWriter;

  private byte[] sectionRowKeyPrefix = null;

  private HFile.Writer virtualHFileWriter = null;

  private final Configuration conf;

  public RowKeyPrefixIndexedHFileWriter(Configuration conf, CacheConfig cacheConf, Path path,
                                        FSDataOutputStream outputStream, HFileContext fileContext,
                                        SingleStoreFileWriter singleStoreFileWriter,
                                        BloomType bloomType, long maxKeysInBloomFilters) throws IOException {
    super(conf, cacheConf, path, outputStream, fileContext, singleStoreFileWriter, bloomType,
            maxKeysInBloomFilters);
    this.rowKeyPrefixLength = fileContext.getPbePrefixLength();
    this.conf = conf;
    assert rowKeyPrefixLength != TableDescriptorBuilder.PBE_PREFIX_LENGTH_DEFAULT;
  }

  public static class WriterFactory extends HFile.WriterFactory {
    WriterFactory(Configuration conf, CacheConfig cacheConf) {
      super(conf, cacheConf);
    }

    @Override
    public HFile.Writer create()
      throws IOException {
      preCreate();
      return new RowKeyPrefixIndexedHFileWriter(conf, cacheConf, path, ostream, fileContext,
              singleStoreFileWriter, bloomType, maxKeysInBloomFilters);
    }
  }

  @Override
  protected void finishInit(Configuration conf, BloomType bloomType) throws IOException {
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    blockWriter = new HFileBlock.Writer(conf, blockEncoder, hFileContext,
            cacheConf.getByteBuffAllocator(), conf.getInt(MAX_BLOCK_SIZE_UNCOMPRESSED,
                    hFileContext.getBlocksize() * 10));
    virtualHFileWriter = new VirtualHFileWriter(conf, cacheConf, null, outputStream, hFileContext
            , singleStoreFileWriter, bloomType, maxKeysInBloomFilters, this);
    // Section index writer to store index of row key prefixes and section start offset + size
    sectionIndexWriter = new HFileBlockIndex.BlockIndexWriter(blockWriter,
            cacheIndexesOnWrite ? cacheConf : null, cacheIndexesOnWrite ? name : null, indexBlockEncoder);
    // To reuse finishTrailer in super class
    dataBlockIndexWriter = sectionIndexWriter;
  }

  private void newSection() throws IOException {
    sectionStartOffset = this.outputStream.getPos();
    virtualHFileWriter = new VirtualHFileWriter(conf, cacheConf, null, outputStream, hFileContext,
            singleStoreFileWriter, bloomType, maxKeysInBloomFilters, this);
    sectionRowKeyPrefix = null;
  }

  private boolean shouldFinishSection(Cell cell) {
    if (sectionRowKeyPrefix == null) {
      return false;
    }
    return Bytes.equals(sectionRowKeyPrefix, 0, rowKeyPrefixLength,
            CellUtil.copyRow(cell), 0, rowKeyPrefixLength);
  }


  private void checkCell(Cell cell) throws IOException {
    int rowKeyPrefixLengthOfCurCell = CellUtil.copyRow(cell).length;
    if (rowKeyPrefixLengthOfCurCell < rowKeyPrefixLength) {
      throw new IOException("Row key length of cell is: " + rowKeyPrefixLengthOfCurCell
              + " instead of expected: " + rowKeyPrefixLength);
    }
  }

  @Override
  public void append(Cell cell) throws IOException {
    checkCell(cell);
    if (shouldFinishSection(cell)) {
      closeSection();
      newSection();
    }
    virtualHFileWriter.append(cell);
    if (sectionRowKeyPrefix == null) {
      sectionRowKeyPrefix = new byte[rowKeyPrefixLength];
      System.arraycopy(CellUtil.copyRow(cell), 0, sectionRowKeyPrefix, 0,
              rowKeyPrefixLength);
    }
  }

  private void closeSection() throws IOException {
    virtualHFileWriter.close();

    // Now add entry in section index
    long nextSectionStartOffset = this.outputStream.getPos();
    // TODO: After compaction the size of a section can be more than INT_MAX bytes
    // Might need to introduce a new type of chunk to write block on disk size as long instead of
    // int
    int sizeOfCurSection = (int) (nextSectionStartOffset - sectionStartOffset);
    sectionIndexWriter.addEntry(sectionRowKeyPrefix, sectionStartOffset, sizeOfCurSection);
  }

  @Override
  public void close() throws IOException {
    closeSection();
    assert this.lastCell == null;

    FixedFileTrailer trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());

    long sectionIndexOffset = sectionIndexWriter.writeMultiLevelIndex(outputStream);
    trailer.setLoadOnOpenOffset(sectionIndexOffset);

    // File info
    trailer.setFileInfoOffset(outputStream.getPos());
    writeFileInfo(blockWriter.startWriting(BlockType.FILE_INFO));

    // Now finish off the trailer.
    finishTrailer(trailer);

    finishClose();
  }

  private long getSectionStartOffset() {
    return sectionStartOffset;
  }

  private void updateStats(long maxMemstoreTs, long entryCount, long totalKeyLength,
                           long totalValueLength, byte[] keyOfBiggestCell, long lenOfBiggestCell,
                           int maxTagsLength, long firstDataBlockOffset, long lastDataBlockOffset,
                           long totalUncompressedBytes) {
    this.maxMemstoreTS = Math.max(this.maxMemstoreTS, maxMemstoreTs);
    this.entryCount += entryCount;
    this.totalKeyLength += totalKeyLength;
    this.totalValueLength += totalValueLength;
    if (this.lenOfBiggestCell < lenOfBiggestCell) {
      this.lenOfBiggestCell = lenOfBiggestCell;
      this.keyOfBiggestCell = keyOfBiggestCell;
    }
    this.maxTagsLength = Math.max(this.maxTagsLength, maxTagsLength);
    if (this.firstDataBlockOffset == UNSET) {
      this.firstDataBlockOffset = firstDataBlockOffset;
    }
    this.lastDataBlockOffset = lastDataBlockOffset;
    this.totalUncompressedBytes += totalUncompressedBytes;
  }

  @InterfaceAudience.Private
  private static class VirtualHFileWriter extends HFileWriterImpl {

    private final long sectionStartOffset;
    private final RowKeyPrefixIndexedHFileWriter physicalHFileWriter;

    public VirtualHFileWriter(Configuration conf, CacheConfig cacheConf, Path path,
                              FSDataOutputStream outputStream, HFileContext fileContext,
                              SingleStoreFileWriter singleStoreFileWriter,
                              BloomType bloomType, long maxKeysInBloomFilters,
                              RowKeyPrefixIndexedHFileWriter physicalHFileWriter)
            throws IOException {
      super(conf, cacheConf, path, outputStream, fileContext, singleStoreFileWriter, bloomType,
              maxKeysInBloomFilters);
      this.physicalHFileWriter = physicalHFileWriter;
      this.sectionStartOffset = physicalHFileWriter.getSectionStartOffset();
    }

    @Override
    protected void finishInit(Configuration conf, BloomType bloomType) throws IOException {
      boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
      if (blockWriter != null) {
        throw new IllegalStateException("finishInit called twice");
      }
      blockWriter =
              new RowKeyPrefixIndexedBlockWriter(conf, blockEncoder, hFileContext,
                      cacheConf.getByteBuffAllocator(), conf.getInt(MAX_BLOCK_SIZE_UNCOMPRESSED,
                      hFileContext.getBlocksize() * 10));
      ((RowKeyPrefixIndexedBlockWriter) blockWriter).setSectionStartOffset(sectionStartOffset);
      // Data block index writer
      dataBlockIndexWriter = new RowKeyPrefixIndexedBlockIndexWriter(blockWriter,
              cacheIndexesOnWrite ? cacheConf : null, cacheIndexesOnWrite ? name : null, indexBlockEncoder);
      dataBlockIndexWriter.setMaxChunkSize(HFileBlockIndex.getMaxChunkSize(conf));
      dataBlockIndexWriter.setMinIndexNumEntries(HFileBlockIndex.getMinIndexNumEntries(conf));
      inlineBlockWriters.add(dataBlockIndexWriter);
      for (InlineBlockWriter ibw: inlineBlockWriters) {
        ibw.setSectionStartOffset(sectionStartOffset);
      }
      // Meta data block index writer
      metaBlockIndexWriter = new RowKeyPrefixIndexedBlockIndexWriter();
      initBloomFilterWriters(conf, bloomType);
      LOG.trace("Initialized with {}", cacheConf);
    }

    @Override
    public void close() throws IOException {
      boolean hasGeneralBloom = closeGeneralBloomFilter();
      boolean hasDeleteFamilyBloom = closeDeleteFamilyBloomFilter();

      // Write out the end of the data blocks, then write meta data blocks.
      // followed by data block indexes (root and intermediate), fileinfo and meta block index.
      // Then write bloom filter indexes followed by HFile trailer.

      finishDataBlockAndInlineBlocks();
      FixedFileTrailer trailer = new FixedFileTrailer(getMajorVersion(), getMinorVersion());
      writeMetadataBlocks();

      // Load-on-open section.

      // Data block index.
      //
      // In version 2, this section of the file starts with the root level data
      // block index. We call a function that writes intermediate-level blocks
      // first, then root level, and returns the offset of the root level block
      // index.

      long rootIndexOffset = dataBlockIndexWriter.writeIndexBlocks(outputStream);
      trailer.setLoadOnOpenOffset(rootIndexOffset - sectionStartOffset);

      writeMetaBlockIndex();

      physicalHFileWriter.updateStats(maxMemstoreTS, entryCount, totalKeyLength, totalValueLength,
              keyOfBiggestCell, lenOfBiggestCell, maxTagsLength, firstDataBlockOffset,
              lastDataBlockOffset, totalUncompressedBytes);
      // File info
      trailer.setFileInfoOffset(outputStream.getPos() - sectionStartOffset);
      writeFileInfo(blockWriter.startWriting(BlockType.FILE_INFO));

      writeBloomIndexes();

      // Now finish off the trailer.
      finishTrailer(trailer);
      // Change absolute offsets to relative offsets
      trailer.setFirstDataBlockOffset(trailer.getFirstDataBlockOffset() - sectionStartOffset);
      trailer.setLastDataBlockOffset(trailer.getLastDataBlockOffset() - sectionStartOffset);
      finishClose();

      // Log final Bloom filter statistics. This needs to be done after close()
      // because compound Bloom filters might be finalized as part of closing.
      if (LOG.isTraceEnabled()) {
        LOG.trace(
                (hasGeneralBloom ? "" : "NO ") + "General Bloom and " + (hasDeleteFamilyBloom ? "" : "NO ")
                        + "DeleteFamily" + " was added to HFile " + getPath());
      }
    }
  }
}
