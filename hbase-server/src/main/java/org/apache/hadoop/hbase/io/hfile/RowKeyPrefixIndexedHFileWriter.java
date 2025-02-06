package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import static org.apache.hadoop.hbase.regionserver.StoreFileWriter.SingleStoreFileWriter;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.MAX_BLOCK_SIZE_UNCOMPRESSED;

public class RowKeyPrefixIndexedHFileWriter extends HFileWriterImpl {
  private static final Logger LOG = LoggerFactory.getLogger(RowKeyPrefixIndexedHFileWriter.class);

  private static final long SECTION_START_OFFSET_ON_WRITER_INIT = 0;

  private final int rowKeyPrefixLength;
  private long sectionStartOffset = SECTION_START_OFFSET_ON_WRITER_INIT;
  private boolean shouldFinishSection = false;
  private RowKeyPrefixIndexedBlockIndexWriter sectionIndexWriter;
  private byte[] sectionRowKeyPrefix = null;
  private HFileInfo sectionFileInfo = new HFileInfo();
  private boolean useSectionFileInfo = false;

  public RowKeyPrefixIndexedHFileWriter(Configuration conf, CacheConfig cacheConf, Path path,
                                        FSDataOutputStream outputStream, HFileContext fileContext,
                                        SingleStoreFileWriter singleStoreFileWriter) {
    super(conf, cacheConf, path, outputStream, fileContext, singleStoreFileWriter);
    this.rowKeyPrefixLength = fileContext.getPbePrefixLength();
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
              singleStoreFileWriter);
    }
  }

  @Override
  protected HFileInfo getFileInfo() {
    if (useSectionFileInfo) {
      return sectionFileInfo;
    }
    else {
      return fileInfo;
    }
  }

  @Override
  protected void finishInit(Configuration conf) {
    if (blockWriter != null) {
      throw new IllegalStateException("finishInit called twice");
    }
    blockWriter =
            new RowKeyPrefixIndexedBlockWriter(conf, blockEncoder, hFileContext,
                    cacheConf.getByteBuffAllocator(), conf.getInt(MAX_BLOCK_SIZE_UNCOMPRESSED,
                    hFileContext.getBlocksize() * 10));
    ((RowKeyPrefixIndexedBlockWriter) blockWriter).setSectionStartOffset(
            SECTION_START_OFFSET_ON_WRITER_INIT);
    // Data block index writer
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    dataBlockIndexWriter = new RowKeyPrefixIndexedBlockIndexWriter(blockWriter,
            cacheIndexesOnWrite ? cacheConf : null, cacheIndexesOnWrite ? name : null, indexBlockEncoder);
    dataBlockIndexWriter.setMaxChunkSize(HFileBlockIndex.getMaxChunkSize(conf));
    dataBlockIndexWriter.setMinIndexNumEntries(HFileBlockIndex.getMinIndexNumEntries(conf));
    inlineBlockWriters.add(dataBlockIndexWriter);
    for (InlineBlockWriter ibw: inlineBlockWriters) {
      ibw.setSectionStartOffset(SECTION_START_OFFSET_ON_WRITER_INIT);
    }

    // Meta data block index writer
    metaBlockIndexWriter = new RowKeyPrefixIndexedBlockIndexWriter();
    // Section index writer to store index of row key prefixes and section start offset + size
    sectionIndexWriter = new RowKeyPrefixIndexedBlockIndexWriter(blockWriter,
            cacheIndexesOnWrite ? cacheConf : null, cacheIndexesOnWrite ? name : null, indexBlockEncoder);
    LOG.trace("Initialized with {}", cacheConf);
  }

  @Override
  protected boolean checkKey(Cell cell) throws IOException {
    boolean isCellValid = super.checkKey(cell);
    int rowKeyPrefixLengthOfCurCell = CellUtil.copyRow(cell).length;
    if (rowKeyPrefixLengthOfCurCell < rowKeyPrefixLength) {
      throw new IOException("Row key length of cell is: " + rowKeyPrefixLengthOfCurCell
        + " instead of expected: " + rowKeyPrefixLength);
    }
    return isCellValid;
  }

  private boolean shouldFinishSection(Cell cell) {
    if (this.lastCell == null) {
      return false;
    }
    else if (rowKeyPrefixLength == TableDescriptorBuilder.PBE_PREFIX_LENGTH_DEFAULT) {
      // In this situation using this writer makes no sense
      return false;
    }
    return CellUtil.matchingRows(this.lastCell, (short) rowKeyPrefixLength, cell,
            (short) rowKeyPrefixLength);
  }

  @Override
  protected boolean shouldFinishBlock(Cell cell) {
    boolean finishBlock = super.shouldFinishBlock(cell);
    shouldFinishSection = shouldFinishSection(cell);
    return finishBlock || shouldFinishSection;
  }

  @Override
  protected void finishCurBlockAndStartNewBlock() throws IOException {
    finishBlock();
    writeInlineBlocks(shouldFinishSection);
    if (shouldFinishSection) {
      closeSection();
      newSection();
    }
    newBlock();
  }

  private void newSection() throws IOException {
    shouldFinishSection = false;
    sectionStartOffset = this.outputStream.getPos();
    ((RowKeyPrefixIndexedBlockWriter) blockWriter).setSectionStartOffset(sectionStartOffset);
    for (InlineBlockWriter ibw: inlineBlockWriters) {
      ibw.setSectionStartOffset(sectionStartOffset);
    }
    this.sectionFileInfo = new HFileInfo();
    sectionRowKeyPrefix = null;
  }

  @Override
  public void append(Cell cell) throws IOException {
    super.append(cell);
    if (sectionRowKeyPrefix == null) {
      sectionRowKeyPrefix = new byte[rowKeyPrefixLength];
      System.arraycopy(CellUtil.copyRow(cell), 0, sectionRowKeyPrefix, 0,
              rowKeyPrefixLength);
    }
  }

  private void closeSection() throws IOException {
    this.useSectionFileInfo = true;
    singleStoreFileWriter.closeGeneralBloomFilter();
    singleStoreFileWriter.closeDeleteFamilyBloomFilter();

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

    // File info
    trailer.setFileInfoOffset(outputStream.getPos() - sectionStartOffset);
    writeFileInfo(blockWriter.startWriting(BlockType.FILE_INFO));

    writeBloomIndexes();

    // Now finish off the trailer.
    finishTrailer(trailer);
    // Change absolute offsets to relative offsets
    trailer.setFirstDataBlockOffset(trailer.getFirstDataBlockOffset() - sectionStartOffset);
    trailer.setLastDataBlockOffset(trailer.getLastDataBlockOffset() - sectionStartOffset);

    // Now add entry in section index
    long nextSectionStartOffset = this.outputStream.getPos();
    int sizeOfCurSection = (int) (nextSectionStartOffset - sectionStartOffset);
    sectionIndexWriter.addEntry(sectionRowKeyPrefix, sectionStartOffset, sizeOfCurSection);
    this.useSectionFileInfo = false;
  }

  @Override
  public void close() {

  }
}
