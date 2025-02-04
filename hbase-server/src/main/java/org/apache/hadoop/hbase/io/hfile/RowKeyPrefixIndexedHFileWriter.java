package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

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

  public RowKeyPrefixIndexedHFileWriter(Configuration conf, CacheConfig cacheConf, Path path,
    FSDataOutputStream outputStream, HFileContext fileContext) {
    super(conf, cacheConf, path, outputStream, fileContext);
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
      return new RowKeyPrefixIndexedHFileWriter(conf, cacheConf, path, ostream, fileContext);
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
      close();
    }
    newBlock();
  }
}
