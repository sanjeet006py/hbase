/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.io.hfile;

import static org.apache.hadoop.hbase.io.hfile.BlockCompressedSizePredicator.MAX_BLOCK_SIZE_UNCOMPRESSED;

import java.io.DataOutput;
import java.io.DataOutputStream;
import java.io.IOException;
import java.net.InetSocketAddress;
import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.List;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.ByteBufferExtendedCell;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellComparator;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.KeyValueUtil;
import org.apache.hadoop.hbase.MetaCellComparator;
import org.apache.hadoop.hbase.PrivateCellUtil;
import org.apache.hadoop.hbase.io.compress.Compression;
import org.apache.hadoop.hbase.io.crypto.Encryption;
import org.apache.hadoop.hbase.io.encoding.DataBlockEncoding;
import org.apache.hadoop.hbase.io.encoding.IndexBlockEncoding;
import org.apache.hadoop.hbase.io.hfile.HFileBlock.BlockWritable;
import org.apache.hadoop.hbase.regionserver.BloomType;
import org.apache.hadoop.hbase.security.EncryptionUtil;
import org.apache.hadoop.hbase.security.User;
import org.apache.hadoop.hbase.util.BloomContext;
import org.apache.hadoop.hbase.util.BloomFilterFactory;
import org.apache.hadoop.hbase.util.BloomFilterUtil;
import org.apache.hadoop.hbase.util.BloomFilterWriter;
import org.apache.hadoop.hbase.util.ByteBufferUtils;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.hbase.util.CommonFSUtils;
import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;
import org.apache.hadoop.hbase.util.FSUtils;
import org.apache.hadoop.hbase.util.RowBloomContext;
import org.apache.hadoop.hbase.util.RowColBloomContext;
import org.apache.hadoop.hbase.util.RowPrefixFixedLengthBloomContext;
import org.apache.hadoop.io.Writable;
import org.apache.yetus.audience.InterfaceAudience;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_PARAM_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.BLOOM_FILTER_TYPE_KEY;
import static org.apache.hadoop.hbase.regionserver.HStoreFile.DELETE_FAMILY_COUNT;
import static org.apache.hadoop.hbase.regionserver.StoreFileWriter.SingleStoreFileWriter;

/**
 * Common functionality needed by all versions of {@link HFile} writers.
 */
@InterfaceAudience.Private
public class HFileWriterImpl implements HFile.Writer {
  private static final Logger LOG = LoggerFactory.getLogger(HFileWriterImpl.class);

  protected static final long UNSET = -1;

  /** if this feature is enabled, preCalculate encoded data size before real encoding happens */
  public static final String UNIFIED_ENCODED_BLOCKSIZE_RATIO =
    "hbase.writer.unified.encoded.blocksize.ratio";

  /** Block size limit after encoding, used to unify encoded block Cache entry size */
  private final int encodedBlockSizeLimit;

  /** The Cell previously appended. Becomes the last cell in the file. */
  protected Cell lastCell = null;

  /** FileSystem stream to write into. */
  protected FSDataOutputStream outputStream;

  /** True if we opened the <code>outputStream</code> (and so will close it). */
  protected boolean closeOutputStream;

  /** A "file info" block: a key-value map of file-wide metadata. */
  protected HFileInfo fileInfo = new HFileInfo();

  /** Total # of key/value entries, i.e. how many times add() was called. */
  protected long entryCount = 0;

  /** Used for calculating the average key length. */
  protected long totalKeyLength = 0;

  /** Used for calculating the average value length. */
  protected long totalValueLength = 0;

  /** Len of the biggest cell. */
  protected long lenOfBiggestCell = 0;
  /** Key of the biggest cell. */
  protected byte[] keyOfBiggestCell;

  /** Total uncompressed bytes, maybe calculate a compression ratio later. */
  protected long totalUncompressedBytes = 0;

  /** Meta block names. */
  protected List<byte[]> metaNames = new ArrayList<>();

  /** {@link Writable}s representing meta block data. */
  protected List<Writable> metaData = new ArrayList<>();

  /**
   * First cell in a block. This reference should be short-lived since we write hfiles in a burst.
   */
  protected Cell firstCellInBlock = null;

  /** May be null if we were passed a stream. */
  protected final Path path;

  /** Cache configuration for caching data on write. */
  protected final CacheConfig cacheConf;

  /**
   * Name for this object used when logging or in toString. Is either the result of a toString on
   * stream or else name of passed file Path.
   */
  protected final String name;

  /**
   * The data block encoding which will be used. {@link NoOpDataBlockEncoder#INSTANCE} if there is
   * no encoding.
   */
  protected final HFileDataBlockEncoder blockEncoder;

  protected final HFileIndexBlockEncoder indexBlockEncoder;

  protected final HFileContext hFileContext;

  protected int maxTagsLength = 0;

  /** KeyValue version in FileInfo */
  public static final byte[] KEY_VALUE_VERSION = Bytes.toBytes("KEY_VALUE_VERSION");

  /** Version for KeyValue which includes memstore timestamp */
  public static final int KEY_VALUE_VER_WITH_MEMSTORE = 1;

  /** Inline block writers for multi-level block index and compound Blooms. */
  protected List<InlineBlockWriter> inlineBlockWriters = new ArrayList<>();

  /** block writer */
  protected HFileBlock.Writer blockWriter;

  protected HFileBlockIndex.BlockIndexWriter dataBlockIndexWriter;
  protected HFileBlockIndex.BlockIndexWriter metaBlockIndexWriter;

  /** The offset of the first data block or -1 if the file is empty. */
  protected long firstDataBlockOffset = UNSET;

  /** The offset of the last data block or 0 if the file is empty. */
  protected long lastDataBlockOffset = UNSET;

  /**
   * The last(stop) Cell of the previous data block. This reference should be short-lived since we
   * write hfiles in a burst.
   */
  protected Cell lastCellOfPreviousBlock = null;

  /** Additional data items to be written to the "load-on-open" section. */
  protected List<BlockWritable> additionalLoadOnOpenData = new ArrayList<>();

  protected long maxMemstoreTS = 0;

  protected SingleStoreFileWriter singleStoreFileWriter;

  protected BloomFilterWriter generalBloomFilterWriter;
  protected BloomFilterWriter deleteFamilyBloomFilterWriter;
  protected BloomType bloomType;
  protected final long maxKeysInBloomFilters;
  protected byte[] bloomParam = null;
  protected long deleteFamilyCnt = 0;
  protected BloomContext bloomContext = null;
  protected BloomContext deleteFamilyBloomContext = null;

  public HFileWriterImpl(final Configuration conf, CacheConfig cacheConf, Path path,
    FSDataOutputStream outputStream, HFileContext fileContext, BloomType bloomType,
    long maxKeysInBloomFilters)
    throws IOException {
    this.outputStream = outputStream;
    this.path = path;
    this.name = path != null ? path.getName() : outputStream.toString();
    this.hFileContext = fileContext;
    DataBlockEncoding encoding = hFileContext.getDataBlockEncoding();
    if (encoding != DataBlockEncoding.NONE) {
      this.blockEncoder = new HFileDataBlockEncoderImpl(encoding);
    } else {
      this.blockEncoder = NoOpDataBlockEncoder.INSTANCE;
    }
    IndexBlockEncoding indexBlockEncoding = hFileContext.getIndexBlockEncoding();
    if (indexBlockEncoding != IndexBlockEncoding.NONE) {
      this.indexBlockEncoder = new HFileIndexBlockEncoderImpl(indexBlockEncoding);
    } else {
      this.indexBlockEncoder = NoOpIndexBlockEncoder.INSTANCE;
    }
    closeOutputStream = path != null;
    this.cacheConf = cacheConf;
    float encodeBlockSizeRatio = conf.getFloat(UNIFIED_ENCODED_BLOCKSIZE_RATIO, 0f);
    this.encodedBlockSizeLimit = (int) (hFileContext.getBlocksize() * encodeBlockSizeRatio);
    this.maxKeysInBloomFilters = maxKeysInBloomFilters;

    finishInit(conf, bloomType);
    if (LOG.isTraceEnabled()) {
      LOG.trace("Writer" + (path != null ? " for " + path : "") + " initialized with cacheConf: "
        + cacheConf + " fileContext: " + fileContext);
    }
  }

  protected HFileInfo getFileInfo() {
    return fileInfo;
  }

  /**
   * Add to the file info. All added key/value pairs can be obtained using
   * {@link HFile.Reader#getHFileInfo()}.
   * @param k Key
   * @param v Value
   * @throws IOException in case the key or the value are invalid
   */
  @Override
  public void appendFileInfo(final byte[] k, final byte[] v) throws IOException {
    getFileInfo().append(k, v, true);
  }

  /**
   * Sets the file info offset in the trailer, finishes up populating fields in the file info, and
   * writes the file info into the given data output. The reason the data output is not always
   * {@link #outputStream} is that we store file info as a block in version 2.
   * @param out     the data output to write the file info to
   */
  protected final void writeFileInfo(DataOutputStream out)
    throws IOException {
    finishFileInfo();
    long startTime = EnvironmentEdgeManager.currentTime();
    getFileInfo().write(out);
    HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime() - startTime);
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
  }

  public long getPos() throws IOException {
    return outputStream.getPos();

  }

  /**
   * Checks that the given Cell's key does not violate the key order.
   * @param cell Cell whose key to check.
   * @return true if the key is duplicate
   * @throws IOException if the key or the key order is wrong
   */
  protected boolean checkKey(final Cell cell) throws IOException {
    boolean isDuplicateKey = false;

    if (cell == null) {
      throw new IOException("Key cannot be null or empty");
    }
    if (lastCell != null) {
      int keyComp = PrivateCellUtil.compareKeyIgnoresMvcc(this.hFileContext.getCellComparator(),
        lastCell, cell);
      if (keyComp > 0) {
        String message = getLexicalErrorMessage(cell);
        throw new IOException(message);
      } else if (keyComp == 0) {
        isDuplicateKey = true;
      }
    }
    return isDuplicateKey;
  }

  private String getLexicalErrorMessage(Cell cell) {
    StringBuilder sb = new StringBuilder();
    sb.append("Added a key not lexically larger than previous. Current cell = ");
    sb.append(cell);
    sb.append(", lastCell = ");
    sb.append(lastCell);
    // file context includes HFile path and optionally table and CF of file being written
    sb.append("fileContext=");
    sb.append(hFileContext);
    return sb.toString();
  }

  /** Checks the given value for validity. */
  protected void checkValue(final byte[] value, final int offset, final int length)
    throws IOException {
    if (value == null) {
      throw new IOException("Value cannot be null");
    }
  }

  /** Returns Path or null if we were passed a stream rather than a Path. */
  @Override
  public Path getPath() {
    return path;
  }

  @Override
  public String toString() {
    return "writer=" + (path != null ? path.toString() : null) + ", name=" + name + ", compression="
      + hFileContext.getCompression().getName();
  }

  public static Compression.Algorithm compressionByName(String algoName) {
    if (algoName == null) {
      return HFile.DEFAULT_COMPRESSION_ALGORITHM;
    }
    return Compression.getCompressionAlgorithmByName(algoName);
  }

  /** A helper method to create HFile output streams in constructors */
  protected static FSDataOutputStream createOutputStream(Configuration conf, FileSystem fs,
    Path path, InetSocketAddress[] favoredNodes) throws IOException {
    FsPermission perms = CommonFSUtils.getFilePermissions(fs, conf, HConstants.DATA_FILE_UMASK_KEY);
    return FSUtils.create(conf, fs, path, perms, favoredNodes);
  }

  /** Additional initialization steps */
  protected void finishInit(final Configuration conf, BloomType bloomType) throws IOException {
    if (blockWriter != null) {
      throw new IllegalStateException("finishInit called twice");
    }
    blockWriter =
      new HFileBlock.Writer(conf, blockEncoder, hFileContext, cacheConf.getByteBuffAllocator(),
        conf.getInt(MAX_BLOCK_SIZE_UNCOMPRESSED, hFileContext.getBlocksize() * 10));
    // Data block index writer
    boolean cacheIndexesOnWrite = cacheConf.shouldCacheIndexesOnWrite();
    dataBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter(blockWriter,
      cacheIndexesOnWrite ? cacheConf : null, cacheIndexesOnWrite ? name : null, indexBlockEncoder);
    dataBlockIndexWriter.setMaxChunkSize(HFileBlockIndex.getMaxChunkSize(conf));
    dataBlockIndexWriter.setMinIndexNumEntries(HFileBlockIndex.getMinIndexNumEntries(conf));
    inlineBlockWriters.add(dataBlockIndexWriter);

    // Meta data block index writer
    metaBlockIndexWriter = new HFileBlockIndex.BlockIndexWriter();
    initBloomFilterWriters(conf, bloomType);
    LOG.trace("Initialized with {}", cacheConf);
  }

  protected void initBloomFilterWriters(Configuration conf, BloomType bloomType) throws IOException {
    generalBloomFilterWriter = BloomFilterFactory.createGeneralBloomAtWrite(conf, cacheConf,
      bloomType, (int) Math.min(maxKeysInBloomFilters, Integer.MAX_VALUE), this);

    if (generalBloomFilterWriter != null) {
      this.bloomType = bloomType;
      this.bloomParam = BloomFilterUtil.getBloomFilterParam(bloomType, conf);
      if (LOG.isTraceEnabled()) {
        LOG.trace("Bloom filter type for " + path + ": " + this.bloomType + ", param: "
          + (bloomType == BloomType.ROWPREFIX_FIXED_LENGTH
          ? Bytes.toInt(bloomParam)
          : Bytes.toStringBinary(bloomParam))
          + ", " + generalBloomFilterWriter.getClass().getSimpleName());
      }
      // init bloom context
      switch (bloomType) {
        case ROW:
          bloomContext =
            new RowBloomContext(generalBloomFilterWriter, hFileContext.getCellComparator());
          break;
        case ROWCOL:
          bloomContext =
            new RowColBloomContext(generalBloomFilterWriter, hFileContext.getCellComparator());
          break;
        case ROWPREFIX_FIXED_LENGTH:
          bloomContext = new RowPrefixFixedLengthBloomContext(generalBloomFilterWriter,
            hFileContext.getCellComparator(), Bytes.toInt(bloomParam));
          break;
        default:
          throw new IOException(
            "Invalid Bloom filter type: " + bloomType + " (ROW or ROWCOL or ROWPREFIX expected)");
      }
    } else {
      // Not using Bloom filters.
      this.bloomType = BloomType.NONE;
    }

    // initialize delete family Bloom filter when there is NO RowCol Bloom filter
    if (this.bloomType != BloomType.ROWCOL) {
      this.deleteFamilyBloomFilterWriter = BloomFilterFactory.createDeleteBloomAtWrite(conf,
        cacheConf, (int) Math.min(maxKeysInBloomFilters, Integer.MAX_VALUE), this);
      deleteFamilyBloomContext =
        new RowBloomContext(deleteFamilyBloomFilterWriter, hFileContext.getCellComparator());
    } else {
      deleteFamilyBloomFilterWriter = null;
    }
    if (deleteFamilyBloomFilterWriter != null && LOG.isTraceEnabled()) {
      LOG.trace("Delete Family Bloom filter type for " + path + ": "
        + deleteFamilyBloomFilterWriter.getClass().getSimpleName());
    }
  }

  protected boolean shouldFinishBlock(Cell cell) {
    boolean finishBlock = false;
    // This means hbase.writer.unified.encoded.blocksize.ratio was set to something different from 0
    // and we should use the encoding ratio
    if (encodedBlockSizeLimit > 0) {
      finishBlock = blockWriter.encodedBlockSizeWritten() >= encodedBlockSizeLimit;
    } else {
      finishBlock = blockWriter.encodedBlockSizeWritten() >= hFileContext.getBlocksize()
        || blockWriter.blockSizeWritten() >= hFileContext.getBlocksize();
    }
    finishBlock &= blockWriter.checkBoundariesWithPredicate();
    return finishBlock;
  }

  protected void finishCurBlockAndStartNewBlock() throws IOException {
    finishBlock();
    writeInlineBlocks(false);
    newBlock();
  }

  /**
   * At a block boundary, write all the inline blocks and opens new block.
   */
  protected void checkBlockBoundary(Cell cell) throws IOException {
    if (shouldFinishBlock(cell)) {
      finishCurBlockAndStartNewBlock();
    }
  }

  /** Clean up the data block that is currently being written. */
  protected void finishBlock() throws IOException {
    if (!blockWriter.isWriting() || blockWriter.blockSizeWritten() == 0) {
      return;
    }

    // Update the first data block offset if UNSET; used scanning.
    if (firstDataBlockOffset == UNSET) {
      firstDataBlockOffset = outputStream.getPos();
    }
    // Update the last data block offset each time through here.
    lastDataBlockOffset = outputStream.getPos();
    blockWriter.writeHeaderAndData(outputStream);
    int onDiskSize = blockWriter.getOnDiskSizeWithHeader();
    Cell indexEntry =
      getMidpoint(this.hFileContext.getCellComparator(), lastCellOfPreviousBlock, firstCellInBlock);
    dataBlockIndexWriter.addEntry(PrivateCellUtil.getCellKeySerializedAsKeyValueKey(indexEntry),
      lastDataBlockOffset, onDiskSize);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
    if (cacheConf.shouldCacheDataOnWrite()) {
      doCacheOnWrite(lastDataBlockOffset);
    }
  }

  /**
   * Try to return a Cell that falls between <code>left</code> and <code>right</code> but that is
   * shorter; i.e. takes up less space. This trick is used building HFile block index. Its an
   * optimization. It does not always work. In this case we'll just return the <code>right</code>
   * cell.
   * @return A cell that sorts between <code>left</code> and <code>right</code>.
   */
  public static Cell getMidpoint(final CellComparator comparator, final Cell left,
    final Cell right) {
    if (right == null) {
      throw new IllegalArgumentException("right cell can not be null");
    }
    if (left == null) {
      return right;
    }
    // If Cells from meta table, don't mess around. meta table Cells have schema
    // (table,startrow,hash) so can't be treated as plain byte arrays. Just skip
    // out without trying to do this optimization.
    if (comparator instanceof MetaCellComparator) {
      return right;
    }
    byte[] midRow;
    boolean bufferBacked =
      left instanceof ByteBufferExtendedCell && right instanceof ByteBufferExtendedCell;
    if (bufferBacked) {
      midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getRowByteBuffer(),
        ((ByteBufferExtendedCell) left).getRowPosition(), left.getRowLength(),
        ((ByteBufferExtendedCell) right).getRowByteBuffer(),
        ((ByteBufferExtendedCell) right).getRowPosition(), right.getRowLength());
    } else {
      midRow = getMinimumMidpointArray(left.getRowArray(), left.getRowOffset(), left.getRowLength(),
        right.getRowArray(), right.getRowOffset(), right.getRowLength());
    }
    if (midRow != null) {
      return PrivateCellUtil.createFirstOnRow(midRow);
    }
    // Rows are same. Compare on families.
    if (bufferBacked) {
      midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) left).getFamilyPosition(), left.getFamilyLength(),
        ((ByteBufferExtendedCell) right).getFamilyByteBuffer(),
        ((ByteBufferExtendedCell) right).getFamilyPosition(), right.getFamilyLength());
    } else {
      midRow = getMinimumMidpointArray(left.getFamilyArray(), left.getFamilyOffset(),
        left.getFamilyLength(), right.getFamilyArray(), right.getFamilyOffset(),
        right.getFamilyLength());
    }
    if (midRow != null) {
      return PrivateCellUtil.createFirstOnRowFamily(right, midRow, 0, midRow.length);
    }
    // Families are same. Compare on qualifiers.
    if (bufferBacked) {
      midRow = getMinimumMidpointArray(((ByteBufferExtendedCell) left).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) left).getQualifierPosition(), left.getQualifierLength(),
        ((ByteBufferExtendedCell) right).getQualifierByteBuffer(),
        ((ByteBufferExtendedCell) right).getQualifierPosition(), right.getQualifierLength());
    } else {
      midRow = getMinimumMidpointArray(left.getQualifierArray(), left.getQualifierOffset(),
        left.getQualifierLength(), right.getQualifierArray(), right.getQualifierOffset(),
        right.getQualifierLength());
    }
    if (midRow != null) {
      return PrivateCellUtil.createFirstOnRowCol(right, midRow, 0, midRow.length);
    }
    // No opportunity for optimization. Just return right key.
    return right;
  }

  /**
   * Try to get a byte array that falls between left and right as short as possible with
   * lexicographical order;
   * @return Return a new array that is between left and right and minimally sized else just return
   *         null if left == right.
   */
  private static byte[] getMinimumMidpointArray(final byte[] leftArray, final int leftOffset,
    final int leftLength, final byte[] rightArray, final int rightOffset, final int rightLength) {
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    for (; diffIdx < minLength; diffIdx++) {
      byte leftByte = leftArray[leftOffset + diffIdx];
      byte rightByte = rightArray[rightOffset + diffIdx];
      if ((leftByte & 0xff) > (rightByte & 0xff)) {
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + Bytes.toStringBinary(leftArray, leftOffset, leftLength) + ", right="
          + Bytes.toStringBinary(rightArray, rightOffset, rightLength));
      } else if (leftByte != rightByte) {
        break;
      }
    }
    if (diffIdx == minLength) {
      if (leftLength > rightLength) {
        // right is prefix of left
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + Bytes.toStringBinary(leftArray, leftOffset, leftLength) + ", right="
          + Bytes.toStringBinary(rightArray, rightOffset, rightLength));
      } else if (leftLength < rightLength) {
        // left is prefix of right.
        byte[] minimumMidpointArray = new byte[minLength + 1];
        System.arraycopy(rightArray, rightOffset, minimumMidpointArray, 0, minLength + 1);
        minimumMidpointArray[minLength] = 0x00;
        return minimumMidpointArray;
      } else {
        // left == right
        return null;
      }
    }
    // Note that left[diffIdx] can never be equal to 0xff since left < right
    byte[] minimumMidpointArray = new byte[diffIdx + 1];
    System.arraycopy(leftArray, leftOffset, minimumMidpointArray, 0, diffIdx + 1);
    minimumMidpointArray[diffIdx] = (byte) (minimumMidpointArray[diffIdx] + 1);
    return minimumMidpointArray;
  }

  /**
   * Try to create a new byte array that falls between left and right as short as possible with
   * lexicographical order.
   * @return Return a new array that is between left and right and minimally sized else just return
   *         null if left == right.
   */
  private static byte[] getMinimumMidpointArray(ByteBuffer left, int leftOffset, int leftLength,
    ByteBuffer right, int rightOffset, int rightLength) {
    int minLength = leftLength < rightLength ? leftLength : rightLength;
    int diffIdx = 0;
    for (; diffIdx < minLength; diffIdx++) {
      int leftByte = ByteBufferUtils.toByte(left, leftOffset + diffIdx);
      int rightByte = ByteBufferUtils.toByte(right, rightOffset + diffIdx);
      if ((leftByte & 0xff) > (rightByte & 0xff)) {
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + ByteBufferUtils.toStringBinary(left, leftOffset, leftLength) + ", right="
          + ByteBufferUtils.toStringBinary(right, rightOffset, rightLength));
      } else if (leftByte != rightByte) {
        break;
      }
    }
    if (diffIdx == minLength) {
      if (leftLength > rightLength) {
        // right is prefix of left
        throw new IllegalArgumentException("Left byte array sorts after right row; left="
          + ByteBufferUtils.toStringBinary(left, leftOffset, leftLength) + ", right="
          + ByteBufferUtils.toStringBinary(right, rightOffset, rightLength));
      } else if (leftLength < rightLength) {
        // left is prefix of right.
        byte[] minimumMidpointArray = new byte[minLength + 1];
        ByteBufferUtils.copyFromBufferToArray(minimumMidpointArray, right, rightOffset, 0,
          minLength + 1);
        minimumMidpointArray[minLength] = 0x00;
        return minimumMidpointArray;
      } else {
        // left == right
        return null;
      }
    }
    // Note that left[diffIdx] can never be equal to 0xff since left < right
    byte[] minimumMidpointArray = new byte[diffIdx + 1];
    ByteBufferUtils.copyFromBufferToArray(minimumMidpointArray, left, leftOffset, 0, diffIdx + 1);
    minimumMidpointArray[diffIdx] = (byte) (minimumMidpointArray[diffIdx] + 1);
    return minimumMidpointArray;
  }

  /** Gives inline block writers an opportunity to contribute blocks. */
  protected void writeInlineBlocks(boolean closing) throws IOException {
    for (InlineBlockWriter ibw : inlineBlockWriters) {
      while (ibw.shouldWriteBlock(closing)) {
        long offset = outputStream.getPos();
        boolean cacheThisBlock = ibw.getCacheOnWrite();
        ibw.writeInlineBlock(blockWriter.startWriting(ibw.getInlineBlockType()));
        blockWriter.writeHeaderAndData(outputStream);
        ibw.blockWritten(offset, blockWriter.getOnDiskSizeWithHeader(),
          blockWriter.getUncompressedSizeWithoutHeader());
        totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

        if (cacheThisBlock) {
          doCacheOnWrite(offset);
        }
      }
    }
  }

  /**
   * Caches the last written HFile block.
   * @param offset the offset of the block we want to cache. Used to determine the cache key.
   */
  void doCacheOnWrite(long offset) {
    cacheConf.getBlockCache().ifPresent(cache -> {
      HFileBlock cacheFormatBlock = blockWriter.getBlockForCaching(cacheConf);
      try {
        cache.cacheBlock(new BlockCacheKey(name, offset, true, cacheFormatBlock.getBlockType()),
          cacheFormatBlock, cacheConf.isInMemory(), true);
      } finally {
        // refCnt will auto increase when block add to Cache, see RAMCache#putIfAbsent
        cacheFormatBlock.release();
      }
    });
  }

  /**
   * Ready a new block for writing.
   */
  protected void newBlock() throws IOException {
    // This is where the next block begins.
    blockWriter.startWriting(BlockType.DATA);
    firstCellInBlock = null;
    if (lastCell != null) {
      lastCellOfPreviousBlock = lastCell;
    }
  }

  /**
   * Add a meta block to the end of the file. Call before close(). Metadata blocks are expensive.
   * Fill one with a bunch of serialized data rather than do a metadata block per metadata instance.
   * If metadata is small, consider adding to file info using
   * {@link #appendFileInfo(byte[], byte[])} name of the block will call readFields to get data
   * later (DO NOT REUSE)
   */
  @Override
  public void appendMetaBlock(String metaBlockName, Writable content) {
    byte[] key = Bytes.toBytes(metaBlockName);
    int i;
    for (i = 0; i < metaNames.size(); ++i) {
      // stop when the current key is greater than our own
      byte[] cur = metaNames.get(i);
      if (Bytes.BYTES_RAWCOMPARATOR.compare(cur, 0, cur.length, key, 0, key.length) > 0) {
        break;
      }
    }
    metaNames.add(i, key);
    metaData.add(i, content);
  }

  protected void writeMetadataBlocks() throws IOException {
    // Write out the metadata blocks if any.
    if (!metaNames.isEmpty()) {
      for (int i = 0; i < metaNames.size(); ++i) {
        // store the beginning offset
        long offset = outputStream.getPos();
        // write the metadata content
        DataOutputStream dos = blockWriter.startWriting(BlockType.META);
        metaData.get(i).write(dos);

        blockWriter.writeHeaderAndData(outputStream);
        totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();

        // Add the new meta block to the meta index.
        metaBlockIndexWriter.addEntry(metaNames.get(i), offset,
          blockWriter.getOnDiskSizeWithHeader());
      }
    }
  }

  protected void writeMetaBlockIndex() throws IOException {
    // Meta block index.
    metaBlockIndexWriter.writeSingleLevelIndex(blockWriter.startWriting(BlockType.ROOT_INDEX),
      "meta");
    blockWriter.writeHeaderAndData(outputStream);
    totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
  }

  protected void finishDataBlockAndInlineBlocks() throws IOException {
    finishBlock();
    writeInlineBlocks(true);
  }

  protected void writeBloomIndexes() throws IOException {
    // Load-on-open data supplied by higher levels, e.g. Bloom filters.
    for (BlockWritable w : additionalLoadOnOpenData) {
      blockWriter.writeBlock(w, outputStream);
      totalUncompressedBytes += blockWriter.getUncompressedSizeWithHeader();
    }
  }

  @Override
  public void close() throws IOException {
    boolean hasGeneralBloom = closeGeneralBloomFilter();
    boolean hasDeleteFamilyBloom = closeDeleteFamilyBloomFilter();

    if (outputStream == null) {
      return;
    }
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
    trailer.setLoadOnOpenOffset(rootIndexOffset);

    writeMetaBlockIndex();

    // File info
    trailer.setFileInfoOffset(outputStream.getPos());
    writeFileInfo(blockWriter.startWriting(BlockType.FILE_INFO));

    writeBloomIndexes();

    // Now finish off the trailer.
    finishTrailer(trailer);
    finishClose();

    // Log final Bloom filter statistics. This needs to be done after close()
    // because compound Bloom filters might be finalized as part of closing.
    if (LOG.isTraceEnabled()) {
      LOG.trace(
        (hasGeneralBloom ? "" : "NO ") + "General Bloom and " + (hasDeleteFamilyBloom ? "" : "NO ")
          + "DeleteFamily" + " was added to HFile " + getPath());
    }
  }

  protected void finishClose() throws IOException {
    if (closeOutputStream) {
      outputStream.close();
      outputStream = null;
    }
    blockWriter.release();
  }

  @Override
  public void addInlineBlockWriter(InlineBlockWriter ibw) {
    inlineBlockWriters.add(ibw);
  }

  @Override
  public void addGeneralBloomFilter(final BloomFilterWriter bfw) {
    this.addBloomFilter(bfw, BlockType.GENERAL_BLOOM_META);
  }

  @Override
  public void addDeleteFamilyBloomFilter(final BloomFilterWriter bfw) {
    this.addBloomFilter(bfw, BlockType.DELETE_FAMILY_BLOOM_META);
  }

  private void addBloomFilter(final BloomFilterWriter bfw, final BlockType blockType) {
    if (bfw.getKeyCount() <= 0) {
      return;
    }

    if (
      blockType != BlockType.GENERAL_BLOOM_META && blockType != BlockType.DELETE_FAMILY_BLOOM_META
    ) {
      throw new RuntimeException("Block Type: " + blockType.toString() + "is not supported");
    }
    additionalLoadOnOpenData.add(new BlockWritable() {
      @Override
      public BlockType getBlockType() {
        return blockType;
      }

      @Override
      public void writeToBlock(DataOutput out) throws IOException {
        bfw.getMetaWriter().write(out);
        Writable dataWriter = bfw.getDataWriter();
        if (dataWriter != null) {
          dataWriter.write(out);
        }
      }
    });
  }

  @Override
  public HFileContext getFileContext() {
    return hFileContext;
  }

  /**
   * Add key/value to file. Keys must be added in an order that agrees with the Comparator passed on
   * construction. Cell to add. Cannot be empty nor null.
   */
  @Override
  public void append(final Cell cell) throws IOException {
    appendGeneralBloomfilter(cell);
    appendDeleteFamilyBloomFilter(cell);
    // checkKey uses comparator to check we are writing in order.
    boolean dupKey = checkKey(cell);
    if (!dupKey) {
      checkBlockBoundary(cell);
    }

    if (!blockWriter.isWriting()) {
      newBlock();
    }

    blockWriter.write(cell);

    totalKeyLength += PrivateCellUtil.estimatedSerializedSizeOfKey(cell);
    totalValueLength += cell.getValueLength();
    if (lenOfBiggestCell < PrivateCellUtil.estimatedSerializedSizeOf(cell)) {
      lenOfBiggestCell = PrivateCellUtil.estimatedSerializedSizeOf(cell);
      keyOfBiggestCell = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(cell);
    }
    // Are we the first key in this block?
    if (firstCellInBlock == null) {
      // If cell is big, block will be closed and this firstCellInBlock reference will only last
      // a short while.
      firstCellInBlock = cell;
    }

    // TODO: What if cell is 10MB and we write infrequently? We hold on to cell here indefinitely?
    lastCell = cell;
    entryCount++;
    this.maxMemstoreTS = Math.max(this.maxMemstoreTS, cell.getSequenceId());
    int tagsLength = cell.getTagsLength();
    if (tagsLength > this.maxTagsLength) {
      this.maxTagsLength = tagsLength;
    }
  }

  private void appendGeneralBloomfilter(final Cell cell) throws IOException {
    if (this.generalBloomFilterWriter != null) {
      /*
       * http://2.bp.blogspot.com/_Cib_A77V54U/StZMrzaKufI/AAAAAAAAADo/ZhK7bGoJdMQ/s400/KeyValue.
       * png Key = RowLen + Row + FamilyLen + Column [Family + Qualifier] + Timestamp 3 Types of
       * Filtering: 1. Row = Row 2. RowCol = Row + Qualifier 3. RowPrefixFixedLength = Fixed
       * Length Row Prefix
       */
      bloomContext.writeBloom(cell);
    }
  }

  private void appendDeleteFamilyBloomFilter(final Cell cell) throws IOException {
    if (!PrivateCellUtil.isDeleteFamily(cell) && !PrivateCellUtil.isDeleteFamilyVersion(cell)) {
      return;
    }

    // increase the number of delete family in the store file
    deleteFamilyCnt++;
    if (this.deleteFamilyBloomFilterWriter != null) {
      deleteFamilyBloomContext.writeBloom(cell);
    }
  }

  private boolean closeBloomFilter(BloomFilterWriter bfw) throws IOException {
    boolean haveBloom = (bfw != null && bfw.getKeyCount() > 0);
    if (haveBloom) {
      bfw.compactBloom();
    }
    return haveBloom;
  }

  public boolean closeGeneralBloomFilter() throws IOException {
    boolean hasGeneralBloom = closeBloomFilter(generalBloomFilterWriter);

    // add the general Bloom filter writer and append file info
    if (hasGeneralBloom) {
      this.addGeneralBloomFilter(generalBloomFilterWriter);
      this.appendFileInfo(BLOOM_FILTER_TYPE_KEY, Bytes.toBytes(bloomType.toString()));
      if (bloomParam != null) {
        this.appendFileInfo(BLOOM_FILTER_PARAM_KEY, bloomParam);
      }
      bloomContext.addLastBloomKey(this);
    }
    return hasGeneralBloom;
  }

  public boolean closeDeleteFamilyBloomFilter() throws IOException {
    boolean hasDeleteFamilyBloom = closeBloomFilter(deleteFamilyBloomFilterWriter);

    // add the delete family Bloom filter writer
    if (hasDeleteFamilyBloom) {
      this.addDeleteFamilyBloomFilter(deleteFamilyBloomFilterWriter);
    }

    // append file info about the number of delete family kvs
    // even if there is no delete family Bloom.
    this.appendFileInfo(DELETE_FAMILY_COUNT, Bytes.toBytes(this.deleteFamilyCnt));

    return hasDeleteFamilyBloom;
  }

  @Override
  public void beforeShipped() throws IOException {
    this.blockWriter.beforeShipped();
    // Add clone methods for every cell
    if (this.lastCell != null) {
      this.lastCell = KeyValueUtil.toNewKeyCell(this.lastCell);
    }
    if (this.firstCellInBlock != null) {
      this.firstCellInBlock = KeyValueUtil.toNewKeyCell(this.firstCellInBlock);
    }
    if (this.lastCellOfPreviousBlock != null) {
      this.lastCellOfPreviousBlock = KeyValueUtil.toNewKeyCell(this.lastCellOfPreviousBlock);
    }
    if (generalBloomFilterWriter != null) {
      generalBloomFilterWriter.beforeShipped();
    }
    if (deleteFamilyBloomFilterWriter != null) {
      deleteFamilyBloomFilterWriter.beforeShipped();
    }
  }

  public Cell getLastCell() {
    return lastCell;
  }

  protected void finishFileInfo() throws IOException {
    // Save data block encoder metadata in the file info.
    blockEncoder.saveMetadata(this);
    // Save index block encoder metadata in the file info.
    indexBlockEncoder.saveMetadata(this);
    if (this.hFileContext.isIncludesMvcc()) {
      appendFileInfo(MAX_MEMSTORE_TS_KEY, Bytes.toBytes(maxMemstoreTS));
      appendFileInfo(KEY_VALUE_VERSION, Bytes.toBytes(KEY_VALUE_VER_WITH_MEMSTORE));
    }
    if (lastCell != null) {
      // Make a copy. The copy is stuffed into our fileinfo map. Needs a clean
      // byte buffer. Won't take a tuple.
      byte[] lastKey = PrivateCellUtil.getCellKeySerializedAsKeyValueKey(this.lastCell);
      getFileInfo().append(HFileInfo.LASTKEY, lastKey, false);
    }

    // Average key length.
    int avgKeyLen = entryCount == 0 ? 0 : (int) (totalKeyLength / entryCount);
    getFileInfo().append(HFileInfo.AVG_KEY_LEN, Bytes.toBytes(avgKeyLen), false);
    getFileInfo().append(HFileInfo.CREATE_TIME_TS, Bytes.toBytes(hFileContext.getFileCreateTime()),
      false);

    // Average value length.
    int avgValueLen = entryCount == 0 ? 0 : (int) (totalValueLength / entryCount);
    getFileInfo().append(HFileInfo.AVG_VALUE_LEN, Bytes.toBytes(avgValueLen), false);

    // Biggest cell.
    if (keyOfBiggestCell != null) {
      getFileInfo().append(HFileInfo.KEY_OF_BIGGEST_CELL, keyOfBiggestCell, false);
      getFileInfo().append(HFileInfo.LEN_OF_BIGGEST_CELL, Bytes.toBytes(lenOfBiggestCell), false);
      LOG.debug("Len of the biggest cell in {} is {}, key is {}",
        this.getPath() == null ? "" : this.getPath().toString(), lenOfBiggestCell,
        CellUtil.toString(new KeyValue.KeyOnlyKeyValue(keyOfBiggestCell), false));
    }

    if (hFileContext.isIncludesTags()) {
      // When tags are not being written in this file, MAX_TAGS_LEN is excluded
      // from the FileInfo
      getFileInfo().append(HFileInfo.MAX_TAGS_LEN, Bytes.toBytes(this.maxTagsLength), false);
      boolean tagsCompressed = (hFileContext.getDataBlockEncoding() != DataBlockEncoding.NONE)
        && hFileContext.isCompressTags();
      getFileInfo().append(HFileInfo.TAGS_COMPRESSED, Bytes.toBytes(tagsCompressed), false);
    }
  }

  protected int getMajorVersion() {
    return 3;
  }

  protected int getMinorVersion() {
    return HFileReaderImpl.MAX_MINOR_VERSION;
  }

  protected void finishTrailer(FixedFileTrailer trailer) throws IOException {
    trailer.setNumDataIndexLevels(dataBlockIndexWriter.getNumLevels());
    trailer.setUncompressedDataIndexSize(dataBlockIndexWriter.getTotalUncompressedSize());
    trailer.setFirstDataBlockOffset(firstDataBlockOffset);
    trailer.setLastDataBlockOffset(lastDataBlockOffset);
    trailer.setComparatorClass(this.hFileContext.getCellComparator().getClass());
    trailer.setDataIndexCount(dataBlockIndexWriter.getNumRootEntries());
    // Write out encryption metadata before finalizing if we have a valid crypto context
    Encryption.Context cryptoContext = hFileContext.getEncryptionContext();
    if (cryptoContext != Encryption.Context.NONE) {
      // Wrap the context's key and write it as the encryption metadata, the wrapper includes
      // all information needed for decryption
      trailer.setEncryptionKey(EncryptionUtil.wrapKey(
        cryptoContext.getConf(), cryptoContext.getConf()
          .get(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, User.getCurrent().getShortName()),
        cryptoContext.getKey()));
    }
    // Now we can finish the close
    trailer.setMetaIndexCount(metaNames.size());
    trailer.setTotalUncompressedBytes(totalUncompressedBytes + trailer.getTrailerSize());
    trailer.setEntryCount(entryCount);
    trailer.setCompressionCodec(hFileContext.getCompression());

    long startTime = EnvironmentEdgeManager.currentTime();
    trailer.serialize(outputStream);
    HFile.updateWriteLatency(EnvironmentEdgeManager.currentTime() - startTime);
  }

  public BloomFilterWriter getGeneralBloomWriter() {
    return generalBloomFilterWriter;
  }
}
