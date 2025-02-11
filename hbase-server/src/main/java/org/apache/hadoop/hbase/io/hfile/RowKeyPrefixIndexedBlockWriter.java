package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.io.ByteBuffAllocator;
import org.apache.hadoop.hbase.nio.ByteBuff;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.yetus.audience.InterfaceAudience;

import static org.apache.hadoop.hbase.io.hfile.HFileBlock.UNSET;

@InterfaceAudience.Private
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

  @Override
  protected void putHeader(byte[] dest, int offset, int onDiskSize, int uncompressedSize,
    int onDiskDataSize) {
    offset = blockType.put(dest, offset);
    offset = Bytes.putInt(dest, offset, onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
    offset = Bytes.putInt(dest, offset, uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
    offset = Bytes.putLong(dest, offset, getPrevOffsetForHeader());
    offset = Bytes.putByte(dest, offset, fileContext.getChecksumType().getCode());
    offset = Bytes.putInt(dest, offset, fileContext.getBytesPerChecksum());
    Bytes.putInt(dest, offset, onDiskDataSize);
  }

  @Override
  protected void putHeader(ByteBuff buff, int onDiskSize, int uncompressedSize,
    int onDiskDataSize) {
    buff.rewind();
    blockType.write(buff);
    buff.putInt(onDiskSize - HConstants.HFILEBLOCK_HEADER_SIZE);
    buff.putInt(uncompressedSize - HConstants.HFILEBLOCK_HEADER_SIZE);
    buff.putLong(getPrevOffsetForHeader());
    buff.put(fileContext.getChecksumType().getCode());
    buff.putInt(fileContext.getBytesPerChecksum());
    buff.putInt(onDiskDataSize);
  }

  private long getPrevOffsetForHeader() {
    if (prevOffset == UNSET) {
      return prevOffset;
    }
    else {
      return prevOffset - sectionStartOffset;
    }
  }
}
