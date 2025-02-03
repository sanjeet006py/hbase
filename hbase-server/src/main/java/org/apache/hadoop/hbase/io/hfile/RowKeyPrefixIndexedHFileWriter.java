package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.TableDescriptorBuilder;

import java.io.IOException;

public class RowKeyPrefixIndexedHFileWriter extends HFileWriterImpl {

  private final int rowKeyPrefixLength;

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
  protected boolean checkKey(Cell cell) throws IOException {
    boolean isCellValid = super.checkKey(cell);
    int rowKeyPrefixLengthOfCurCell = CellUtil.copyRow(cell).length;
    if (rowKeyPrefixLengthOfCurCell < rowKeyPrefixLength) {
      throw new IOException("Row key length of cell is: " + rowKeyPrefixLengthOfCurCell
        + " instead of expected: " + rowKeyPrefixLength);
    }
    return isCellValid;
  }

  @Override
  protected boolean shouldFinishBlock(Cell cell) {
    if (this.lastCell == null) {
      return false;
    }
    else if (rowKeyPrefixLength == TableDescriptorBuilder.PBE_PREFIX_LENGTH_DEFAULT) {
      // In this situation using this writer makes no sense
      return false;
    }
    return CellUtil.matchingRows(this.lastCell, (short) rowKeyPrefixLength, cell, (short) rowKeyPrefixLength);
  }
}
