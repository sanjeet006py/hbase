package org.apache.hadoop.hbase.io.hfile;

import org.apache.commons.lang3.NotImplementedException;

public interface RowKeyPrefixIndexedWriter {
  default void setSectionStartOffset(long sectionStartOffset) {
    throw new NotImplementedException("Not implemented!");
  }

  default long getSectionStartOffset() {
    throw new NotImplementedException("Not implemented!");
  }
}
