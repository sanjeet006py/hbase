package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Rule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.rules.TestName;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;

import static org.apache.hadoop.hbase.regionserver.HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL;

// TODO: Figure out why classes from hbase-annotations module are not getting picked up in
//  classpath by IntelliJ IDEA
//@Category({ IOTests.class, SmallTests.class })
public class TestHFileV4 {
//  @ClassRule
//  public static final HBaseClassTestRule CLASS_RULE =
//    HBaseClassTestRule.forClass(TestHFileV4.class);

  @Rule
  public TestName testName = new TestName();

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileV4.class);

  static final byte[] FAMILY = Bytes.toBytes("cf");

  private static final byte[][] families = new byte[][] { FAMILY };

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setInt("hfile.format.version", 4);
    UTIL.getConfiguration().setInt(MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0);
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    LOG.info("before");
    UTIL.ensureSomeRegionServersAvailable(1);
    LOG.info("before done");
  }

  @Test
  public void testFlushWithHFileV4() throws IOException {
    LOG.info("testFlushWithHFileV4");
    String tableName = "testpbe";
    Table table = UTIL.createTableForHFileV4(TableName.valueOf(tableName), families, 4);
    Put put = new Put("row1".getBytes());
    put.addColumn(FAMILY, "c1".getBytes(), "value1".getBytes());
    table.put(put);
    put = new Put("row2".getBytes());
    put.addColumn(FAMILY, "c1".getBytes(), "value2".getBytes());
    table.put(put);
    LOG.info("Added a new row, going for flush");
    // Flush is expected to fail as after flush we file is read to validate and reader hasn't been
    // implemented yet for HFile v4.
    try {
      UTIL.flush(TableName.valueOf(tableName));
    }
    catch (DroppedSnapshotException ex) {
      if(ex.getCause() instanceof CorruptHFileException) {
        LOG.info("Flush failed as expected as reader for new HFile format is yet to be implemented");
      }
      else {
        throw ex;
      }
    }
    table.close();
  }

  @Test
  public void testRowKeyLengthLessThanPbePrefixLength() throws IOException {
    LOG.info("testRowKeyLengthLessThanPbePrefixLength");
    String tableName = "testpbe1";
    Table table = UTIL.createTableForHFileV4(TableName.valueOf(tableName), families, 4);
    Put put = new Put("row".getBytes());
    put.addColumn(FAMILY, "c1".getBytes(), "value1".getBytes());
    table.put(put);
    LOG.info("Added a new row, going for flush");
    try {
      UTIL.flush(TableName.valueOf(tableName));
      Assert.fail();
    }
    catch (DroppedSnapshotException ex) {
      Exception innerEx = (Exception) ex.getCause();
      if (innerEx instanceof IOException) {
        Assert.assertEquals("Row key length of cell is: 3 instead of expected: 4",
          innerEx.getMessage());
      }
      else {
        throw ex;
      }
    }
    table.close();
 }
}
