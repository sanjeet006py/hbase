package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.testclassification.IOTests;
import org.apache.hadoop.hbase.testclassification.SmallTests;
import org.apache.hadoop.hbase.util.Bytes;

import org.junit.AfterClass;
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
    LOG.info("Added a new row, going for flush");
    UTIL.flush(TableName.valueOf(tableName));
    LOG.info("Flush succeeded");
    table.close();
  }


}
