package org.apache.hadoop.hbase.io.hfile;

import org.apache.hadoop.hbase.DroppedSnapshotException;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtility;
import org.apache.hadoop.hbase.HColumnDescriptor;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hbase.HTableDescriptor;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.io.crypto.KeyProviderForTesting;
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

import static org.apache.hadoop.hbase.regionserver.HRegion.MEMSTORE_PERIODIC_FLUSH_INTERVAL;

@Category({ IOTests.class, SmallTests.class })
public class TestHFileWriterV4WithEncryption {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestHFileWriterV4WithEncryption.class);

  @Rule
  public TestName testName = new TestName();

  private final static HBaseTestingUtility UTIL = new HBaseTestingUtility();

  private static final Logger LOG = LoggerFactory.getLogger(TestHFileWriterV4WithEncryption.class);

  static final String FAMILY = "cf";

  @BeforeClass
  public static void beforeClass() throws Exception {
    UTIL.getConfiguration().setInt("hfile.format.version", 4);
    UTIL.getConfiguration().setInt(MEMSTORE_PERIODIC_FLUSH_INTERVAL, 0);
    UTIL.getConfiguration().set(HConstants.CRYPTO_KEYPROVIDER_CONF_KEY,
      KeyProviderForTesting.class.getName());
    UTIL.getConfiguration().set(HConstants.CRYPTO_MASTERKEY_NAME_CONF_KEY, "hbase");
    UTIL.startMiniCluster();
  }

  @AfterClass
  public static void afterClass() throws Exception {
    UTIL.shutdownMiniCluster();
  }

  @Before
  public void before() throws IOException {
    LOG.info("Before test execution");
    UTIL.ensureSomeRegionServersAvailable(1);
    LOG.info("Before test execution done");
  }

  @Test
  public void testFlushWithHFileV4() throws IOException {
    LOG.info("testFlushWithHFileV4");
    String tableName = "testpbe";
    HTableDescriptor htd = new HTableDescriptor(TableName.valueOf(tableName));
    HColumnDescriptor hcd = new HColumnDescriptor(FAMILY);
    String algorithm = UTIL.getConfiguration().get(
      HConstants.CRYPTO_KEY_ALGORITHM_CONF_KEY, HConstants.CIPHER_AES);
    hcd.setEncryptionType(algorithm);
    htd.addFamily(hcd);
    htd.setPbePrefixLength(4);
    UTIL.getAdmin().createTable(htd);
    Table table = UTIL.getConnection().getTable(htd.getTableName());
    Put put = new Put("row1".getBytes());
    put.addColumn(Bytes.toBytes(FAMILY), "c1".getBytes(), "value1".getBytes());
    table.put(put);
    put = new Put("row2".getBytes());
    put.addColumn(Bytes.toBytes(FAMILY), "c1".getBytes(), "value2".getBytes());
    table.put(put);
    LOG.info("Added a new row, going for flush");
    // Flush is expected to fail as after flush, file is read to validate and reader hasn't been
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
}
