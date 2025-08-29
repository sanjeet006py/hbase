/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package org.apache.hadoop.hbase.mapreduce;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseClassTestRule;
import org.apache.hadoop.hbase.HBaseTestingUtil;
import org.apache.hadoop.hbase.testclassification.MediumTests;
import org.apache.hadoop.mapreduce.JobContext;
import org.apache.hadoop.mapreduce.OutputCommitter;
import org.apache.hadoop.mapreduce.TaskAttemptContext;
import org.junit.AfterClass;
import org.junit.Assert;
import org.junit.BeforeClass;
import org.junit.ClassRule;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.mockito.Mockito;

@Category(MediumTests.class)
public class TestTableOutputFormat {
  @ClassRule
  public static final HBaseClassTestRule CLASS_RULE =
    HBaseClassTestRule.forClass(TestTableOutputFormat.class);

  private static final HBaseTestingUtil util = new HBaseTestingUtil();
  private static Configuration conf;
  private static TaskAttemptContext context;
  private static TableOutputFormat tableOutputFormat;

  @BeforeClass
  public static void setUp() throws Exception {
    util.startMiniCluster();
    conf = new Configuration(util.getConfiguration());
    context = Mockito.mock(TaskAttemptContext.class);
    tableOutputFormat = new TableOutputFormat();
    conf.set(TableOutputFormat.OUTPUT_TABLE, "TEST_TABLE");
  }

  @AfterClass
  public static void tearDown() throws Exception {
    util.shutdownMiniCluster();
  }

  @Test
  public void testOutputCommitterConfiguration() throws IOException, InterruptedException {
    // 1. Verify it returns the default committer when the property is not set.
    conf.unset(TableOutputFormat.OUTPUT_COMMITTER_CLASS);
    tableOutputFormat.setConf(conf);
    Assert.assertEquals("Should use default committer", TableOutputCommitter.class,
      tableOutputFormat.getOutputCommitter(context).getClass());

    // 2. Verify it returns the custom committer when the property is set.
    conf.set(TableOutputFormat.OUTPUT_COMMITTER_CLASS, DummyCommitter.class.getName());
    tableOutputFormat.setConf(conf);
    Assert.assertEquals("Should use custom committer", DummyCommitter.class,
      tableOutputFormat.getOutputCommitter(context).getClass());
  }

  // Simple dummy committer for testing
  public static class DummyCommitter extends OutputCommitter {
    @Override
    public void setupJob(JobContext jobContext) {
    }

    @Override
    public void setupTask(TaskAttemptContext taskContext) {
    }

    @Override
    public boolean needsTaskCommit(TaskAttemptContext taskContext) {
      return false;
    }

    @Override
    public void commitTask(TaskAttemptContext taskContext) {
    }

    @Override
    public void abortTask(TaskAttemptContext taskContext) {
    }
  }
}
