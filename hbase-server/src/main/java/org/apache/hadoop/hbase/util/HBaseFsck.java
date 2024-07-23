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
package org.apache.hadoop.hbase.util;

import java.io.IOException;
import java.io.InterruptedIOException;
import java.net.InetAddress;
import java.util.concurrent.Callable;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.FutureTask;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hbase.HBaseInterfaceAudience;
import org.apache.hadoop.hbase.HConstants;
import org.apache.hadoop.hdfs.protocol.AlreadyBeingCreatedException;
import org.apache.hadoop.ipc.RemoteException;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * This is remaining HBCK1 functionality still used internally in HBase 2. Most of
 * the implementation of the HBCK1 tool has been removed to prevent operational
 * errors.
 * @deprecated For removal in hbase-4.0.0. Use HBCK2 instead.
 */
@Deprecated
@InterfaceAudience.LimitedPrivate(HBaseInterfaceAudience.TOOLS)
@InterfaceStability.Evolving
public class HBaseFsck {
  /**
   * Here is where hbase-1.x used to default the lock for hbck1.
   * It puts in place a lock when it goes to write/make changes.
   */
  @InterfaceAudience.Private
  public static final String HBCK_LOCK_FILE = "hbase-hbck.lock";

  private static final int DEFAULT_MAX_LOCK_FILE_ATTEMPTS = 5;
  private static final int DEFAULT_LOCK_FILE_ATTEMPT_SLEEP_INTERVAL = 200; // milliseconds
  private static final int DEFAULT_LOCK_FILE_ATTEMPT_MAX_SLEEP_TIME = 5000; // milliseconds
  // We have to set the timeout value > HdfsConstants.LEASE_SOFTLIMIT_PERIOD.
  // In HADOOP-2.6 and later, the Namenode proxy now created with custom RetryPolicy for
  // AlreadyBeingCreatedException which is implies timeout on this operations up to
  // HdfsConstants.LEASE_SOFTLIMIT_PERIOD (60 seconds).
  private static final int DEFAULT_WAIT_FOR_LOCK_TIMEOUT = 80; // seconds

  /**********************
   * Internal resources
   **********************/
  private static final Logger LOG = LoggerFactory.getLogger(HBaseFsck.class.getName());

  /**
   * @return A retry counter factory configured for retrying lock file creation.
   */
  public static RetryCounterFactory createLockRetryCounterFactory(Configuration conf) {
    return new RetryCounterFactory(
        conf.getInt("hbase.hbck.lockfile.attempts", DEFAULT_MAX_LOCK_FILE_ATTEMPTS),
        conf.getInt("hbase.hbck.lockfile.attempt.sleep.interval",
            DEFAULT_LOCK_FILE_ATTEMPT_SLEEP_INTERVAL),
        conf.getInt("hbase.hbck.lockfile.attempt.maxsleeptime",
            DEFAULT_LOCK_FILE_ATTEMPT_MAX_SLEEP_TIME));
  }

  /**
   * @return Return the tmp dir this tool writes too.
   */
  @InterfaceAudience.Private
  public static Path getTmpDir(Configuration conf) throws IOException {
    return new Path(CommonFSUtils.getRootDir(conf), HConstants.HBASE_TEMP_DIRECTORY);
  }

  private static class FileLockCallable implements Callable<FSDataOutputStream> {
    RetryCounter retryCounter;
    private final Configuration conf;
    private Path hbckLockPath = null;

    public FileLockCallable(Configuration conf, RetryCounter retryCounter) {
      this.retryCounter = retryCounter;
      this.conf = conf;
    }

    /**
     * @return Will be <code>null</code> unless you call {@link #call()}
     */
    Path getHbckLockPath() {
      return this.hbckLockPath;
    }

    @Override
    public FSDataOutputStream call() throws IOException {
      try {
        FileSystem fs = CommonFSUtils.getCurrentFileSystem(this.conf);
        FsPermission defaultPerms =
          CommonFSUtils.getFilePermissions(fs, this.conf, HConstants.DATA_FILE_UMASK_KEY);
        Path tmpDir = getTmpDir(conf);
        this.hbckLockPath = new Path(tmpDir, HBCK_LOCK_FILE);
        fs.mkdirs(tmpDir);
        final FSDataOutputStream out = createFileWithRetries(fs, this.hbckLockPath, defaultPerms);
        out.writeBytes(InetAddress.getLocalHost().toString());
        // Add a note into the file we write on why hbase2 is writing out an hbck1 lock file.
        out.writeBytes(" Written by an hbase-2.x Master to block an " +
            "attempt by an hbase-1.x HBCK tool making modification to state. " +
            "See 'HBCK must match HBase server version' in the hbase refguide.");
        out.flush();
        return out;
      } catch(RemoteException e) {
        if(AlreadyBeingCreatedException.class.getName().equals(e.getClassName())){
          return null;
        } else {
          throw e;
        }
      }
    }

    private FSDataOutputStream createFileWithRetries(final FileSystem fs,
        final Path hbckLockFilePath, final FsPermission defaultPerms)
        throws IOException {
      IOException exception = null;
      do {
        try {
          return CommonFSUtils.create(fs, hbckLockFilePath, defaultPerms, false);
        } catch (IOException ioe) {
          LOG.info("Failed to create lock file " + hbckLockFilePath.getName()
              + ", try=" + (retryCounter.getAttemptTimes() + 1) + " of "
              + retryCounter.getMaxAttempts());
          LOG.debug("Failed to create lock file " + hbckLockFilePath.getName(),
              ioe);
          try {
            exception = ioe;
            retryCounter.sleepUntilNextRetry();
          } catch (InterruptedException ie) {
            throw (InterruptedIOException) new InterruptedIOException(
                "Can't create lock file " + hbckLockFilePath.getName())
            .initCause(ie);
          }
        }
      } while (retryCounter.shouldRetry());

      throw exception;
    }
  }

  /**
   * This method maintains a lock using a file. If the creation fails we return null
   *
   * @return FSDataOutputStream object corresponding to the newly opened lock file
   * @throws IOException if IO failure occurs
   */
  public static Pair<Path, FSDataOutputStream> checkAndMarkRunningHbck(Configuration conf,
      RetryCounter retryCounter) throws IOException {
    FileLockCallable callable = new FileLockCallable(conf, retryCounter);
    ExecutorService executor = Executors.newFixedThreadPool(1);
    FutureTask<FSDataOutputStream> futureTask = new FutureTask<>(callable);
    executor.execute(futureTask);
    final int timeoutInSeconds = conf.getInt(
      "hbase.hbck.lockfile.maxwaittime", DEFAULT_WAIT_FOR_LOCK_TIMEOUT);
    FSDataOutputStream stream = null;
    try {
      stream = futureTask.get(timeoutInSeconds, TimeUnit.SECONDS);
    } catch (ExecutionException ee) {
      LOG.warn("Encountered exception when opening lock file", ee);
    } catch (InterruptedException ie) {
      LOG.warn("Interrupted when opening lock file", ie);
      Thread.currentThread().interrupt();
    } catch (TimeoutException exception) {
      // took too long to obtain lock
      LOG.warn("Took more than " + timeoutInSeconds + " seconds in obtaining lock");
      futureTask.cancel(true);
    } finally {
      executor.shutdownNow();
    }
    return new Pair<Path, FSDataOutputStream>(callable.getHbckLockPath(), stream);
  }

}
