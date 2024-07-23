package org.apache.hadoop.hbase.client;

import org.apache.hadoop.hbase.ipc.RpcControllerFactory;
import org.apache.hadoop.hbase.metrics.Snapshot;
import org.apache.hadoop.hbase.metrics.impl.TimerImpl;
import org.apache.hbase.thirdparty.com.google.common.base.Preconditions;
import org.apache.hbase.thirdparty.com.google.common.base.Stopwatch;
import org.apache.yetus.audience.InterfaceAudience;
import org.apache.yetus.audience.InterfaceStability;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.Closeable;
import java.io.IOException;
import java.io.InterruptedIOException;
import java.util.Arrays;
import java.util.concurrent.TimeUnit;

/**
 * This class is similar to BufferedMutatorImpl, except that it throttles itself,
 * based on the measured latency. It keeps track of the latency observed during last flush,
 * and if that exceeds a baseline value (in ms), this implementation sleeps twice that value.
 * This allows the client to aggressively throttle itself, in case of a latency spike.
 * The baseline value depends on the environment & can be overridden via configuration.
 * By default, 50 ms is assumed as an average baseline value. When it closes, it logs
 * the histogram of latencies observed during its operation.
 */
@InterfaceAudience.Public
@InterfaceStability.Evolving
public class ThrottledBufferedMutatorImpl extends BufferedMutatorImpl {
  private static final Logger LOG = LoggerFactory.getLogger(ThrottledBufferedMutatorImpl.class);
  private LatencyMeasurement tracker;
  private long totalSleepTime = 0;

  private long baselineLatencyMs = 50;

  public ThrottledBufferedMutatorImpl(ClusterConnection conn, BufferedMutatorParams params,
                                      AsyncProcess ap) {
    super(conn, params, ap);
    tracker = new LatencyMeasurement();
    baselineLatencyMs = Long.parseLong(super.getConfiguration().get("hbase.client.throttledmutator.baseline", String.valueOf(50)));
    LOG.info("Instantiated ThrottledBufferedMutatorImpl.class");
  }

  public ThrottledBufferedMutatorImpl(ClusterConnection conn, RpcRetryingCallerFactory rpcCallerFactory,
                                      RpcControllerFactory rpcFactory, BufferedMutatorParams params) {
    super(conn, rpcCallerFactory, rpcFactory, params);
    tracker = new LatencyMeasurement();
    baselineLatencyMs = Long.parseLong(super.getConfiguration().get("hbase.client.throttledmutator.baseline", String.valueOf(50)));
    LOG.info("Instantiated ThrottledBufferedMutatorImpl.class");
  }

  @Override
  protected void doFlush(boolean flushAll)
    throws InterruptedIOException, RetriesExhaustedWithDetailsException {

    final boolean drainBuffers = flushAll || (getCurrentWriteBufferSize() > getWriteBufferSize());
    if (drainBuffers) {
      //slow down
      throttle();
    }

    // measure flush latency
    try (TimedBlock block = new TimedBlock(drainBuffers, this.tracker)) {
      super.doFlush(flushAll);
    } catch (RetriesExhaustedWithDetailsException | InterruptedIOException e) {
      //propagate any exception thrown by the BufferedMutatorImpl
      throw e;
    } catch (IOException e) {
      LOG.warn("Unable to close timer");
    }
  } //doFlush

  /**
   * This makes the calling thread to sleep as a throttle measure.
   */
  protected void throttle() {
    final long sleepDuration = getSleepDuration();
    try {
      Thread.sleep(sleepDuration);
      this.totalSleepTime += sleepDuration;
    } catch (InterruptedException e) {
      LOG.warn("Sleep Thread aborted: ", e);
    }
  }

  /**
   * @return sleep duration to sleep for throttling
   */
  protected long getSleepDuration() {
    long sleepDuration = 0;

    /**
     * A very simple throttling approach: If the latency is above a baseline,
     * just sleep twice that amount.
     * This allows for a simple adaptive slowdown of clients based on the latency they observe.
     */
    if (tracker.lastLatencyMs > baselineLatencyMs) {
      sleepDuration = 2 * tracker.lastLatencyMs;
    }

    return sleepDuration;
  }

  /**
   * @return Overall how much was the sleep duration for throttling so far (in ms).
   */
  public long getTotalSleepTime() {
    return this.totalSleepTime;
  }

  public Snapshot getLatencyDistribution() {
    return this.tracker.timer.getHistogram().snapshot();
  }

  @Override
  public synchronized void close() throws IOException {
    String quorum = super.getConfiguration().get("hbase.mapred.output.quorum");
    quorum = Arrays.stream(quorum.split(",")).findFirst().get();

    final Snapshot latencyHist = getLatencyDistribution();

    LOG.info("Latency info for table: {} target: {} sleep(ms): {} min: {} max: {} mean: {} median: {} count: {}",
            super.getName().getNameAsString(), quorum == null? "": quorum,
            totalSleepTime, latencyHist.getMin(), latencyHist.getMax(), latencyHist.getMean(), latencyHist.getMedian(),
            latencyHist.getCount());

    LOG.info("Latency percentile for table: {} target: {} p_50: {} p_75: {} p_90: {} p_95: {} p_99: {}",
            super.getName().getNameAsString(), quorum == null? "": quorum,
            latencyHist.getMedian(), latencyHist.get75thPercentile(), latencyHist.get90thPercentile(),
            latencyHist.get95thPercentile(), latencyHist.get99thPercentile());

    super.close();
  }

  /**
   * A utility class to track 2 measurements:
   * 1. last latency of an op
   * 2. A historgram of latencies
   */
  private class LatencyMeasurement {
    private long lastLatencyMs = 0;
    private TimerImpl timer = new TimerImpl();
  }

  /**
   * A utility class to track time spent in a code block
   */
  private class TimedBlock implements Closeable {
    Stopwatch watch = null;
    LatencyMeasurement tracker = null;

    TimedBlock(boolean trackTime, LatencyMeasurement tracker) {
      Preconditions.checkNotNull(tracker);
      if (trackTime) {
        watch = Stopwatch.createStarted();
        this.tracker = tracker;
      }
    }
    @Override
    public void close() throws IOException {
      if (watch != null) {
        final long elapsedMs = watch.stop().elapsed(TimeUnit.MILLISECONDS);
        this.tracker.lastLatencyMs = elapsedMs;
        this.tracker.timer.update(elapsedMs, TimeUnit.MILLISECONDS);
      }
    } //close
  } //TimedBlock

}
