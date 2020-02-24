package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.exceptions.PAThreadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadProvider {

  private static final Logger LOG = LogManager.getLogger(ThreadProvider.class);
  private static final String PA_THREADS_SPUN_METRIC_NAME = "NumberOfPAThreads";

  public synchronized Thread createThreadForRunnable(final Runnable innerRunnable,
      final PerformanceAnalyzerThreads paThread) {
    Thread t = new Thread(() -> {
      try {
        innerRunnable.run();
      } catch (Throwable innerThrowable) {
        try {
          PerformanceAnalyzerApp.exceptionQueue.put(new PAThreadException(paThread,
              innerThrowable));
        } catch (InterruptedException e) {
          LOG.error("Thread was interrupted while waiting to put an exception into the queue. "
              + "Message: {}", e.getMessage(), e);
        }
      }
    });

    LOG.info("Spun up a thread with name: {}", paThread.toString());
    StatsCollector.instance().logMetric(PA_THREADS_SPUN_METRIC_NAME);
    return t;
  }
}
