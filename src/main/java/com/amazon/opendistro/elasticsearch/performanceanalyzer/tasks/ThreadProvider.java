package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.exceptions.PAThreadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ThreadProvider {

  private static final Logger LOG = LogManager.getLogger(ThreadProvider.class);
  private static volatile ThreadProvider _instance = null;
  private int numberOfThreadsSpunUp = 0;

  /**
   * Empty private default ctor to prevent instantiation.
   */
  private ThreadProvider() {
  }

  public static ThreadProvider instance() {
    if (_instance != null) {
      return _instance;
    }

    _instance = new ThreadProvider();
    return _instance;
  }

  public Thread createThreadForRunnable(final Runnable innerRunnable, final String name) {
    Thread t = new Thread(() -> {
      try {
        innerRunnable.run();
      } catch (Throwable innerThrowable) {
        if (!PerformanceAnalyzerApp.exceptionQueue.offer(new PAThreadException(name,
            innerThrowable))) {
          LOG.error("Unable to write to exception queue. Dropping exception: {}",
              innerThrowable.getMessage(), innerThrowable.getCause());
        }
      }
    });

    LOG.info("Spun up a thread with name: {}", name);
    this.numberOfThreadsSpunUp++;

    return t;
  }
}
