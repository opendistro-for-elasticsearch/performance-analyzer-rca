/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.threads;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.exceptions.PAThreadException;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class that wraps a given runnable in a thread with exception handling capabilities.
 */
public class ThreadProvider {

  private static final Logger LOG = LogManager.getLogger(ThreadProvider.class);
  private static final String PA_THREADS_STARTED_METRIC_NAME = "NumberOfPAThreadsStarted";
  private static final String PA_THREADS_ENDED_METRIC_NAME = "NumberOfPAThreadsEnded";

  /**
   * Creates a thread which executes the given runnable when started. If the given runnable throws
   * an uncaught exception, it is then written to the exception queue which will be processed by the
   * exception handler thread.
   *
   * @param innerRunnable The runnable to execute when the thread starts.
   * @param paThread      The thread enum value from {@link PerformanceAnalyzerThreads}
   * @return The thread with the wrapped runnable.
   */
  public Thread createThreadForRunnable(final Runnable innerRunnable,
      final PerformanceAnalyzerThreads paThread, String threadNameAppender) {
    StringBuilder threadName = new StringBuilder(paThread.toString());
    if (!threadNameAppender.isEmpty()) {
      threadName.append("-").append(threadNameAppender);
    }
    String threadNameStr = threadName.toString();

    Thread t = new Thread(() -> {
      try {
        innerRunnable.run();
      } catch (Throwable innerThrowable) {
        LOG.error("A thread crashed: ", innerThrowable);
        try {
          PerformanceAnalyzerApp.exceptionQueue.put(new PAThreadException(paThread,
              innerThrowable));
        } catch (InterruptedException e) {
          LOG.error("Thread was interrupted while waiting to put an exception into the queue. "
              + "Message: {}", e.getMessage(), e);
        }
      }
      StatsCollector.instance().logMetric(PA_THREADS_ENDED_METRIC_NAME);
      LOG.info("Thread: {} completed.", threadNameStr);
    }, threadNameStr);

    LOG.info("Spun up a thread with name: {}", threadNameStr);
    StatsCollector.instance().logMetric(PA_THREADS_STARTED_METRIC_NAME);
    return t;
  }

  public Thread createThreadForRunnable(final Runnable innerRunnable,
                                        final PerformanceAnalyzerThreads paThread) {
    return createThreadForRunnable(innerRunnable, paThread, "");
  }
}
