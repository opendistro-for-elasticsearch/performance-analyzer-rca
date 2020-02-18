package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerTaskException;
import java.util.concurrent.BlockingQueue;

public abstract class BaseThreadTask implements ControllableTask {
  protected final BlockingQueue<PerformanceAnalyzerTaskException> exceptionQueue;

  protected BaseThreadTask(
      final BlockingQueue<PerformanceAnalyzerTaskException> exceptionQueue) {
    this.exceptionQueue = exceptionQueue;
  }
}
