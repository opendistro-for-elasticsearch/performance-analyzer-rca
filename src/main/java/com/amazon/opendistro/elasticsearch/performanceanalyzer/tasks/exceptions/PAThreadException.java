package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.exceptions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerThreads;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;

public class PAThreadException extends Exception {

  private final PerformanceAnalyzerThreads paThread;

  private Throwable innerThrowable;

  public PAThreadException(final PerformanceAnalyzerThreads paThread, final Throwable throwable) {
    this.paThread = paThread;
    this.innerThrowable = throwable;
  }

  public String getPaThreadName() {
    return paThread.toString();
  }

  public StatExceptionCode getExceptionCode() {
    return paThread.getThreadExceptionCode();
  }

  public Throwable getInnerThrowable() {
    return innerThrowable;
  }
}
