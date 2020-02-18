package com.amazon.opendistro.elasticsearch.performanceanalyzer;

public class PerformanceAnalyzerThreadException {
  private final Throwable innerThrowable;
  private final String threadName;

  public PerformanceAnalyzerThreadException(Throwable innerThrowable, String threadName) {
    this.innerThrowable = innerThrowable;
    this.threadName = threadName;
  }

  public Throwable getThrowable() {
    return this.innerThrowable;
  }

  public String getThreadName() {
    return this.threadName;
  }
}
