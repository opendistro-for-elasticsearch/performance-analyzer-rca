package com.amazon.opendistro.elasticsearch.performanceanalyzer;

public class PerformanceAnalyzerTaskException {
  private final Throwable innerThrowable;
  private final String taskName;

  public PerformanceAnalyzerTaskException(Throwable innerThrowable, String taskName) {
    this.innerThrowable = innerThrowable;
    this.taskName = taskName;
  }

  public Throwable getThrowable() {
    return this.innerThrowable;
  }

  public String getTaskName() {
    return this.taskName;
  }
}
