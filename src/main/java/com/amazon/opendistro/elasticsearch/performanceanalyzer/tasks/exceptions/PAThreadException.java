package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.exceptions;

public class PAThreadException extends Exception {

  private final String threadName;

  private Throwable innerThrowable;

  public PAThreadException(final String threadName, final Throwable throwable) {
    this.threadName = threadName;
    this.innerThrowable = throwable;
  }

  public String getThreadName() {
    return threadName;
  }

  public Throwable getInnerThrowable() {
    return innerThrowable;
  }
}
