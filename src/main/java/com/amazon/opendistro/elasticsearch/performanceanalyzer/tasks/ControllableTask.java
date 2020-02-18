package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

public interface ControllableTask {

  /**
   * Gets the name of this task
   * @return The name of this task.
   */
  String getName();

  /**
   * Sets the run state of this task. A {@param shouldRun} value of true implies that the task
   * will start or continue to run if already started, and a value of false will make the task
   * stop executing if it's currently running, or it will not start if it has not started yet.
   * @param shouldRun boolean value indicating if this task should run or not.
   */
  void setRunState(boolean shouldRun);

  /**
   * Gets the current run state of the task.
   * @return The current run state of the task.
   */
  boolean getRunState();

  /**
   * Unlike the {@link Runnable#run()} method, this method will allow for throwing exceptions.
   * Exceptions thrown from here are caught by the runner's run method and signalled to the top
   * level thread.
   *
   * @throws Throwable the exception encountered while executing the task.
   */
  void run() throws Throwable;
}
