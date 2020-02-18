package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

public interface ControllableTask extends Runnable {

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
}
