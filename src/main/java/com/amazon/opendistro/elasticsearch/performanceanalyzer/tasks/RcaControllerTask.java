package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RcaControllerTask implements ControllableTask {

  private static final Logger LOG = LogManager.getLogger(RcaControllerTask.class);
  private static final String TASK_NAME = "rca-controller";

  private boolean shouldRun = false;
  /**
   * Gets the name of this task
   *
   * @return The name of this task.
   */
  @Override
  public String getName() {
    return TASK_NAME;
  }

  /**
   * Sets the run state of this task. A {@param shouldRun} value of true implies that the task will
   * start or continue to run if already started, and a value of false will make the task stop
   * executing if it's currently running, or it will not start if it has not started yet.
   *
   * @param shouldRun boolean value indicating if this task should run or not.
   */
  @Override
  public void setRunState(boolean shouldRun) {
    this.shouldRun = shouldRun;
  }

  /**
   * Gets the current run state of the task.
   *
   * @return The current run state of the task.
   */
  @Override
  public boolean getRunState() {
    return this.shouldRun;
  }

  /**
   * Unlike the {@link Runnable#run()} method, this method will allow for throwing exceptions.
   * Exceptions thrown from here are caught by the runner's run method and signalled to the top
   * level thread.
   *
   * @throws Throwable the exception encountered while executing the task.
   */
  @Override
  public void run() throws Throwable {

  }
}
