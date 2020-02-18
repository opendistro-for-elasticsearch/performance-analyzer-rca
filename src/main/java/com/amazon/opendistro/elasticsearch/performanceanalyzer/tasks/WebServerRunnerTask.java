package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerTaskException;
import com.sun.net.httpserver.HttpServer;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WebServerRunnerTask extends BaseThreadTask {

  private static final Logger LOG = LogManager.getLogger(WebServerRunnerTask.class);
  private static final String TASK_NAME = "web-server";

  private boolean shouldRun = false;
  private final HttpServer server;

  public WebServerRunnerTask(final HttpServer server,
      final BlockingQueue<PerformanceAnalyzerTaskException> exceptionQueue) {
    super(exceptionQueue);
    this.server = server;
  }

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

  @Override
  public void run() {
    try {
      server.start();
    } catch (Throwable throwable) {
      LOG.error("{} encountered an exception: {}", TASK_NAME, throwable.getCause());
      if (!this.exceptionQueue.offer(new PerformanceAnalyzerTaskException(throwable, TASK_NAME))) {
        LOG.error("Couldn't update the queue with the exception. Task: {} will now fail.",
            TASK_NAME);
      }
    }
  }
}
