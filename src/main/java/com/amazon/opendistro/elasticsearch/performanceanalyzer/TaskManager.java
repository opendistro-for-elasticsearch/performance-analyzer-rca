package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.ControllableTask;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TaskManager {

  private static final Logger LOG = LogManager.getLogger(TaskManager.class);
  private final BlockingQueue<PerformanceAnalyzerTaskException> concurrentExceptionQueue;
  private final int EXCEPTION_POLL_INTERVAL_IN_MS = 5000;

  private Map<String, ControllableTask> nameToTaskLookup = new HashMap<>();
  private Map<String, Thread> nameToThreadLookup = new HashMap<>();
  private int nThreads = 0;

  public TaskManager(
      final BlockingQueue<PerformanceAnalyzerTaskException> concurrentExceptionQueue) {
    this.concurrentExceptionQueue = concurrentExceptionQueue;
  }

  /**
   * Runs the task in a new thread. The thread that it runs in is a single-use thread in the sense
   * that if the task threw an exception that it doesn't itself handle, then the exception is
   * propagated to the top level thread. The top level thread can then decide how it wants to handle
   * that exception for the thread it spawned.
   *
   * @param task The task to be executed in a new thread.
   */
  public synchronized void submit(final ControllableTask task) {

    task.setRunState(true);
    Thread t = new Thread(task, task.getName());

    addThreadInfo(t, task);
    t.start();
  }

  private void addThreadInfo(final Thread thread, final ControllableTask task) {
    nThreads++;
    nameToTaskLookup.put(task.getName(), task);
    nameToThreadLookup.put(task.getName(), thread);
  }

  public void waitForExceptionsOrTermination() {
    while (true) {
      try {
        long startTime = System.currentTimeMillis();
        List<PerformanceAnalyzerTaskException> exceptionList = new ArrayList<>();
        concurrentExceptionQueue.drainTo(exceptionList);
        for (PerformanceAnalyzerTaskException e : exceptionList) {
          handleException(e);
        }
        long duration = System.currentTimeMillis() - startTime;
        if (duration < EXCEPTION_POLL_INTERVAL_IN_MS) {
          Thread.sleep(EXCEPTION_POLL_INTERVAL_IN_MS - duration);
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("TaskManager was interrupted while waiting for exceptions: {}", e.getMessage());
        LOG.error(e);
      }
    }
  }

  private void handleException(PerformanceAnalyzerTaskException e) {
    LOG.error(e.getTaskName(), e.getThrowable());
    LOG.error(e.getThrowable());
  }

  private boolean allThreadsDead() {
    for (Map.Entry<String, Thread> entry : nameToThreadLookup.entrySet()) {
      if (entry.getValue().isAlive()) {
        return false;
      }
    }

    return true;
  }
}
