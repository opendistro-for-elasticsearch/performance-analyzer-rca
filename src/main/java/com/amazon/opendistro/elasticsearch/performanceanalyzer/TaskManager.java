package com.amazon.opendistro.elasticsearch.performanceanalyzer;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks.ControllableTask;
import java.util.HashMap;
import java.util.Map;
import java.util.Queue;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class TaskManager {

  private static final Logger LOG = LogManager.getLogger(TaskManager.class);
  private final BlockingQueue<PerformanceAnalyzerThreadException> concurrentExceptionQueue;

  private Map<String, ControllableTask> nameToTaskLookup = new HashMap<>();
  private Map<String, Thread> nameToThreadLookup = new HashMap<>();
  private int nThreads = 0;

  public TaskManager(final BlockingQueue<PerformanceAnalyzerThreadException> concurrentExceptionQueue) {
    this.concurrentExceptionQueue = concurrentExceptionQueue;
  }

  public synchronized void submit(final ControllableTask task) {
    Thread t = new Thread(() -> {
      try {
        task.setRunState(true);
        task.run();
      } catch (Throwable throwable) {
        if (!concurrentExceptionQueue
            .offer(new PerformanceAnalyzerThreadException(throwable, task.getName()))) {
          LOG.error("Encountered exception: {}, however exception queue was full, so dropping the"
              + " exception!", throwable.getMessage());
          LOG.error(throwable);
          task.setRunState(false);
        }
      }
    });

    t.setName((task.getName()));
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
        PerformanceAnalyzerThreadException e = concurrentExceptionQueue.poll(1, TimeUnit.SECONDS);
        Thread.sleep(4000);
        if (e != null) {
          handleException(e);
        } else {
          if (allThreadsDead()) {
            LOG.info("All PerformanceAnalyzer agent threads are dead. Agent will terminate now.");
            break;
          }
        }
      } catch (InterruptedException e) {
        Thread.currentThread().interrupt();
        LOG.error("TaskManager was interrupted while waiting for exceptions: {}", e.getMessage());
        LOG.error(e);
      }
    }
  }

  private void handleException(PerformanceAnalyzerThreadException e) {
    LOG.error(e.getThreadName(), e.getThrowable());
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
