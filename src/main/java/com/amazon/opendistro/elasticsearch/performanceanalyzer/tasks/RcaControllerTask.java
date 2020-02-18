package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerTaskException;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RcaControllerTask extends BaseThreadTask {

  private static final Logger LOG = LogManager.getLogger(RcaControllerTask.class);
  private static final String TASK_NAME = "rca-controller";
  private final RcaController rcaController;
  private final int interval;

  private boolean shouldRun = false;

  public RcaControllerTask(final RcaController rcaController, final int stateCheckInterval,
      final BlockingQueue<PerformanceAnalyzerTaskException> exceptionQueue) {
    super(exceptionQueue);
    this.rcaController = rcaController;
    this.interval = stateCheckInterval;
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
    while (shouldRun) {
      try {
        long startTime = System.currentTimeMillis();
        rcaController.readRcaEnabledFromConf();
        if (rcaController.isRcaEnabled()) {
          final NodeDetails nodeDetails = ClusterDetailsEventProcessor.getCurrentNodeDetails();
          if (nodeDetails != null) {
            rcaController.handleNodeRoleChange(nodeDetails);
          }
        }

        rcaController.checkAndUpdateSchedulerState();
        long duration = System.currentTimeMillis() - startTime;
        if (duration < interval) {
          Thread.sleep(interval - duration);
        }
      } catch (Throwable e) {
        LOG.error("{} Encountered Exception: {}", TASK_NAME, e.getCause());
        LOG.error(e);
        if (!this.exceptionQueue.offer(new PerformanceAnalyzerTaskException(e, TASK_NAME))) {
          LOG.error("Couldn't update the queue with the exception. Task: {} will now fail.",
              TASK_NAME);
          shouldRun = false;
        }
      }
    }
  }
}
