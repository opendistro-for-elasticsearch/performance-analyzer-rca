package com.amazon.opendistro.elasticsearch.performanceanalyzer.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerTaskException;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.PluginSettings;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.TroubleshootingConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;
import java.util.concurrent.BlockingQueue;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class PerformanceAnalyzerReaderTask extends BaseThreadTask {

  private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerReaderTask.class);

  private static final String TASK_NAME = "pa-reader";

  private boolean shouldRun = false;

  public PerformanceAnalyzerReaderTask(
      BlockingQueue<PerformanceAnalyzerTaskException> exceptionQueue) {
    super(exceptionQueue);
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
    return shouldRun;
  }

  @Override
  public void run() {
    final PluginSettings settings = PluginSettings.instance();
    while (shouldRun) {
      try {
        ReaderMetricsProcessor mp =
            new ReaderMetricsProcessor(settings.getMetricsLocation(), true);
        ReaderMetricsProcessor.setCurrentInstance(mp);
        mp.run();
      } catch (Throwable e) {
        LOG.error("{} encountered an exception: {}", TASK_NAME, e.getCause());
        if (TroubleshootingConfig.getEnableDevAssert()) {
          break;
        }
        LOG.error(
            "Error in ReaderMetricsProcessor...restarting, ExceptionCode: {}",
            StatExceptionCode.READER_RESTART_PROCESSING.toString());
        StatsCollector.instance()
                      .logException(StatExceptionCode.READER_RESTART_PROCESSING);
        if (!this.exceptionQueue.offer(new PerformanceAnalyzerTaskException(e, TASK_NAME))) {
          LOG.error("Couldn't update the queue with the exception. Task: {} will now fail.",
              TASK_NAME);
          shouldRun = false;
        }
      }
    }
  }
}
