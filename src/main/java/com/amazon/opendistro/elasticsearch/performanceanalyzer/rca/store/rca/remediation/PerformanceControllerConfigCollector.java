package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.remediation;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PerformanceControllerConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.EsConfigNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a node level collector in RCA graph which collect the current config settings of PerformanceControllor
 * PerformanceController is a ES plugin that helps with cache/queue auto tuning and this collector collect configs
 * set by PerformanceController and pass them down to Decision Maker for the next round of resource auto-tuning.
 */
public class PerformanceControllerConfigCollector extends EsConfigNode {

  private static final Logger LOG = LogManager.getLogger(PerformanceControllerConfigCollector.class);
  private final Metric threadPool_queueCapacity;
  private final int rcaPeriod;
  private int counter;
  private int writeQueueCapacity;
  private int searchQueueCapacity;

  public <M extends Metric> PerformanceControllerConfigCollector(int rcaPeriod, M threadPool_queueCapacity) {
    this.threadPool_queueCapacity = threadPool_queueCapacity;
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.writeQueueCapacity = -1;
    this.searchQueueCapacity = -1;
  }

  private void collectQueueCapacity(MetricFlowUnit flowUnit) {
    double writeQueueCapacity = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
        THREAD_POOL_TYPE.getField(), ThreadPoolType.WRITE.toString(), MetricsDB.MAX);
    if (!Double.isNaN(writeQueueCapacity)) {
      this.writeQueueCapacity = (int) writeQueueCapacity;
    }
    double searchQueueCapacity = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
        THREAD_POOL_TYPE.getField(), ThreadPoolType.SEARCH.toString(), MetricsDB.MAX);
    if (!Double.isNaN(searchQueueCapacity)) {
      this.searchQueueCapacity = (int) searchQueueCapacity;
    }
  }

  /**
   * collect config settings from the upstream metric flowunits and set them into the protobuf
   * message PerformanceControllerConfiguration. This will allow us to serialize / de-serialize
   * the config settings across grpc and send them to Decision Maker on elected master.
   * @return ResourceFlowUnit with HotNodeSummary. And HotNodeSummary carries PerformanceControllerConfiguration
   */
  @Override
  public ResourceFlowUnit<HotNodeSummary> operate() {
    counter += 1;
    for (MetricFlowUnit flowUnit : threadPool_queueCapacity.getFlowUnits()) {
      if (flowUnit.isEmpty()) {
        continue;
      }
      collectQueueCapacity(flowUnit);
    }
    if (counter == rcaPeriod) {
      counter = 0;
      ClusterDetailsEventProcessor.NodeDetails currentNode = ClusterDetailsEventProcessor
          .getCurrentNodeDetails();
      HotNodeSummary nodeSummary = new HotNodeSummary(currentNode.getId(), currentNode.getHostAddress());
      nodeSummary.setPerformanceControllerConfiguration(PerformanceControllerConfiguration.newBuilder()
              .setWriteQueueCapacity(writeQueueCapacity)
              .setSearchQueueCapacity(searchQueueCapacity)
              .build());
      return new ResourceFlowUnit<>(System.currentTimeMillis(), new ResourceContext(Resources.State.HEALTHY), nodeSummary);
    }
    else {
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }
  }
}
