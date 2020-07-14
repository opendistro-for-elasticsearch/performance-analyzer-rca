package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.remediation;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.EsConfigNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.NodeConfigFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.HashMap;
import java.util.function.Function;
import java.util.function.Supplier;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Record;
import org.jooq.Result;

/**
 * This is a node level collector in RCA graph which collect the current config settings of PerformanceControllor
 * PerformanceController is a ES plugin that helps with cache/queue auto tuning and this collector collect configs
 * set by PerformanceController and pass them down to Decision Maker for the next round of resource auto-tuning.
 */
public class NodeConfigCollector extends EsConfigNode {

  private static final Logger LOG = LogManager.getLogger(NodeConfigCollector.class);
  private final ThreadPool_QueueCapacity threadPool_queueCapacity;
  private final int rcaPeriod;
  private int counter;
  private final HashMap<Resource, Double> configResult;

  public NodeConfigCollector(int rcaPeriod, ThreadPool_QueueCapacity threadPool_queueCapacity) {
    this.threadPool_queueCapacity = threadPool_queueCapacity;
    this.rcaPeriod = rcaPeriod;
    this.counter = 0;
    this.configResult = new HashMap<>();
  }

  private void collectQueueCapacity(MetricFlowUnit flowUnit) {
    double writeQueueCapacity = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
        THREAD_POOL_TYPE.getField(), ThreadPoolType.WRITE.toString(), MetricsDB.MAX);
    if (!Double.isNaN(writeQueueCapacity)) {
      configResult.put(ResourceUtil.WRITE_QUEUE_CAPACITY, writeQueueCapacity);
    }
    else {
      LOG.error("write queue capacity is NaN");
    }
    double searchQueueCapacity = SQLParsingUtil.readDataFromSqlResult(flowUnit.getData(),
        THREAD_POOL_TYPE.getField(), ThreadPoolType.SEARCH.toString(), MetricsDB.MAX);
    if (!Double.isNaN(searchQueueCapacity)) {
      configResult.put(ResourceUtil.SEARCH_QUEUE_CAPACITY, searchQueueCapacity);
    }
    else {
      LOG.error("search queue capacity is NaN");
    }
  }

  /**
   * collect config settings from the upstream metric flowunits and set them into the protobuf
   * message PerformanceControllerConfiguration. This will allow us to serialize / de-serialize
   * the config settings across grpc and send them to Decision Maker on elected master.
   * @return ResourceFlowUnit with HotNodeSummary. And HotNodeSummary carries PerformanceControllerConfiguration
   */
  @Override
  public NodeConfigFlowUnit operate() {
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
      configResult.values().forEach(nodeSummary::appendNestedSummary);
      return new NodeConfigFlowUnit(System.currentTimeMillis(), nodeSummary);
    }
    else {
      return new NodeConfigFlowUnit(System.currentTimeMillis());
    }
  }
}
