package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.remediation;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.NodeConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist.SQLParsingUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeConfigurationRca extends Rca<ResourceFlowUnit<HotNodeSummary>> {

  private static final Logger LOG = LogManager.getLogger(NodeConfigurationRca.class);
  private final Metric threadPool_queueCapacity;
  private final int rcaPeriod;
  private int counter;
  private int writeQueueCapacity;
  private int searchQueueCapacity;

  public <M extends Metric> NodeConfigurationRca(int rcaPeriod, M threadPool_queueCapacity) {
    super(5);
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
      nodeSummary.setNodeConfiguration(NodeConfiguration.newBuilder()
              .setWriteQueueCapacity(writeQueueCapacity)
              .setSearchQueueCapacity(searchQueueCapacity)
              .build());
      return new ResourceFlowUnit<>(System.currentTimeMillis(), new ResourceContext(Resources.State.HEALTHY), nodeSummary);
    }
    else {
      return new ResourceFlowUnit<>(System.currentTimeMillis());
    }
  }

  // TODO: move this method back into the Rca base class
  @Override
  public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
    final List<FlowUnitMessage> flowUnitMessages =
        args.getWireHopper().readFromWire(args.getNode());
    List<ResourceFlowUnit<HotNodeSummary>> flowUnitList = new ArrayList<>();
    LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
    for (FlowUnitMessage flowUnitMessage : flowUnitMessages) {
      flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(flowUnitMessage));
    }
    setFlowUnits(flowUnitList);
  }
}
