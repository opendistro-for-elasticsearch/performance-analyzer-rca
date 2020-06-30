package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import java.util.ArrayList;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * this is a base class for node(vertex) in RCA graph that reads configuration settings from ES.
 */
public abstract class EsConfigNode extends NonLeafNode<ResourceFlowUnit<HotNodeSummary>> {

  private static final Logger LOG = LogManager.getLogger(EsConfigNode.class);

  public EsConfigNode() {
    super(0, 5);
  }

  /**
   * fetch flowunits from local graph node
   *
   * @param args The wrapper around the flow unit operation.
   */
  @Override
  public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
    long startTime = System.currentTimeMillis();

    ResourceFlowUnit<HotNodeSummary> result;
    try {
      result = this.operate();
    } catch (Exception ex) {
      LOG.error("Exception in operate.", ex);
      PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
          ExceptionsAndErrors.EXCEPTION_IN_OPERATE, name(), 1);
      result = new ResourceFlowUnit<>(System.currentTimeMillis());
    }
    long endTime = System.currentTimeMillis();
    long duration = endTime - startTime;

    PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR.updateStat(
        RcaGraphMetrics.GRAPH_NODE_OPERATE_CALL, this.name(), duration);

    setLocalFlowUnit(result);
  }

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

  @Override
  public void handleNodeMuted() {
    setLocalFlowUnit(new ResourceFlowUnit<>(System.currentTimeMillis()));
  }

  /**
   * EsConfig metrics are not intended to be persisted
   * @param args FlowUnitOperationArgWrapper
   */
  @Override
  public void persistFlowUnit(FlowUnitOperationArgWrapper args) {
  }
}
