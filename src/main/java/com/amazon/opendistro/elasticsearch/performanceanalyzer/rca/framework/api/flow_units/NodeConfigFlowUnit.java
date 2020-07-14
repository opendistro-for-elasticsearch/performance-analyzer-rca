package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage.SummaryOneofCase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.Resource;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class NodeConfigFlowUnit extends ResourceFlowUnit<HotNodeSummary> {

  private static final Logger LOG = LogManager.getLogger(NodeConfigFlowUnit.class);
  private final HashMap<Resource, HotResourceSummary> configMap;

  public NodeConfigFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public NodeConfigFlowUnit(long timeStamp, HotNodeSummary summary) {
    super(timeStamp, new ResourceContext(Resources.State.HEALTHY), summary, false);
    configMap = new HashMap<>();
  }

  public void addConfig(Resource resource, double value) {
    HotResourceSummary configSummary = new HotResourceSummary(resource, Double.NaN, value, 0);
    configMap.put(resource, configSummary);
  }

  public double readConfig(Resource resource) {
    HotResourceSummary configSummary = configMap.getOrDefault(resource, null);
    if (configSummary == null) {
      return Double.NaN;
    }
    return configSummary.getValue();
  }

  /**
   * parse the "oneof" section in protocol buffer call the corresponding object build function for
   * each summary type
   */
  public static NodeConfigFlowUnit buildFlowUnitFromWrapper(final FlowUnitMessage message) {
    //if the flowunit is empty. empty flowunit does not have context
    if (message.hasResourceContext()) {
      ResourceContext newContext = ResourceContext
          .buildResourceContextFromMessage(message.getResourceContext());
      HotNodeSummary newSummary = null;
      try {
        if (message.getSummaryOneofCase() == SummaryOneofCase.HOTNODESUMMARY) {
          newSummary = HotNodeSummary
              .buildHotNodeSummaryFromMessage(message.getHotNodeSummary());
        } else {
          throw new IllegalArgumentException();
        }
      } catch (Exception e) {
        // we are not supposed to run into this unless we specified wrong summary template
        // for this function. Make sure the summary type passed in as template are consistent
        // between serialization and de-serializing.
        LOG.error("RCA: casting to wrong summary type when de-serializing this flowunit");
      }
      return new NodeConfigFlowUnit(message.getTimeStamp(), newSummary);
    } else {
      //empty flowunit;
      return new NodeConfigFlowUnit(message.getTimeStamp());
    }
  }
}
