package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import java.util.List;

/**
 * Class representing the flow unit for the HighHeapUsage RCA node.
 */
public class HighHeapUsageFlowUnit extends ResourceFlowUnit {

  public HighHeapUsageFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public HighHeapUsageFlowUnit(long timeStamp,
      ResourceContext context) {
    super(timeStamp, context);
  }

  public HighHeapUsageFlowUnit(long timeStamp, List<List<String>> data,
      ResourceContext context) {
    super(timeStamp, data, context);
  }

  /**
   * Unwraps the flowunit from the flow unit wrapper.
   * TODO: This will be removed once we start modeling flow units as protobuf messages.
   * @param wrapper The flow unit wrapper
   * @return The unwrapped HighHeapUsageFlowUnit object.
   */
  public static HighHeapUsageFlowUnit buildFlowUnitFromWrapper(final FlowUnitWrapper wrapper) {
    if (wrapper.hasData()) {
      return new HighHeapUsageFlowUnit(
          wrapper.getTimeStamp(), wrapper.getData(), wrapper.getResourceContext());
    } else {
      return new HighHeapUsageFlowUnit(wrapper.getTimeStamp(), wrapper.getResourceContext());
    }
  }
}
