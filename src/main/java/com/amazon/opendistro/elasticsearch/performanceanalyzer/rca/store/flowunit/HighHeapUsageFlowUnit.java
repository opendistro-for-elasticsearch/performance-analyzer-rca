package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import java.util.List;

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

  public static HighHeapUsageFlowUnit buildFlowUnitFromWrapper(final FlowUnitWrapper wrapper) {
    if (wrapper.hasData()) {
      return new HighHeapUsageFlowUnit(
          wrapper.getTimeStamp(), wrapper.getData(), wrapper.getResourceContext());
    } else {
      return new HighHeapUsageFlowUnit(wrapper.getTimeStamp(), wrapper.getResourceContext());
    }
  }
}
