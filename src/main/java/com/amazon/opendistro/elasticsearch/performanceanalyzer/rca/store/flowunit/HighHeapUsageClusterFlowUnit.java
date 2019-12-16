package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import java.util.List;

public class HighHeapUsageClusterFlowUnit extends ResourceFlowUnit {

  public HighHeapUsageClusterFlowUnit(long timeStamp) {
    super(timeStamp);
  }

  public HighHeapUsageClusterFlowUnit(long timeStamp,
      ResourceContext context) {
    super(timeStamp, context);
  }

  public HighHeapUsageClusterFlowUnit(long timeStamp, List<List<String>> data,
      ResourceContext context) {
    super(timeStamp, data, context);
  }

  public static HighHeapUsageClusterFlowUnit buildFlowUnitFromWrapper(final FlowUnitWrapper wrapper) {
    if (wrapper.hasData()) {
      return new HighHeapUsageClusterFlowUnit(
          wrapper.getTimeStamp(), wrapper.getData(), wrapper.getResourceContext());
    } else {
      return new HighHeapUsageClusterFlowUnit(wrapper.getTimeStamp(), wrapper.getResourceContext());
    }
  }
}
