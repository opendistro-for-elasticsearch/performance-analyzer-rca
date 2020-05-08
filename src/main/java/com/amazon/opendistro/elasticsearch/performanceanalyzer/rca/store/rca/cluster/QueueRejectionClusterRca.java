package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ThreadPoolEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import java.util.Arrays;

public class QueueRejectionClusterRca extends GenericClusterRca {
  public static final String RCA_TABLE_NAME = QueueRejectionClusterRca.class.getSimpleName();

  public <R extends Rca> QueueRejectionClusterRca(final int rcaPeriod, final R hotNodeRca) {
    super(rcaPeriod, hotNodeRca, Arrays.asList(
        ResourceType.newBuilder().setThreadpool(ThreadPoolEnum.THREADPOOL_REJECTED_REQS).build()
    ));
  }
}
