package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.poc;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.OPERATION_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.SHARDID_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.SHARD_ROLE_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.LOCUS_DATA_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_AGGREGATE_UPSTREAM;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants.TAG_LOCUS;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATuple;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.api.TestApi;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners.RcaItNotEncryptedRunner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.poc.validator.PocValidator;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Test;
import org.junit.runner.RunWith;

@RunWith(RcaItNotEncryptedRunner.class)

@AClusterType(ClusterType.SINGLE_NODE)
@ARcaGraph(RcaItPocSingleNode.SimpleAnalysisGraphForSingle.class)
@AMetric(name = CPU_Utilization.class,
    dimensionNames = {SHARDID_VALUE, INDEX_NAME_VALUE, OPERATION_VALUE, SHARD_ROLE_VALUE},
    tables = {
        @ATable(hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(dimensionValues = {"0", "logs", "bulk", "p"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
                @ATuple(dimensionValues = {"1", "logs", "bulk", "r"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 80.0),
                @ATuple(dimensionValues = {"2", "logs", "bulk", "p"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 10.0)
            }
        )
    }
)
public class RcaItPocSingleNode {
  private TestApi api;

  @Test
  @AExpect(
      what = AExpect.Type.REST_API,
      on = HostTag.DATA_0,
      validator = PocValidator.class,
      forRca = SimpleAnalysisGraph.ClusterRca.class)
  public void simple() {
  }

  public void setTestApi(final TestApi api) {
    this.api = api;
  }


  public static class SimpleAnalysisGraphForSingle extends SimpleAnalysisGraph {

    @Override
    public void construct() {
      CPU_Utilization cpuUtilization = new CPU_Utilization(1);
      cpuUtilization.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
      addLeaf(cpuUtilization);

      SimpleAnalysisGraph.NodeRca nodeRca = new SimpleAnalysisGraph.NodeRca(cpuUtilization);
      nodeRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
      nodeRca.addAllUpstreams(Arrays.asList(cpuUtilization));

      SimpleAnalysisGraph.ClusterRca clusterRca = new SimpleAnalysisGraph.ClusterRca(nodeRca);
      clusterRca.addTag(TAG_LOCUS, LOCUS_DATA_MASTER_NODE);
      clusterRca.addAllUpstreams(Collections.singletonList(nodeRca));
      clusterRca.addTag(TAG_AGGREGATE_UPSTREAM, LOCUS_DATA_NODE);
    }
  }
}
