package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.poc;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.OPERATION_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.SHARDID_VALUE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.Constants.SHARD_ROLE_VALUE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.RcaItMarker;
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
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@RunWith(RcaItNotEncryptedRunner.class)

@Category(RcaItMarker.class)
@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)
@ARcaGraph(RcaItPocSingleNode.SimpleAnalysisGraphForCoLocated.class)
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
        ),
        @ATable(hostTag = {HostTag.ELECTED_MASTER},
            tuple = {
                @ATuple(dimensionValues = {"0", "logs", "bulk", "r"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 50.0),
                @ATuple(dimensionValues = {"1", "logs", "bulk", "p"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 5.0),
                @ATuple(dimensionValues = {"2", "logs", "bulk", "r"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 11.0)
            }
        )
    }
)
public class RcaItPocCoLocatedMaster {
  private TestApi api;

  @Test
  @AExpect(
      what = AExpect.Type.REST_API,
      on = HostTag.ELECTED_MASTER,
      validator = PocValidator.class,
      forRca = RcaItPocSingleNode.SimpleAnalysisGraphForCoLocated.ClusterRca.class)
  public void simple() {
  }

  public void setTestApi(final TestApi api) {
    this.api = api;
  }
}
