package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.remediation;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.NodeConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.remediation.NodeConfigurationRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeConfigurationRcaTest {

  private MetricTestHelper threadPool_QueueCapacity;
  private NodeConfigurationRca nodeConfigurationRca;

  @Before
  public void init() throws Exception {
    threadPool_QueueCapacity = new MetricTestHelper(5);
    nodeConfigurationRca = new NodeConfigurationRca(1, threadPool_QueueCapacity);
    ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
    clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
    clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
  }

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  private void mockFlowUnits(int writeQueueCapacity, int searchQueueCapacity) {
    threadPool_QueueCapacity.createTestFlowUnitsWithMultipleRows(
        Arrays.asList(THREAD_POOL_TYPE.toString(), MetricsDB.MAX),
        Arrays.asList(
            Arrays.asList(ThreadPoolType.WRITE.toString(), String.valueOf(writeQueueCapacity)),
            Arrays.asList(ThreadPoolType.SEARCH.toString(), String.valueOf(searchQueueCapacity))
        )
    );
  }

  @Test
  public void testCapacityMetricNotExist() {
    threadPool_QueueCapacity.createEmptyFlowunit();
    ResourceFlowUnit<HotNodeSummary> flowUnit = nodeConfigurationRca.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    NodeConfiguration nodeConfiguration = flowUnit.getSummary().getNodeConfiguration();
    Assert.assertEquals(-1, nodeConfiguration.getSearchQueueCapacity());
    Assert.assertEquals(-1, nodeConfiguration.getWriteQueueCapacity());
  }

  @Test
  public void testCapacityCollection() {
    mockFlowUnits(100, 200);
    ResourceFlowUnit<HotNodeSummary> flowUnit = nodeConfigurationRca.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    NodeConfiguration nodeConfiguration = flowUnit.getSummary().getNodeConfiguration();
    Assert.assertEquals(200, nodeConfiguration.getSearchQueueCapacity());
    Assert.assertEquals(100, nodeConfiguration.getWriteQueueCapacity());
  }
}
