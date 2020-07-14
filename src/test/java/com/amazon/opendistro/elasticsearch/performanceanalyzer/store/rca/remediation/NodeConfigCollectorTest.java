package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.remediation;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PerformanceControllerConfiguration;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.remediation.NodeConfigCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;
import java.util.Arrays;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class NodeConfigCollectorTest {

  private MetricTestHelper threadPool_QueueCapacity;
  private NodeConfigCollector nodeConfigCollector;

  @Before
  public void init() throws Exception {
    threadPool_QueueCapacity = new MetricTestHelper(5);
    nodeConfigCollector = new NodeConfigCollector(1, threadPool_QueueCapacity);
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
    ResourceFlowUnit<HotNodeSummary> flowUnit = nodeConfigCollector.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    PerformanceControllerConfiguration performanceControllerConfiguration = flowUnit.getSummary().getPerformanceControllerConfiguration();
    Assert.assertEquals(-1, performanceControllerConfiguration.getSearchQueueCapacity());
    Assert.assertEquals(-1, performanceControllerConfiguration.getWriteQueueCapacity());
  }

  @Test
  public void testCapacityCollection() {
    mockFlowUnits(100, 200);
    ResourceFlowUnit<HotNodeSummary> flowUnit = nodeConfigCollector.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    PerformanceControllerConfiguration performanceControllerConfiguration = flowUnit.getSummary().getPerformanceControllerConfiguration();
    Assert.assertEquals(200, performanceControllerConfiguration.getSearchQueueCapacity());
    Assert.assertEquals(100, performanceControllerConfiguration.getWriteQueueCapacity());
  }
}
