package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.remediation;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolDimension.THREAD_POOL_TYPE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.NodeConfigFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ThreadPool_QueueCapacity;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.flow_units.MetricFlowUnitTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.remediation.NodeConfigCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.util.Arrays;
import java.util.Collections;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Ignore
@Category(GradleTaskForRca.class)
public class NodeConfigCollectorTest {

  private ThreadPool_QueueCapacity threadPool_QueueCapacity;
  private NodeConfigCollector nodeConfigCollector;

  @Before
  public void init() {
  }

  /**
   * generate flowunit and bind the flowunits it generate to metrics
   */
  @SuppressWarnings("unchecked")
  private void mockFlowUnits(int writeQueueCapacity, int searchQueueCapacity) {
    MetricFlowUnit flowUnit = MetricFlowUnitTestHelper.createFlowUnit(
        Arrays.asList(THREAD_POOL_TYPE.toString(), MetricsDB.MAX),
        Arrays.asList(ThreadPoolType.WRITE.toString(), String.valueOf(writeQueueCapacity)),
        Arrays.asList(ThreadPoolType.SEARCH.toString(), String.valueOf(searchQueueCapacity))
    );
    threadPool_QueueCapacity.setLocalFlowUnit(flowUnit);
  }

  @Test
  public void testCapacityMetricNotExist() {
    threadPool_QueueCapacity.setLocalFlowUnit(MetricFlowUnit.generic());
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertTrue(flowUnit.isEmpty());
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY));
    Assert.assertFalse(flowUnit.hasConfig(ResourceUtil.WRITE_QUEUE_CAPACITY));
  }

  @Test
  public void testCapacityCollection() {
    mockFlowUnits(100, 200);
    NodeConfigFlowUnit flowUnit = nodeConfigCollector.operate();
    Assert.assertFalse(flowUnit.isEmpty());
    Assert.assertTrue(flowUnit.hasConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY));
    Assert.assertEquals(200, flowUnit.readConfig(ResourceUtil.SEARCH_QUEUE_CAPACITY), 0.01);
    Assert.assertTrue(flowUnit.hasConfig(ResourceUtil.WRITE_QUEUE_CAPACITY));
    Assert.assertEquals(100, flowUnit.readConfig(ResourceUtil.WRITE_QUEUE_CAPACITY), 0.01);
  }
}
