package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.hotshard;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.SHARD_ID;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HighCPUShardRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessorTestHelper;

import java.time.Clock;
import java.time.Duration;
import java.time.ZoneId;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class HighCPUShardRcaTest {

    private HighCPUShardRcaX highCPUShardRcaX;
    private MetricTestHelper cpuUtilization;
    private MetricTestHelper ioTotThroughput;
    private MetricTestHelper ioTotSyscallRate;
    private List<String> columnName;

    private ClusterDetailsEventProcessorTestHelper clusterDetailsEventProcessorTestHelper;

    private enum index {
        index_1,
        index_2
    }

    private enum shard {
        shard_1,
        shard_2,
        shard_3
    }

    @Before
    public void setup() {
        cpuUtilization = new MetricTestHelper(5);
        ioTotThroughput = new MetricTestHelper(5);
        ioTotSyscallRate = new MetricTestHelper(5);
        highCPUShardRcaX = new HighCPUShardRcaX(5, 1,
                cpuUtilization, ioTotThroughput, ioTotSyscallRate);
        columnName = Arrays.asList(INDEX_NAME.toString(), SHARD_ID.toString(), MetricsDB.SUM);

        try {
            clusterDetailsEventProcessorTestHelper = new ClusterDetailsEventProcessorTestHelper();
            clusterDetailsEventProcessorTestHelper.addNodeDetails("node1", "127.0.0.0", false);
            clusterDetailsEventProcessorTestHelper.generateClusterDetailsEvent();
        } catch (Exception e) {
            Assert.assertTrue("Exception when generating cluster details event", false);
            return;
        }
    }


    // 1. No Flow Units received
    @Test
    public void testOperateForMissingFlowUnits() {
        cpuUtilization = null;
        ioTotThroughput = null;
        ioTotSyscallRate = null;

        ResourceFlowUnit flowUnit = highCPUShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    }

    // 2. Empty Flow Units received
    @Test
    public void testOperateForEmptyFlowUnits() {
        cpuUtilization.createTestFlowUnits(columnName, Collections.emptyList());
        ioTotThroughput.createTestFlowUnits(columnName, Collections.emptyList());
        ioTotSyscallRate.createTestFlowUnits(columnName, Collections.emptyList());

        ResourceFlowUnit flowUnit = highCPUShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    }

    // 1. No Flow Units received/generated on master
    @Test
    public void testOperate() {
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

        // ts = 0
        // index = index_1, shard = shard_1, cpuUtilization = 0, ioTotThroughput = 0, ioTotSyscallRate = 0
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0)));
        highCPUShardRcaX.setClock(constantClock);
        ResourceFlowUnit flowUnit = highCPUShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        // ts = 1
        // index = index_1, shard = shard_1, cpuUtilization = 0.005, ioTotThroughput = 200000, ioTotSyscallRate = 0.005
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0.005)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(200000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0.005)));

        highCPUShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(1)));
        flowUnit = highCPUShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        //ts = 2
        // index = index_1, shard = shard_1, cpuUtilization = 0.75, ioTotThroughput = 200000, ioTotSyscallRate = 0.005
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0.75)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(200000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_1.toString(), String.valueOf(0.005)));

        highCPUShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(2)));
        flowUnit = highCPUShardRcaX.operate();
        HotNodeSummary summary1 = (HotNodeSummary) flowUnit.getResourceSummary();
        List<HotShardSummary> hotShardSummaryList1 = summary1.getHotShardSummaryList();

        /*Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
        Assert.assertEquals(1, hotShardSummaryList1.size());
        Assert.assertEquals(shard.shard_1.toString(), hotShardSummaryList1.get(0).getShardId());
        Assert.assertEquals(index.index_1.toString(), hotShardSummaryList1.get(0).getIndexName());
        Assert.assertEquals("node1", hotShardSummaryList1.get(0).getNodeId());

        // ts = 3
        // index = index_1, shard = shard_2, cpuUtilization = 0.75, ioTotThroughput = 400000, ioTotSyscallRate = 0.10
        //
        // and
        // ts = 4
        // index = index_1, shard = shard_2, cpuUtilization = 0.25, ioTotThroughput = 100000, ioTotSyscallRate = 0.10
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_2.toString(), String.valueOf(0.75)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_2.toString(), String.valueOf(400000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_2.toString(), String.valueOf(0.10)));;

        highCPUShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(3)));
        flowUnit = highCPUShardRcaX.operate();

        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_2.toString(), String.valueOf(0.25)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_2.toString(), String.valueOf(100000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), shard.shard_2.toString(), String.valueOf(0.10)));;

        highCPUShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(4)));
        flowUnit = highCPUShardRcaX.operate();
        HotNodeSummary summary2 = (HotNodeSummary) flowUnit.getResourceSummary();
        List<HotShardSummary> hotShardSummaryList2 = summary2.getHotShardSummaryList();

        Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
        Assert.assertEquals(2, hotShardSummaryList2.size());
        Assert.assertEquals(index.index_1.toString(), hotShardSummaryList2.get(0).getIndexName());
        Assert.assertEquals(shard.shard_2.toString(), hotShardSummaryList2.get(0).getShardId());
        Assert.assertEquals("node1", hotShardSummaryList2.get(0).getNodeId());
        Assert.assertEquals(shard.shard_1.toString(), hotShardSummaryList2.get(1).getShardId());
        Assert.assertEquals(index.index_1.toString(), hotShardSummaryList2.get(1).getIndexName());
        Assert.assertEquals("node1", hotShardSummaryList2.get(1).getNodeId());*/

    }

    private static class HighCPUShardRcaX extends HighCPUShardRca {
        public <M extends Metric> HighCPUShardRcaX(final long evaluationIntervalSeconds,
            final int rcaPeriod, final M cpuUtilization, final M ioTotThroughput, final M ioTotSyscallRate) {
          super(evaluationIntervalSeconds, rcaPeriod, cpuUtilization, ioTotThroughput,ioTotSyscallRate);
        }

        public void setClock(Clock clock) {
            this.clock = clock;
        }
    }
}
