package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca.hotshard;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.CommonDimension.SHARD_ID;
import static java.time.Instant.ofEpochMilli;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotShardSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotshard.HotShardRca;
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
public class HotShardRcaTest {

    private HotShardRcaX hotShardRcaX;
    private MetricTestHelper cpuUtilization;
    private MetricTestHelper ioTotThroughput;
    private MetricTestHelper ioTotSyscallRate;
    private List<String> columnName;

    private enum index {
        index_1,
        index_2
    }

    @Before
    public void setup() {
        cpuUtilization = new MetricTestHelper(5);
        ioTotThroughput = new MetricTestHelper(5);
        ioTotSyscallRate = new MetricTestHelper(5);
        hotShardRcaX = new HotShardRcaX(5, 1,
                cpuUtilization, ioTotThroughput, ioTotSyscallRate);
        columnName = Arrays.asList(INDEX_NAME.toString(), SHARD_ID.toString(), MetricsDB.SUM);

        InstanceDetails instanceDetails =
            new InstanceDetails(AllMetrics.NodeRole.DATA, "node1", "127.0.0.1", false);
        hotShardRcaX.setInstanceDetails(instanceDetails);
    }


    // 1. No Flow Units received
    @Test
    public void testOperateForMissingFlowUnits() {
        cpuUtilization = null;
        ioTotThroughput = null;
        ioTotSyscallRate = null;

        ResourceFlowUnit flowUnit = hotShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    }

    // 2. Empty Flow Units received
    @Test
    public void testOperateForEmptyFlowUnits() {
        cpuUtilization.createTestFlowUnits(columnName, Collections.emptyList());
        ioTotThroughput.createTestFlowUnits(columnName, Collections.emptyList());
        ioTotSyscallRate.createTestFlowUnits(columnName, Collections.emptyList());

        ResourceFlowUnit flowUnit = hotShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());
    }

    // 1. No Flow Units received/generated on master
    @Test
    public void testOperate() {
        Clock constantClock = Clock.fixed(ofEpochMilli(0), ZoneId.systemDefault());

        // ts = 0
        // index = index_1, shard = shard_1, cpuUtilization = 0, ioTotThroughput = 0, ioTotSyscallRate = 0
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0)));
        hotShardRcaX.setClock(constantClock);
        ResourceFlowUnit flowUnit = hotShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        // ts = 1
        // index = index_1, shard = shard_1, cpuUtilization = 0.005, ioTotThroughput = 200000, ioTotSyscallRate = 0.005
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0.005)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(200000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0.005)));

        hotShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(1)));
        flowUnit = hotShardRcaX.operate();
        Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

        //ts = 2
        // index = index_1, shard = shard_1, cpuUtilization = 0.75, ioTotThroughput = 200000, ioTotSyscallRate = 0.005
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0.75)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(200000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "1", String.valueOf(0.005)));

        hotShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(2)));
        flowUnit = hotShardRcaX.operate();
        HotNodeSummary summary1 = (HotNodeSummary) flowUnit.getSummary();
        List<GenericSummary> hotShardSummaryList1 = summary1.getNestedSummaryList();

        Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
        Assert.assertEquals(1, hotShardSummaryList1.size());

        HotShardSummary hotShardSummary1 = (HotShardSummary) hotShardSummaryList1.get(0);
        Assert.assertEquals("1", hotShardSummary1.getShardId());
        Assert.assertEquals(index.index_1.toString(), hotShardSummary1.getIndexName());
        Assert.assertEquals("node1", hotShardSummary1.getNodeId());

        // ts = 3
        // index = index_1, shard = shard_2, cpuUtilization = 0.75, ioTotThroughput = 400000, ioTotSyscallRate = 0.10
        //
        // and
        // ts = 4
        // index = index_1, shard = shard_2, cpuUtilization = 0.25, ioTotThroughput = 100000, ioTotSyscallRate = 0.10
        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "2", String.valueOf(0.75)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "2", String.valueOf(400000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "2", String.valueOf(0.10)));;

        hotShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(3)));
        flowUnit = hotShardRcaX.operate();

        cpuUtilization.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "2", String.valueOf(0.25)));
        ioTotThroughput.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "2", String.valueOf(100000)));
        ioTotSyscallRate.createTestFlowUnits(columnName,
                Arrays.asList(index.index_1.toString(), "2", String.valueOf(0.10)));;

        hotShardRcaX.setClock(Clock.offset(constantClock, Duration.ofSeconds(4)));
        flowUnit = hotShardRcaX.operate();
        HotNodeSummary summary2 = (HotNodeSummary) flowUnit.getSummary();
        List<GenericSummary> hotShardSummaryList2 = summary2.getNestedSummaryList();

        Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
        Assert.assertEquals(2, hotShardSummaryList2.size());

        HotShardSummary hotShardSummary2 = (HotShardSummary) hotShardSummaryList2.get(0);
        HotShardSummary hotShardSummary3 = (HotShardSummary) hotShardSummaryList2.get(1);
        Assert.assertEquals(index.index_1.toString(), hotShardSummary2.getIndexName());
        Assert.assertEquals("1", hotShardSummary2.getShardId());
        Assert.assertEquals("node1", hotShardSummary2.getNodeId());
        Assert.assertEquals("2", hotShardSummary3.getShardId());
        Assert.assertEquals(index.index_1.toString(), hotShardSummary3.getIndexName());
        Assert.assertEquals("node1", hotShardSummary3.getNodeId());
    }

    private static class HotShardRcaX extends HotShardRca {
        public <M extends Metric> HotShardRcaX(final long evaluationIntervalSeconds, final int rcaPeriod,
                                               final M cpuUtilization, final M ioTotThroughput, final M ioTotSyscallRate) {
          super(evaluationIntervalSeconds, rcaPeriod, cpuUtilization, ioTotThroughput,ioTotSyscallRate);
        }

        public void setClock(Clock clock) {
            this.clock = clock;
        }
    }
}
