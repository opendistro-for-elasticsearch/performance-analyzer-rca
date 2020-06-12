package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.grpc;

import com.google.common.collect.Lists;
import org.junit.Before;
import org.junit.Test;

import java.util.List;

/**
 * GRPCClientTest is a class which can be used to test manual calls to our RCA GRPC servers
 */
public class GRPCClientTest {
    private GRPCClient client;

    @Before
    public void setup() {
        // TODO replace these with your endpoint
        client = new GRPCClient("localhost", 9650);
    }

    /**
     * testClient is an method demonstrating the use of our GRPC client
     */
    @Test
    public void testClient() throws Exception {
        // Example getMetrics class
        MetricsRequest request = MetricsRequest.newBuilder().addMetricList("CPU_Utilization").addAggList("avg")
                .addDimList("ShardID").build();
        MetricsResponse response = client.getMetrics(request);
        // Example subscribe call
        SubscribeMessage subscribeRequest = SubscribeMessage.newBuilder().setDestinationNode("destNode")
                .setRequesterNode("reqNode").build();
        SubscribeResponse subscribeResponse = client.subscribe(subscribeRequest);
        // Example publish call
        HotNodeSummaryMessage hnsMessage = HotNodeSummaryMessage.newBuilder().setHostAddress("localhost").setNodeID("ABC").build();
        HotClusterSummaryMessage hcsMessage = HotClusterSummaryMessage.newBuilder().setNodeCount(1).build();
        List<FlowUnitMessage> messages = Lists.newArrayList(
                FlowUnitMessage.newBuilder().setEsNode("EsNode").setGraphNode("GraphNode").setHotNodeSummary(hnsMessage).build(),
                FlowUnitMessage.newBuilder().setEsNode("EsNode2").setGraphNode("GraphNode").setHotClusterSummary(hcsMessage).build()
        );
        PublishResponse publishResponse = client.publish(messages);
    }
}
