package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_CLUSTER_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_NODE_SUMMARY_TABLE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.HOT_RESOURCE_SUMMARY_TABLE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.NodeSummaryResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.RcaResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.response.ResourceSummaryResponse;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import org.jooq.DSLContext;
import org.jooq.Record;
import org.jooq.impl.DSL;
import org.jooq.tools.jdbc.Mock;
import org.jooq.tools.jdbc.MockConnection;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaResponseUtilTest {
  private static final String RESOURCE_TYPE = "Resource";
  private static final String UNIT_TYPE = "Unit_Type";
  private static final double THRESHOLD = 1.4;
  private static final double AVERAGE = 1.6;
  private static final double ACTUAL = 1.4;
  private static final double MAXIMUM = 1.9;
  private static final double MINIMUM = 1.2;
  private static final String NODE_ID = "NodeId";
  private static final String IP_ADDRESS = "IpAddress";
  private static final String TIMESTAMP = "6382174682928";
  private static final String STATE = "State";
  private static final int NUM_OF_NODES = 4;
  private static final int NUM_OF_UNHEALTHY_NODES = 2;
  private static final String RCA = "RCA";
  private Record record;
  private DSLContext context;

  @Before
  public void init() {
    context = DSL.using(new MockConnection(Mock.of(0)));
    record = getRecord(RESOURCE_TYPE, RCA, UNIT_TYPE, STATE, THRESHOLD, AVERAGE, ACTUAL, MAXIMUM, MINIMUM, NODE_ID,
            IP_ADDRESS, TIMESTAMP, NUM_OF_NODES, NUM_OF_UNHEALTHY_NODES);
  }

  @Test
  public void testRcaResponseForEmptyRecordList() {
    Assert.assertEquals(null, RcaResponseUtil.getRcaResponse(RCA, ImmutableList.of(), ImmutableSet.of(RCA)));
  }

  @Test
  public void testRcaResponseWithoutSummaryTables() {
    RcaResponse expected = new RcaResponse(RCA, STATE, TIMESTAMP);
    Assert.assertEquals(expected, RcaResponseUtil.getRcaResponse(RCA, ImmutableList.of(record), ImmutableSet.of(RCA)));
  }

  @Test
  public void testRcaResponseWithClusterSummaryTable() {
    RcaResponse expected = new RcaResponse(RCA, STATE, NUM_OF_NODES, NUM_OF_UNHEALTHY_NODES, TIMESTAMP);
    Assert.assertEquals(expected, RcaResponseUtil.getRcaResponse(RCA, ImmutableList.of(record),
            ImmutableSet.of(RCA, HOT_CLUSTER_SUMMARY_TABLE)));
  }

  @Test
  public void testRcaResponseWithNodeSummaryTable() {
    NodeSummaryResponse nodeSummaryResponse = new NodeSummaryResponse(NODE_ID, IP_ADDRESS);
    RcaResponse expected = new RcaResponse(RCA, STATE, NUM_OF_NODES, NUM_OF_UNHEALTHY_NODES, TIMESTAMP);
    expected.addSummary(nodeSummaryResponse);
    Assert.assertEquals(expected, RcaResponseUtil.getRcaResponse(RCA, ImmutableList.of(record),
            ImmutableSet.of(RCA, HOT_CLUSTER_SUMMARY_TABLE, HOT_NODE_SUMMARY_TABLE)));
  }

  @Test
  public void testRcaResponseWithResourceSummaryTable() {
    ResourceSummaryResponse resourceSummaryResponse = new ResourceSummaryResponse(RESOURCE_TYPE, UNIT_TYPE,
            THRESHOLD, ACTUAL, AVERAGE, MINIMUM, MAXIMUM);
    NodeSummaryResponse nodeSummaryResponse = new NodeSummaryResponse(NODE_ID, IP_ADDRESS);
    nodeSummaryResponse.addResource(resourceSummaryResponse);
    RcaResponse expected = new RcaResponse(RCA, STATE, NUM_OF_NODES, NUM_OF_UNHEALTHY_NODES, TIMESTAMP);
    expected.addSummary(nodeSummaryResponse);
    Assert.assertEquals(expected, RcaResponseUtil.getRcaResponse(RCA, ImmutableList.of(record),
            ImmutableSet.of(RCA, HOT_CLUSTER_SUMMARY_TABLE, HOT_NODE_SUMMARY_TABLE, HOT_RESOURCE_SUMMARY_TABLE)));
  }

  private Record getRecord(String resource,
                           String rca,
                           String unitType,
                           String state,
                           double threshold,
                           double avg,
                           double actual,
                           double max,
                           double min,
                           String nodeId,
                           String ipAddress,
                           String timestamp,
                           int numOfNodes,
                           int numOfUnhealthyNodes) {
    return context.newRecord(DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.RESOURCE_TYPE_COL_NAME),
            DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.UNIT_TYPE_COL_NAME),
            DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.THRESHOLD_COL_NAME),
            DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.AVG_VALUE_COL_NAME),
            DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.VALUE_COL_NAME),
            DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.MAX_VALUE_COL_NAME),
            DSL.field(HotResourceSummary.SQL_SCHEMA_CONSTANTS.MIN_VALUE_COL_NAME),
            DSL.field(HotNodeSummary.SQL_SCHEMA_CONSTANTS.HOST_IP_ADDRESS_COL_NAME),
            DSL.field(ResourceFlowUnit.SQL_SCHEMA_CONSTANTS.TIMESTAMP_COL_NAME),
            DSL.field(ResourceContext.SQL_SCHEMA_CONSTANTS.STATE_COL_NAME),
            DSL.field(HotClusterSummary.SQL_SCHEMA_CONSTANTS.NUM_OF_NODES_COL_NAME),
            DSL.field(HotClusterSummary.SQL_SCHEMA_CONSTANTS.NUM_OF_UNHEALTHY_NODES_COL_NAME),
            DSL.field(HotNodeSummary.SQL_SCHEMA_CONSTANTS.NODE_ID_COL_NAME))
            .values(resource, unitType, threshold, avg, actual, max, min, ipAddress, timestamp, state,
                    numOfNodes, numOfUnhealthyNodes, nodeId);

  }
}
