package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyCacheMaxSizeAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelUtil.ControlType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs.CacheDeciderConfig;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ModifyCacheMaxSizeRequestBuilderTest {
  final InstanceDetails.Id id = new Id("node1");
  final InstanceDetails.Ip ip = new Ip("127.0.0.1");
  final NodeKey nodeKey = new NodeKey(id, ip);

  private AppContext appContext;

  @Before
  public void setUp() {
    final long heapMaxSizeInBytes = 12000 * 1_000_000L;
    final long fieldDataCacheMaxSizeInBytes = 12000;
    final long shardRequestCacheMaxSizeInBytes = 12000;

    appContext = new AppContext();
    appContext
        .getNodeConfigCache()
        .put(nodeKey, ResourceUtil.HEAP_MAX_SIZE, heapMaxSizeInBytes);
    appContext
        .getNodeConfigCache()
        .put(nodeKey, ResourceUtil.FIELD_DATA_CACHE_MAX_SIZE, fieldDataCacheMaxSizeInBytes);
    appContext
        .getNodeConfigCache()
        .put(nodeKey, ResourceUtil.SHARD_REQUEST_CACHE_MAX_SIZE, shardRequestCacheMaxSizeInBytes);
  }

  @Test
  public void testBuilder() {
    ModifyCacheMaxSizeAction action =
        new ModifyCacheMaxSizeAction(
            nodeKey,
            ResourceEnum.FIELD_DATA_CACHE,
            appContext.getNodeConfigCache(),
            CacheDeciderConfig.DEFAULT_FIELD_DATA_CACHE_UPPER_BOUND,
            true);
    BatchStartControlRequest request = ModifyCacheMaxSizeRequestBuilder.newBuilder(action).build();
    Assert.assertEquals(1, request.getActionsCount());
    ReactionWheel.Action requestAction = request.getActions(0);

    // control
    Assert.assertTrue(requestAction.hasControl());
    ReactionWheel.Control control = requestAction.getControl();
    ReactionWheel.Control expectedControl =
        ReactionWheelUtil.buildControl(
            ControlType.FIELDDATA_CACHE_TUNING, ReactionWheelTestUtil.generateTestPayload(action));
    Assert.assertEquals(expectedControl, control);

    // target
    Assert.assertTrue(requestAction.hasTarget());
    ReactionWheel.Target target = requestAction.getTarget();
    ReactionWheel.Target expectedTarget = ReactionWheelUtil.buildTarget(nodeKey);
    Assert.assertEquals(expectedTarget, target);
  }
}

