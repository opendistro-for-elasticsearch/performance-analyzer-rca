package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelUtil.ControlType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlRequest;
import org.junit.Assert;
import org.junit.Test;

public class ModifyQueueCapacityRequestBuilderTest {
  InstanceDetails.Id id = new Id("node1");
  InstanceDetails.Ip ip = new Ip("127.0.0.1");
  NodeKey nodeKey = new NodeKey(id, ip);


  @Test
  public void testBuilder() {
    AppContext appContext = new AppContext();  
    RcaConf rcaConf = new RcaConf();
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(nodeKey, ResourceEnum.SEARCH_THREADPOOL, appContext, rcaConf);
    ModifyQueueCapacityAction action = builder.increase(true).build();
    BatchStartControlRequest request = ModifyQueueCapacityRequestBuilder.newBuilder(action).build();
    Assert.assertEquals(1, request.getActionsCount());
    ReactionWheel.Action requestAction = request.getActions(0);

    //control
    Assert.assertTrue(requestAction.hasControl());
    ReactionWheel.Control control = requestAction.getControl();
    ReactionWheel.Control expectedControl =
        ReactionWheelUtil.buildControl(
            ControlType.SEARCH_QUEUE_TUNING, ReactionWheelTestUtil.generateTestPayload(action));
    Assert.assertEquals(expectedControl, control);

    //target
    Assert.assertTrue(requestAction.hasTarget());
    ReactionWheel.Target target = requestAction.getTarget();
    ReactionWheel.Target expectedTarget = ReactionWheelUtil.buildTarget(nodeKey);
    Assert.assertEquals(expectedTarget, target);
  }
}

