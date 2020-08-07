package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelDummyService.SERVER_PORT;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelUtil.ControlType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Id;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails.Ip;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel;
import com.amazon.searchservices.reactionwheel.controller.ReactionWheel.BatchStartControlResult;
import io.grpc.Server;
import io.grpc.ServerBuilder;
import java.io.IOException;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public class ReactionWheelHandlerTest {
  private Server server;
  private ReactionWheelDummyService reactionWheelDummyService;
  private ReactionWheelHandler reactionWheelHandler;
  InstanceDetails.Id id = new Id("node1");
  InstanceDetails.Ip ip = new Ip("127.0.0.1");
  NodeKey nodeKey = new NodeKey(id, ip);

  @Before
  public void initService() throws IOException {
    reactionWheelDummyService = new ReactionWheelDummyService();
    server = ServerBuilder.forPort(SERVER_PORT).addService(reactionWheelDummyService).build().start();
    reactionWheelHandler = new ReactionWheelHandler();
  }

  @After
  public void closeService() {
    reactionWheelHandler.shutdown();
    server.shutdown();
  }

  @Test
  public void testPublishQueueAction() {
    ModifyQueueCapacityAction action =
        new ModifyQueueCapacityAction(nodeKey, ResourceEnum.WRITE_THREADPOOL, 100, true);
    reactionWheelHandler.actionPublished(action);
    BatchStartControlResult result = reactionWheelDummyService.getAndClearResult();
    Assert.assertNotNull(result);
    Assert.assertEquals(0, result.getErrorsCount());
    Assert.assertEquals(1, result.getResultsCount());
    ReactionWheel.ControlResult controlResult = result.getResults(0);

    ReactionWheel.Control control = controlResult.getControl();
    ReactionWheel.Control expectedControl =
        ReactionWheelUtil.buildControl(
            ControlType.WRITE_QUEUE_TUNING, ReactionWheelTestUtil.generateTestPayload(action));
    Assert.assertEquals(expectedControl, control);

    ReactionWheel.Target target = controlResult.getTarget();
    ReactionWheel.Target expectedTarget = ReactionWheelUtil.buildTarget(nodeKey);
    Assert.assertEquals(expectedTarget, target);
  }

  @Test
  public void testPublishInvalidAction() {
    InvalidAction invalidAction =
        new InvalidAction(nodeKey, ResourceEnum.WRITE_THREADPOOL, 100, true);
    reactionWheelHandler.actionPublished(invalidAction);
    BatchStartControlResult result = reactionWheelDummyService.getAndClearResult();
    Assert.assertNull(result);
  }

  private static class InvalidAction extends ModifyQueueCapacityAction {

    public InvalidAction(NodeKey esNode, ResourceEnum threadPool, int currentCapacity, boolean increase) {
      super(esNode, threadPool, currentCapacity, increase);
    }

    @Override
    public String name() {
      return "InvalidAction";
    }
  }
}

