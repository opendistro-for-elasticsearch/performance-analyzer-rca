package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelDummyService.SERVER_PORT;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.ResourceUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.reaction_wheel.ReactionWheelUtil.ControlType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
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
import java.util.List;
import java.util.Map;

public class ReactionWheelHandlerTest {
  private Server server;
  private ReactionWheelDummyService reactionWheelDummyService;
  private ReactionWheelHandler reactionWheelHandler;
  private AppContext appContext;
  InstanceDetails.Id id = new Id("node1");
  InstanceDetails.Ip ip = new Ip("127.0.0.1");
  NodeKey nodeKey = new NodeKey(id, ip);

  @Before
  public void initService() throws IOException {
    reactionWheelDummyService = new ReactionWheelDummyService();
    server = ServerBuilder.forPort(SERVER_PORT).addService(reactionWheelDummyService).build().start();
    reactionWheelHandler = new ReactionWheelHandler();
    appContext = new AppContext();
  }

  @After
  public void closeService() {
    reactionWheelHandler.shutdown();
    server.shutdown();
  }

  @Test
  public void testPublishQueueAction() throws Exception {
    RcaConf rcaConf = new RcaConf();
    rcaConf.readConfigFromString("{}");
    appContext.getNodeConfigCache()
        .put(nodeKey, ResourceUtil.WRITE_QUEUE_CAPACITY, 300);
    ModifyQueueCapacityAction.Builder builder =
        ModifyQueueCapacityAction.newBuilder(nodeKey, ResourceEnum.WRITE_THREADPOOL, appContext, rcaConf);
    ModifyQueueCapacityAction action = builder.increase(true).build();
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
    InvalidAction invalidAction = new InvalidAction();

    reactionWheelHandler.actionPublished(invalidAction);
    BatchStartControlResult result = reactionWheelDummyService.getAndClearResult();
    Assert.assertNull(result);
  }

  private static class InvalidAction implements Action {

    @Override
    public String name() {
      return "InvalidAction";
    }

    @Override
    public boolean isActionable() {
      return true;
    }

    @Override
    public long coolOffPeriodInMillis() {
      return 0;
    }

    @Override
    public List<NodeKey> impactedNodes() {
      return null;
    }

    @Override
    public Map<NodeKey, ImpactVector> impact() {
      return null;
    }

    @Override
    public String summary() {
      return null;
    }

    @Override
    public boolean isMuted() {
      return false;
    }
  }
}

