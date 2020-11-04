package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.tests.queue_tuning.validator;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ModifyQueueCapacityAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaControllerHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.api.IValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import org.junit.Assert;

public class QDeciderNoActionOnUnhealthyValidator implements IValidator {
  AppContext appContext;
  RcaConf rcaConf;
  long startTime;

  public QDeciderNoActionOnUnhealthyValidator() {
    appContext = new AppContext();
    startTime = System.currentTimeMillis();
    rcaConf = RcaControllerHelper.pickRcaConfForRole(NodeRole.ELECTED_MASTER);
  }

  /**
   * {"actionName":"ModifyQueueCapacity",
   * "resourceValue":4,
   * "timestamp":"1599257910923",
   * "nodeId":"node1",
   * "nodeIp":127.0.0.1,
   * "actionable":1,
   * "coolOffPeriod": 300000,
   * "muted": 0
   * "summary": "Id":"DATA_0","Ip":"127.0.0.1","resource":4,"desiredCapacity":547,
   *            "currentCapacity":500,"coolOffPeriodInMillis":10000,"canUpdate":true}
   */
  @Override
  public boolean checkDbObj(Object object) {
    if (object == null) {
      return false;
    }
    PersistedAction persistedAction = (PersistedAction) object;
    return checkPersistedAction(persistedAction);
  }

  /**
   * {"actionName":"ModifyQueueCapacity",
   * "resourceValue":4,
   * "timestamp":"1599257910923",
   * "nodeId":"node1",
   * "nodeIp":127.0.0.1,
   * "actionable":1,
   * "coolOffPeriod": 300000,
   * "muted": 0
   * "summary": "Id":"DATA_0","Ip":"127.0.0.1","resource":4,"desiredCapacity":547,
   *            "currentCapacity":500,"coolOffPeriodInMillis":10000,"canUpdate":true}
   */
  private boolean checkPersistedAction(final PersistedAction persistedAction) {
    ModifyQueueCapacityAction modifyQueueCapacityAction =
        ModifyQueueCapacityAction.fromSummary(persistedAction.getSummary(), appContext);
    Assert.assertEquals(ModifyQueueCapacityAction.NAME, persistedAction.getActionName());
    Assert.assertEquals("{DATA_0}", persistedAction.getNodeIds());
    Assert.assertEquals("{127.0.0.1}", persistedAction.getNodeIps());
    Assert.assertEquals(ModifyQueueCapacityAction.Builder.DEFAULT_COOL_OFF_PERIOD_IN_MILLIS, persistedAction.getCoolOffPeriod());
    Assert.assertTrue(persistedAction.isActionable());
    Assert.assertFalse(persistedAction.isMuted());
    Assert.assertEquals(ResourceEnum.WRITE_THREADPOOL, modifyQueueCapacityAction.getThreadPool());
    int writeQueueStepSize = rcaConf.getQueueActionConfig().getStepSize(ResourceEnum.WRITE_THREADPOOL);
    Assert.assertEquals(500 + writeQueueStepSize, modifyQueueCapacityAction.getDesiredCapacity());
    Assert.assertEquals(500, modifyQueueCapacityAction.getCurrentCapacity());
    return true;
  }
}
