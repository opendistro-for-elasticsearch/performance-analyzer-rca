package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

public class SizeUpJvmAction extends SuppressibleAction {

  public static final String NAME = "SizeUpJvmAction";
  private static final String SUMMARY = "Update heap size to 128GB";
  private final boolean canUpdate;
  private final NodeKey esNode;
  private static final long DEFAULT_COOL_OFF_PERIOD_IN_MILLIS = TimeUnit.DAYS.toMillis(3);
  private static final long GB_TO_B = 1024 * 1024 * 1024;

  public SizeUpJvmAction(final AppContext appContext) {
    super(appContext);
    this.esNode = new NodeKey(appContext.getMyInstanceDetails());
    this.canUpdate = Runtime.getRuntime().totalMemory() > 200 * GB_TO_B;
  }

  @Override
  public boolean canUpdate() {
    return canUpdate;
  }

  @Override
  public long coolOffPeriodInMillis() {
    return DEFAULT_COOL_OFF_PERIOD_IN_MILLIS;
  }

  @Override
  public List<NodeKey> impactedNodes() {
    return Collections.singletonList(esNode);
  }

  @Override
  public Map<NodeKey, ImpactVector> impact() {
    final ImpactVector impactVector = new ImpactVector();
    impactVector.increasesPressure(Dimension.RAM, Dimension.DISK, Dimension.CPU);
    impactVector.decreasesPressure(Dimension.HEAP);

    return Collections.singletonMap(esNode, impactVector);
  }

  @Override
  public String name() {
    return NAME;
  }

  @Override
  public String summary() {
    if (!isActionable()) {
      return String.format("No action to take for: [%s]", NAME);
    }

    return SUMMARY;
  }
}
