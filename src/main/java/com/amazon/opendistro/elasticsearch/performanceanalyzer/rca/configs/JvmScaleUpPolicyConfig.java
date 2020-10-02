package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.configs;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;

public class JvmScaleUpPolicyConfig {

  private static final String POLICY_NAME = "JvmScaleUpPolicy";
  public static final int DEFAULT_UNHEALTHY_NODE_PERCENTAGE = 50;
  public static final int DEFAULT_MIN_UNHEALTHY_MINUTES = 2 * 24 * 60;
  private final int unhealthyNodePercentage;

  private final int minUnhealthyMinutes;

  public JvmScaleUpPolicyConfig(final RcaConf rcaConf) {
    this.unhealthyNodePercentage = rcaConf.readRcaConfig(POLICY_NAME,
        ScaleUpPolicyKeys.UNHEALTHY_NODE_PERCENTAGE_KEY.toString(), DEFAULT_UNHEALTHY_NODE_PERCENTAGE
        , Integer.class);
    this.minUnhealthyMinutes = rcaConf.readRcaConfig(POLICY_NAME,
        ScaleUpPolicyKeys.MIN_UNHEALTHY_MINUTES_KEY.toString(), DEFAULT_MIN_UNHEALTHY_MINUTES,
        Integer.class);
  }

  enum ScaleUpPolicyKeys {
    UNHEALTHY_NODE_PERCENTAGE_KEY("unhealthy-node-percentage"),
    MIN_UNHEALTHY_MINUTES_KEY("min-unhealthy-minutes");

    private final String value;

    ScaleUpPolicyKeys(final String value) {
      this.value = value;
    }
    @Override
    public String toString() {
      return value;
    }
  }

  public int getUnhealthyNodePercentage() {
    return unhealthyNodePercentage;
  }

  public int getMinUnhealthyMinutes() {
    return minUnhealthyMinutes;
  }
}
