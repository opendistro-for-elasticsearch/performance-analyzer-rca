package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConfJsonWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import org.apache.commons.lang3.math.NumberUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Class the manages applying the various overrides for RCAs, deciders, and action nodes as
 * written by the NodeDetails collector and read by the {@link ClusterDetailsEventProcessor}
 */
public class ConfigOverridesApplier {

  private static final Logger LOG = LogManager.getLogger(ConfigOverridesApplier.class);
  private volatile long lastAppliedTimestamp;

  public void applyOverride(final String overridesJson, final String lastUpdatedTimestampString) {
    if (!valid(overridesJson, lastUpdatedTimestampString)) {
      LOG.warn("Received invalid overrides or timestamp. Overrides json: {}, last updated "
          + "timestamp: {}", overridesJson, lastAppliedTimestamp);
      return;
    }

    try {
      long lastUpdatedTimestamp = Long.parseLong(lastUpdatedTimestampString);
      LOG.info("Last updated(writer): {}, Last applied(reader): {}", lastUpdatedTimestamp,
          lastAppliedTimestamp);
      if (lastUpdatedTimestamp > lastAppliedTimestamp) {
        apply(ConfigOverridesHelper.deserialize(overridesJson));
      } else {
        LOG.debug("Not applying override. Last updated timestamp {} is behind last applied "
            + "timestamp {}", lastUpdatedTimestamp, lastAppliedTimestamp);
      }
    } catch (IOException ioe) {
      LOG.error("Unable to deserialize overrides JSON:" + overridesJson, ioe);
    }
  }

  private void apply(final ConfigOverrides overrides) {
    if (PerformanceAnalyzerApp.getRcaController() != null && PerformanceAnalyzerApp.getRcaController().isRcaEnabled()) {
      LOG.error("Applying overrides: {}", overrides.getEnable().getRcas());
      RcaConf rcaConf = PerformanceAnalyzerApp.getRcaController().getRcaConf();
      if (rcaConf != null) {
        Set<String> currentMutedRcaSet = new HashSet<>(rcaConf.getMutedRcaList());
        Set<String> currentMutedDeciderSet = new HashSet<>(rcaConf.getMutedDeciderList());
        Set<String> currentMutedActionSet = new HashSet<>(rcaConf.getMutedActionList());
        // check and remove any nodes that are in the disabled list that were enabled just now.
        if (overrides.getEnable() != null) {
          if (overrides.getEnable().getRcas() != null) {
            currentMutedRcaSet.removeAll(overrides.getEnable().getRcas());
          }
          if (overrides.getEnable().getDeciders() != null) {
            currentMutedDeciderSet.removeAll(overrides.getEnable().getDeciders());
          }
          if (overrides.getEnable().getActions() != null) {
            currentMutedActionSet.removeAll(overrides.getEnable().getActions());
          }
        }

        // union the remaining already disabled nodes with the new set of disabled nodes.
        if (overrides.getDisable() != null) {
          if (overrides.getDisable().getRcas() != null) {
            currentMutedRcaSet.addAll(overrides.getDisable().getRcas());
          }
          if (overrides.getDisable().getDeciders() != null) {
            currentMutedDeciderSet.addAll(overrides.getDisable().getDeciders());
          }
          if (overrides.getDisable().getActions() != null) {
            currentMutedActionSet.addAll(overrides.getDisable().getActions());
          }
        }
        rcaConf.updateRcaConf(currentMutedRcaSet, currentMutedDeciderSet, currentMutedActionSet);
        setLastAppliedTimestamp(System.currentTimeMillis());
      }
    }
  }

  private boolean valid(final String overridesJson, final String timestamp) {
    if (overridesJson == null || timestamp == null) {
      return false;
    }

    if (overridesJson.isEmpty() || timestamp.isEmpty()) {
      return false;
    }

    return NumberUtils.isCreatable(timestamp);
  }

  public long getLastAppliedTimestamp() {
    return lastAppliedTimestamp;
  }

  public void setLastAppliedTimestamp(final long lastAppliedTimestamp) {
    this.lastAppliedTimestamp = lastAppliedTimestamp;
  }
}
