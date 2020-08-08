/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import com.google.common.annotations.VisibleForTesting;
import java.time.Clock;
import java.util.HashMap;
import java.util.Map;
import org.apache.commons.lang3.builder.HashCodeBuilder;

/**
 * Detect whether an action has passed its cool off period or not. Due to the heterogeneous nature of
 * Decision Maker framework, this CoolOffDetector needs to keep track of the latest execution timestamp
 * of the most recent actions on each node. An action is identified as "Cooled Off" only if the cool off
 * period of the action has elapsed on all impacted nodes of this action
 */
public class CoolOffDetector {
  private Map<NodeActionKey, Long> actionToExecutionTime;
  private long initTime;
  private Clock clock;

  public CoolOffDetector() {
    this.actionToExecutionTime = new HashMap<>();
    this.clock = Clock.systemUTC();
    this.initTime = clock.millis();
  }

  /**
   * Returns true if a given {@link Action}'s last execution time was >= {@link Action#coolOffPeriodInMillis()} ago
   *
   * <p>If this Publisher has never executed the action, the last execution time is defined as the time when the
   * CoolOffDetector object was constructed.
   * @param action The {@link Action} to test
   * @return true if action is cooled off on all impacted nodes of this action
   */
  public boolean isCooledOff(Action action) {
    for (NodeKey esNode : action.impactedNodes()) {
      boolean cooledOff = checkCooledOff(action.name(), esNode, action.coolOffPeriodInMillis());
      if (!cooledOff) {
        return false;
      }
    }
    return true;
  }

  /**
   * record the timestamp when this action is published
   * @param action action to be recorded
   */
  public void recordAction(Action action) {
    long currentTimestamp = clock.millis();
    for (NodeKey esNode : action.impactedNodes()) {
      NodeActionKey nodeActionKey = new NodeActionKey(action.name(), esNode);
      actionToExecutionTime.put(nodeActionKey, currentTimestamp);
    }
  }

  private boolean checkCooledOff(String actionName, NodeKey esNode, long coolOffPeriod) {
    long currentTimestamp = clock.millis();
    NodeActionKey nodeActionKey = new NodeActionKey(actionName, esNode);
    long lastExecution = actionToExecutionTime.getOrDefault(nodeActionKey, initTime);
    long elapsed = currentTimestamp - lastExecution;
    return elapsed >= coolOffPeriod;
  }

  @VisibleForTesting
  public void setClock(Clock clock) {
    this.clock = clock;
  }

  @VisibleForTesting
  public void setInitTime(long initTime) {
    this.initTime = initTime;
  }

  /**
   * define a hash key class for cool off map.
   */
  private static class NodeActionKey {
    private final NodeKey nodeKey;
    private final String actionName;

    public NodeActionKey(final String actionName, final NodeKey nodeKey) {
      this.nodeKey = nodeKey;
      this.actionName = actionName;
    }

    public NodeKey getNodeKey() {
      return this.nodeKey;
    }

    public String getActionName() {
      return this.actionName;
    }

    @Override
    public boolean equals(Object obj) {
      if (obj instanceof CoolOffDetector.NodeActionKey) {
        CoolOffDetector.NodeActionKey key = (CoolOffDetector.NodeActionKey)obj;
        return nodeKey.equals(key.getNodeKey()) && actionName.equals(key.getActionName());
      }
      return false;
    }

    @Override
    public int hashCode() {
      return new HashCodeBuilder(17, 37)
          .append(nodeKey.hashCode())
          .append(actionName.hashCode())
          .toHashCode();
    }
  }
}
