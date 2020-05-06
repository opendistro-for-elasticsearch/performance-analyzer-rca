/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.decision_maker;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.RemediationController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.request.RemediationRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.request.WriteQueueRequest;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.remediation.request.WriteQueueRequest.Action;
import java.util.List;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class QueueAutoTuningDecisionMaker extends DecisionMaker<ResourceFlowUnit> {
  private static final Logger LOG = LogManager.getLogger(QueueAutoTuningDecisionMaker.class);
  private final Rca<ResourceFlowUnit> queueRejectionClusterRca;
  private final Rca<ResourceFlowUnit> highHeapUsageClusterRca;
  private final int rcaPeriod;
  private int counter;

  public <R extends Rca> QueueAutoTuningDecisionMaker(final int rcaPeriod, R queueRejectionClusterRca, R highHeapUsageClusterRca) {
    this.rcaPeriod = rcaPeriod;
    this.queueRejectionClusterRca = queueRejectionClusterRca;
    this.highHeapUsageClusterRca = highHeapUsageClusterRca;
    this.counter = 0;
  }

  @Override
  public ResourceFlowUnit operate() {
    counter += 1;
    if (counter >= rcaPeriod) {
      counter = 0;
      List<ResourceFlowUnit> queueRejFlowUnits = queueRejectionClusterRca.getFlowUnits();
      List<ResourceFlowUnit> jvmFlowUnits = highHeapUsageClusterRca.getFlowUnits();
      Action resizingAction = null;
      // scale down to min
      if (hasUnhealthyFlowUnit(jvmFlowUnits)) {
        resizingAction = Action.SIZE_DOWN_TO_MIN;
      }
      //scale up
      else if (hasUnhealthyFlowUnit(queueRejFlowUnits)) {
        resizingAction = Action.SIZE_UP_BY_10;
      }
      if (resizingAction != null) {
        RemediationRequest request = new WriteQueueRequest(resizingAction);
        RemediationController.instance().getBlockingQueue().add(request);
      }
    }
    return new ResourceFlowUnit(System.currentTimeMillis());
  }

  private boolean hasUnhealthyFlowUnit(List<ResourceFlowUnit> flowUnits) {
    if (flowUnits.isEmpty()) {
      return false;
    }
    return flowUnits.get(0).getResourceContext().isUnhealthy();
  }
}
