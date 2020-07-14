/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.google.common.annotations.VisibleForTesting;

public class InstanceDetails {
  private final AllMetrics.NodeRole role;
  private final String InstanceId;
  private final String instanceIp;
  private final boolean isMaster;

  public InstanceDetails(AllMetrics.NodeRole role, String instanceId, String instanceIp, boolean isMaster) {
    this.role = role;
    InstanceId = instanceId;
    this.instanceIp = instanceIp;
    this.isMaster = isMaster;
  }

  public InstanceDetails(AllMetrics.NodeRole role) {
    this(role, "", "", false);
  }

  public AllMetrics.NodeRole getRole() {
    return role;
  }

  public String getInstanceId() {
    return InstanceId;
  }

  public String getInstanceIp() {
    return instanceIp;
  }

  public boolean getIsMaster() {
    return isMaster;
  }
}
