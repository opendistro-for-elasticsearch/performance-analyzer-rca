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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RcaControllerIt extends RcaController {
  List<ConnectedComponent> rcaGraphComponents;

  public RcaControllerIt(ThreadProvider threadProvider,
                         ScheduledExecutorService netOpsExecutorService,
                         GRPCConnectionManager grpcConnectionManager,
                         ClientServers clientServers,
                         String rca_enabled_conf_location,
                         long rcaStateCheckIntervalMillis,
                         long nodeRoleCheckPeriodicityMillis,
                         AllMetrics.NodeRole nodeRole,
                         final AppContext appContext) {
    super(threadProvider,
        netOpsExecutorService,
        grpcConnectionManager,
        clientServers,
        rca_enabled_conf_location,
        rcaStateCheckIntervalMillis,
        nodeRoleCheckPeriodicityMillis,
        appContext);
    this.currentRole = nodeRole;
  }

  @Override
  protected List<ConnectedComponent> getRcaGraphComponents(RcaConf rcaConf) throws
      ClassNotFoundException,
      NoSuchMethodException,
      InvocationTargetException,
      InstantiationException,
      IllegalAccessException {
    if (rcaGraphComponents != null) {
      return rcaGraphComponents;
    } else {
      return super.getRcaGraphComponents(rcaConf);
    }
  }

  public void setRcaGraphComponents(List<ConnectedComponent> rcaGraphComponents) {
    this.rcaGraphComponents = rcaGraphComponents;
  }

  public void setDbProvider(final Queryable db) {
    dbProvider = db;
  }

  protected void checkUpdateNodeRole() {
  }

  public synchronized void setNodeRole(AllMetrics.NodeRole role) {
    this.currentRole = role;
  }

  public void waitForRcaState(boolean enabled) throws Exception {
    WaitFor.waitFor(() -> isRcaEnabled() == enabled, 20, TimeUnit.SECONDS);
  }
}
