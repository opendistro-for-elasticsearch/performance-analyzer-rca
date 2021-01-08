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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.overrides;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.ClientServers;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.GRPCConnectionManager;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RCAScheduler;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.RcaSchedulerState;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.threads.ThreadProvider;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.WaitFor;
import java.lang.reflect.InvocationTargetException;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

public class RcaControllerIt extends RcaController {
  private final String rcaPath;
  private List<ConnectedComponent> rcaGraphComponents;

  public RcaControllerIt(ThreadProvider threadProvider,
                         ScheduledExecutorService netOpsExecutorService,
                         GRPCConnectionManager grpcConnectionManager,
                         ClientServers clientServers,
                         String rca_enabled_conf_location,
                         long rcaStateCheckIntervalMillis,
                         long nodeRoleCheckPeriodicityMillis,
                         AllMetrics.NodeRole nodeRole,
                         final AppContext appContext,
                         final Queryable dbProvider) {
    super(threadProvider,
        netOpsExecutorService,
        grpcConnectionManager,
        clientServers,
        rca_enabled_conf_location,
        rcaStateCheckIntervalMillis,
        nodeRoleCheckPeriodicityMillis,
        appContext,
        dbProvider);
    this.currentRole = nodeRole;
    this.rcaPath = rca_enabled_conf_location;
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

  @Override
  protected RcaConf getRcaConfForMyRole(AllMetrics.NodeRole nodeRole) {
    RcaConfIt rcaConfIt = new RcaConfIt(super.getRcaConfForMyRole(nodeRole));
    rcaConfIt.setRcaDataStorePath(rcaPath);
    return rcaConfIt;
  }

  public void setDbProvider(final Queryable db) throws InterruptedException {
    dbProvider = db;
    RCAScheduler sched = getRcaScheduler();

    // The change is optional and only happens in the next line if the scheduler is already running.
    // If the scheduler is not running at the moment, then it will pick up the new DB when it starts
    // next.
    if (sched != null) {
      sched.setQueryable(db);
    }
  }

  public void setRcaGraphComponents(Class rcaGraphClass)
      throws NoSuchMethodException, IllegalAccessException, InvocationTargetException, InstantiationException {
    AnalysisGraph graphObject =
        (AnalysisGraph) rcaGraphClass.getDeclaredConstructor().newInstance();
    this.rcaGraphComponents = RcaUtil.getAnalysisGraphComponents(graphObject);
  }

  public void waitForRcaState(RcaSchedulerState state) throws Exception {
    WaitFor.waitFor(() -> getRcaScheduler() != null && getRcaScheduler().getState() == state, 20, TimeUnit.SECONDS);
  }
}
