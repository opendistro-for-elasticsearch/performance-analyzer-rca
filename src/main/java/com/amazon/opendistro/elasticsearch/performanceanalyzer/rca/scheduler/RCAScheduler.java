/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.List;
import java.util.Set;
import java.util.concurrent.CountDownLatch;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.TimeUnit;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is the top level class for the RCA Scheduler. This initializes all the required objects such
 * as the AnalysisGraph framework, the Queryable instance to get data from MetricsDB, the
 * Persistable instance to dump the results of an RCA into a data store. This then creates an
 * instance of the newScheduledThreadPool so that the Rcas are evaluated with a periodicity. The
 * newScheduledThreadPool takes an instance of RCASchedulerTask which is a wrapper to execute the
 * actual Graph nodes. RCASchedulerTask has its own thread pool which is used to execute the
 * Analysis graph nodes in parallel.
 */
public class RCAScheduler {

  private WireHopper net;
  private boolean shutdownRequested;
  private volatile RcaSchedulerState schedulerState = RcaSchedulerState.STATE_NOT_STARTED;
  private final NodeRole role;
  private final AppContext appContext;

  final ThreadFactory schedThreadFactory;

  // TODO: Fix number of threads based on config.
  final ThreadFactory taskThreadFactory;

  ExecutorService rcaSchedulerPeriodicExecutor;
  ScheduledExecutorService scheduledPool;

  List<ConnectedComponent> connectedComponents;
  volatile Queryable db;
  RcaConf rcaConf;
  ThresholdMain thresholdMain;
  Persistable persistable;
  static final int PERIODICITY_SECONDS = 1;
  static final int PERIODICITY_IN_MS = PERIODICITY_SECONDS * 1000;

  private static final Logger LOG = LogManager.getLogger(RCAScheduler.class);

  private CountDownLatch schedulerTrackingLatch;

  public RCAScheduler(
      List<ConnectedComponent> connectedComponents,
      Queryable db,
      RcaConf rcaConf,
      ThresholdMain thresholdMain,
      Persistable persistable,
      WireHopper net,
      final AppContext appContext) {
    String instanceId = appContext.getMyInstanceDetails().getInstanceId().toString();
    this.schedThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(instanceId + "-sched-%d")
        .setDaemon(true)
        .build();

    // TODO: Fix number of threads based on config.
    this.taskThreadFactory = new ThreadFactoryBuilder()
        .setNameFormat(instanceId + "-task-%d-")
        .setDaemon(true)
        .build();

    this.connectedComponents = connectedComponents;
    this.db = db;
    this.rcaConf = rcaConf;
    this.thresholdMain = thresholdMain;
    this.persistable = persistable;
    this.net = net;
    this.shutdownRequested = false;
    this.appContext = appContext;
    this.role = this.appContext.getMyInstanceDetails().getRole();
  }

  public void start() {
    // Implement multiple tasks scheduled at different ticks.
    // Simulation service
    LOG.info("RCA: Starting RCA scheduler ...........");
    createExecutorPools();

    if (scheduledPool == null) {
      LOG.error("Couldn't start RCA scheduler. Executor pool is not set.");
      if (schedulerTrackingLatch != null) {
        schedulerTrackingLatch.countDown();
      }
      return;
    }
    if (role == NodeRole.UNKNOWN) {
      LOG.error("Couldn't start RCA scheduler as the node role is UNKNOWN.");
      if (schedulerTrackingLatch != null) {
        schedulerTrackingLatch.countDown();
      }
      return;
    }

    final RCASchedulerTask task = new RCASchedulerTask(
        10000,
        rcaSchedulerPeriodicExecutor,
        connectedComponents,
        db,
        persistable,
        rcaConf,
        net,
        appContext);

    schedulerState = RcaSchedulerState.STATE_STARTED;
    LOG.info("RCA scheduler thread started successfully on node: {}",
        appContext.getMyInstanceDetails().getInstanceId());
    if (schedulerTrackingLatch != null) {
      schedulerTrackingLatch.countDown();
    }

    while (schedulerState == RcaSchedulerState.STATE_STARTED) {
      try {
        long startTime = System.currentTimeMillis();
        task.run();
        long duration = System.currentTimeMillis() - startTime;
        if (duration < PERIODICITY_IN_MS) {
          Thread.sleep(PERIODICITY_IN_MS - duration);
        }
      } catch (InterruptedException ie) {
        LOG.error("**ERR: Rca scheduler thread sleep interrupted.", ie);
        shutdown();
        schedulerState = RcaSchedulerState.STATE_STOPPED_DUE_TO_EXCEPTION;
      } catch (Exception ex) {
        LOG.error("**ERR Scheduler failed: ", ex);
      }
    }
  }

  /**
   * Signal a shutdown on the scheduled pool first and then to the executor pool. Calling a shutdown
   * on them does not lead to immediate shutdown instead, they stop taking new tasks and wait for
   * the running tasks to complete. This is where the waitForShutdown is important. We want to wait
   * for all the tasks to end their work before we close the database connection.
   */
  public void shutdown() {
    LOG.info("Shutting down the scheduler..");
    shutdownRequested = true;
    scheduledPool.shutdown();
    waitForShutdown(scheduledPool);
    rcaSchedulerPeriodicExecutor.shutdown();
    waitForShutdown(rcaSchedulerPeriodicExecutor);
    try {
      persistable.close();
    } catch (SQLException e) {
      LOG.error(
          "RCA: Error while closing the DB connection: {}::{}", e.getErrorCode(), e.getCause());
    }
    schedulerState = RcaSchedulerState.STATE_STOPPED;
    if (schedulerTrackingLatch != null) {
      schedulerTrackingLatch.countDown();
    }
  }

  private void waitForShutdown(ExecutorService execPool) {
    try {
      if (!execPool.awaitTermination(PERIODICITY_SECONDS * 2, TimeUnit.SECONDS)) {
        execPool.shutdownNow();
      }
    } catch (InterruptedException e) {
      LOG.error("RCA: Error in call to shutdownNow. {}", e.getMessage());
      execPool.shutdownNow();
    }
  }

  public RcaSchedulerState getState() {
    return this.schedulerState;
  }

  private void createExecutorPools() {
    scheduledPool = Executors.newScheduledThreadPool(1, schedThreadFactory);
    rcaSchedulerPeriodicExecutor = Executors.newFixedThreadPool(2, taskThreadFactory);
  }

  /**
   * Updates the list of muted actions in the current instance of {@link AppContext}.
   *
   * @param mutedActions The set of actions names that need to be muted.
   */
  public void updateAppContextWithMutedActions(final Set<String> mutedActions) {
    if (this.appContext != null) {
      this.appContext.updateMutedActions(mutedActions);
    }
  }

  public NodeRole getRole() {
    return role;
  }

  public void setSchedulerTrackingLatch(final CountDownLatch schedulerTrackingLatch) {
    this.schedulerTrackingLatch = schedulerTrackingLatch;
  }

  @VisibleForTesting
  public void setQueryable(Queryable queryable) {
    this.db = queryable;
  }

  @VisibleForTesting
  public AppContext getAppContext() {
    return this.appContext;
  }
}
