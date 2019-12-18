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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ThresholdMain;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.google.common.util.concurrent.ThreadFactoryBuilder;
import java.sql.SQLException;
import java.util.List;
import java.util.concurrent.CancellationException;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledFuture;
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
  private boolean running = false;
  private NodeRole role = NodeRole.UNKNOWN;
  final ThreadFactory schedThreadFactory =
      new ThreadFactoryBuilder().setNameFormat("sched-%d").setDaemon(true).build();

  // TODO: Fix number of threads based on config.
  final ThreadFactory taskThreadFactory =
      new ThreadFactoryBuilder().setNameFormat("task-%d-").setDaemon(true).build();

  ExecutorService executorPool;
  ScheduledExecutorService scheduledPool;

  List<ConnectedComponent> connectedComponents;
  Queryable db;
  RcaConf rcaConf;
  ThresholdMain thresholdMain;
  Persistable persistable;
  static final int PERIODICITY_SECONDS = 1;
  ScheduledFuture<?> futureHandle;

  private static final Logger LOG = LogManager.getLogger(RCAScheduler.class);

  public RCAScheduler(
      List<ConnectedComponent> connectedComponents,
      Queryable db,
      RcaConf rcaConf,
      ThresholdMain thresholdMain,
      Persistable persistable,
      WireHopper net) {
    this.connectedComponents = connectedComponents;
    this.db = db;
    this.rcaConf = rcaConf;
    this.thresholdMain = thresholdMain;
    this.persistable = persistable;
    this.net = net;
    this.shutdownRequested = false;
  }

  public void start() {
    // Implement multiple tasks scheduled at different ticks.
    // Simulation service
    LOG.info("RCA: Starting RCA scheduler ...........");
    createExecutorPools();

    if (scheduledPool != null && role != NodeRole.UNKNOWN) {
      futureHandle =
          scheduledPool.scheduleAtFixedRate(
              new RCASchedulerTask(
                  10000, executorPool, connectedComponents, db, persistable, rcaConf, net),
              1,
              PERIODICITY_SECONDS,
              TimeUnit.SECONDS);
      startExceptionHandlerThread();
      this.running = true;
    } else {
      LOG.error("Couldn't start RCA scheduler. Executor pool is not set.");
    }
  }

  // This thread exists for exception handling and error recovery and safe shutdown. This is called
  // from within
  // the start method. This creates a new thread and waits on the future to complete. If it catches
  // an exception,
  // then it does a clean shutdown nd logs the shutdown event.
  private void startExceptionHandlerThread() {
    new Thread(
            () -> {
              while (true) {
                try {
                  futureHandle.get();
                } catch (RejectedExecutionException
                    | ExecutionException
                    | CancellationException ex) {
                  if (!shutdownRequested) {
                    LOG.error("Exception cause : {}", ex.getCause());
                    shutdown();
                  }
                } catch (InterruptedException ix) {
                  LOG.error("Interrupted exception cause : {}", ix.getCause());
                  shutdown();
                }
              }
            })
        .start();
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
    executorPool.shutdown();
    waitForShutdown(executorPool);
    try {
      persistable.close();
    } catch (SQLException e) {
      LOG.error(
          "RCA: Error while closing the DB connection: {}::{}", e.getErrorCode(), e.getCause());
    }
    running = false;
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

  public boolean isRunning() {
    return running;
  }

  private void createExecutorPools() {
    scheduledPool = Executors.newScheduledThreadPool(1, schedThreadFactory);
    executorPool = Executors.newFixedThreadPool(1, taskThreadFactory);
  }

  public NodeRole getRole() {
    return role;
  }

  public void setRole(NodeRole role) {
    this.role = role;
  }
}
