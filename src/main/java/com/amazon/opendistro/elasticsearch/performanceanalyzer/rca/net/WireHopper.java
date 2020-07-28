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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.UnicastIntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks.BroadcastSubscriptionTxTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks.FlowUnitTxTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks.UnicastSubscriptionTxTask;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util.ClusterUtils;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import java.util.List;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.RejectedExecutionException;
import java.util.concurrent.atomic.AtomicReference;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class WireHopper {

  private static final Logger LOG = LogManager.getLogger(WireHopper.class);
  private static final int MS_IN_S = 1000;

  private final NetClient netClient;
  private final SubscriptionManager subscriptionManager;
  private final NodeStateManager nodeStateManager;
  private final AtomicReference<ExecutorService> executorReference;
  private final ReceivedFlowUnitStore receivedFlowUnitStore;
  private final AppContext appContext;

  public WireHopper(
      final NodeStateManager nodeStateManager,
      final NetClient netClient,
      final SubscriptionManager subscriptionManager,
      final AtomicReference<ExecutorService> executorReference,
      final ReceivedFlowUnitStore receivedFlowUnitStore,
      final AppContext appContext) {
    this.netClient = netClient;
    this.subscriptionManager = subscriptionManager;
    this.nodeStateManager = nodeStateManager;
    this.executorReference = executorReference;
    this.receivedFlowUnitStore = receivedFlowUnitStore;
    this.appContext = appContext;
  }

  public void sendIntent(IntentMsg msg) {
    ExecutorService executor = executorReference.get();
    if (executor != null) {
      try {
        executor.execute(new BroadcastSubscriptionTxTask(
            netClient,
            msg,
            subscriptionManager,
            nodeStateManager,
            appContext));
      } catch (final RejectedExecutionException ree) {
        LOG.warn("Dropped sending subscription because the threadpool queue is full");
        StatsCollector.instance()
                      .logException(StatExceptionCode.RCA_NETWORK_THREADPOOL_QUEUE_FULL_ERROR);
      }
    }
  }

  public void sendData(DataMsg msg) {
    ExecutorService executor = executorReference.get();
    if (executor != null) {
      try {
        executor.execute(new FlowUnitTxTask(netClient, subscriptionManager, msg, appContext));
      } catch (final RejectedExecutionException ree) {
        LOG.warn("Dropped sending flow unit because the threadpool queue is full");
        StatsCollector.instance()
                      .logException(StatExceptionCode.RCA_NETWORK_THREADPOOL_QUEUE_FULL_ERROR);
      }
    }
  }

  @VisibleForTesting
  public AppContext getAppContext() {
    return appContext;
  }

  public List<FlowUnitMessage> readFromWire(Node<?> node) {
    final String nodeName = node.name();
    final long intervalInSeconds = node.getEvaluationIntervalSeconds();
    final ImmutableList<FlowUnitMessage> remoteFlowUnits = receivedFlowUnitStore.drainNode(nodeName);

    // Publishers are a set of cluster-instances that send out flowUnits for the corresponding graph node,
    // when one is generated.
    final Set<InstanceDetails.Id> publisherSet = subscriptionManager.getPublishersForNode(nodeName);

    for (final InstanceDetails.Id publisher : publisherSet) {
      if (!ClusterUtils.isHostIdInCluster(publisher, appContext.getAllClusterInstances())) {
        subscriptionManager.unsubscribeAndTerminateConnection(nodeName, publisher);
      }
    }

    final ImmutableList<InstanceDetails> hostsToSubscribeTo =
        nodeStateManager.getStaleOrNotSubscribedNodes(nodeName, 2 * intervalInSeconds * MS_IN_S, publisherSet);

    // There are some stale hosts from which this node hasn't received any FLowUnits. This might be because the remote node
    // restarted and lost out subscription msg. Therefore, we resend it.
    for (final InstanceDetails instance : hostsToSubscribeTo) {
      final ExecutorService executor = executorReference.get();
      if (executor != null) {
        try {
          executor.execute(
                  new UnicastSubscriptionTxTask(
                          netClient,
                          new UnicastIntentMsg("", nodeName, node.getTags(), instance),
                          subscriptionManager,
                          nodeStateManager,
                          appContext));
        } catch (final RejectedExecutionException ree) {
          LOG.warn("Dropped sending subscription request because the threadpool queue is "
              + "full");
          StatsCollector.instance()
                        .logException(StatExceptionCode.RCA_NETWORK_THREADPOOL_QUEUE_FULL_ERROR);
        }
      }
    }
    return remoteFlowUnits;
  }

  @VisibleForTesting
  public void shutdownAll() {
    executorReference.get().shutdown();
    netClient.stop();
    netClient.getConnectionManager().shutdown();
  }
}
