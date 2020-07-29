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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.tasks;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatExceptionCode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse.PublishResponseStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaGraphMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.SubscriptionManager;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Task that sends a flow unit to a remote host.
 */
public class FlowUnitTxTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(FlowUnitTxTask.class);

  /**
   * The client object to make the publish RPC call.
   */
  private final NetClient client;

  /**
   * The subscription manager for getting who to send the flow unit to.
   */
  private final SubscriptionManager subscriptionManager;

  /**
   * The encapsulated flow unit.
   */
  private final DataMsg dataMsg;

  private final AppContext appContext;

  public FlowUnitTxTask(
      final NetClient client,
      final SubscriptionManager subscriptionManager,
      final DataMsg dataMsg,
      final AppContext appContext) {
    this.client = client;
    this.subscriptionManager = subscriptionManager;
    this.dataMsg = dataMsg;
    this.appContext = appContext;
  }

  /**
   * Sends the flow unit across the network.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    final String sourceGraphNode = dataMsg.getSourceGraphNode();
    final InstanceDetails esInstanceDetails = appContext.getMyInstanceDetails();

    if (subscriptionManager.isNodeSubscribed(sourceGraphNode)) {
      final Set<InstanceDetails.Id> downstreamHostIds = subscriptionManager.getSubscribersFor(sourceGraphNode);
      LOG.debug("{} has downstream subscribers: {}", sourceGraphNode, downstreamHostIds);
      for (final InstanceDetails.Id downstreamHostId : downstreamHostIds) {
        for (final GenericFlowUnit flowUnit : dataMsg.getFlowUnits()) {
          LOG.debug("rca: [pub-tx]: {} -> {}", sourceGraphNode, downstreamHostId);
          client.publish(
              appContext.getInstanceById(downstreamHostId),
              flowUnit.buildFlowUnitMessage(sourceGraphNode, esInstanceDetails.getInstanceId()),
              new StreamObserver<PublishResponse>() {
                @Override
                public void onNext(final PublishResponse value) {
                  LOG.debug("rca: Received acknowledgement from the server. status: {}", value.getDataStatus());
                  if (value.getDataStatus() == PublishResponseStatus.NODE_SHUTDOWN) {
                    subscriptionManager.unsubscribeAndTerminateConnection(sourceGraphNode, downstreamHostId);
                    client.flushStream(downstreamHostId);
                  }
                }

                @Override
                public void onError(final Throwable t) {
                  LOG.error("rca: Encountered an exception at the server: ", t);
                  StatsCollector.instance().logException(StatExceptionCode.RCA_NETWORK_ERROR);
                  subscriptionManager.unsubscribeAndTerminateConnection(sourceGraphNode, downstreamHostId);
                  client.flushStream(downstreamHostId);
                }

                @Override
                public void onCompleted() {
                  LOG.debug("rca: Server closed the data channel!");
                }
              });
          PerformanceAnalyzerApp.RCA_GRAPH_METRICS_AGGREGATOR
              .updateStat(RcaGraphMetrics.RCA_NODES_FU_PUBLISH_COUNT, sourceGraphNode, 1);
        }
      }
    } else {
      LOG.debug("No subscribers for {}.", sourceGraphNode);
    }
  }
}
