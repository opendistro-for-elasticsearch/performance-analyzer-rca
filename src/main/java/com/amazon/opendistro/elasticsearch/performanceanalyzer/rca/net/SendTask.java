package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.PublishResponse.PublishResponseStatus;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.net.NetClient;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import io.grpc.stub.StreamObserver;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class SendTask implements Runnable {

  private static final Logger LOG = LogManager.getLogger(SendTask.class);
  private final SubscriptionManager subscriptionManager;
  private final NetworkQueue<DataMsg> txQ;
  private final NetClient netClient;

  public SendTask(final SubscriptionManager subscriptionManager, final NetworkQueue<DataMsg> txQ,
      final NetClient netClient) {
    this.subscriptionManager = subscriptionManager;
    this.txQ = txQ;
    this.netClient = netClient;
  }

  /**
   * When an object implementing interface <code>Runnable</code> is used to create a thread,
   * starting the thread causes the object's
   * <code>run</code> method to be called in that separately executing
   * thread.
   * The general contract of the method <code>run</code> is that it may take any action whatsoever.
   *
   * @see Thread#run()
   */
  @Override
  public void run() {
    for (final DataMsg dataMsg : txQ.drain()) {
      final String sourceNode = dataMsg.getSourceNode();
      final String esNode;
      final NodeDetails currentNode = ClusterDetailsEventProcessor.getCurrentNodeDetails();
      if (currentNode != null) {
        esNode = currentNode.getHostAddress();
      } else {
        LOG.error("Could not get current host address from cluster level metrics reader.");
        esNode = "";
      }
      if (subscriptionManager.isNodeSubscribed(sourceNode)) {
        final Set<String> downstreamHostAddresses = subscriptionManager
            .getSubscribersFor(sourceNode);
        LOG.debug("{} has downstream subscribers: {}", sourceNode, downstreamHostAddresses);
        for (final String downstreamHostAddress : downstreamHostAddresses) {
          for (final GenericFlowUnit flowUnit : dataMsg.getFlowUnits()) {
            netClient.publish(
                downstreamHostAddress,
                flowUnit.buildFlowUnitMessage(sourceNode, esNode),
                new StreamObserver<PublishResponse>() {
                  @Override
                  public void onNext(final PublishResponse value) {
                    LOG.debug(
                        "rca: Received acknowledgement from the server. status: {}",
                        value.getDataStatus());
                    if (value.getDataStatus() == PublishResponseStatus.NODE_SHUTDOWN) {
                      subscriptionManager.unsubscribe(sourceNode, downstreamHostAddress);
                    }
                  }

                  @Override
                  public void onError(final Throwable t) {
                    LOG.error("rca: Encountered an exception at the server: ", t);
                    subscriptionManager.unsubscribe(sourceNode, downstreamHostAddress);
                    // TODO: When an error happens, onCompleted is not guaranteed. So, terminate the
                    // connection.
                  }

                  @Override
                  public void onCompleted() {
                    LOG.debug("rca: Server closed the data channel!");
                  }
                });
          }
        }
      } else {
        LOG.debug("No subscribers for {}.", sourceNode);
      }
    }
  }
}
