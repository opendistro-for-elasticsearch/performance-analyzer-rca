package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.google.common.annotations.VisibleForTesting;
import java.util.List;
import java.util.stream.Collectors;

/**
 * Utility class to get details about the nodes in the cluster.
 */
public class ClusterUtils {

  @VisibleForTesting
  static final String EMPTY_STRING = "";

  /**
   * Get host addresses for all the other nodes in the cluster.
   *
   * @return List of host addresses.
   */
  public static List<String> getAllPeerHostAddresses() {
    return ClusterDetailsEventProcessor.getNodesDetails().stream()
                                       .skip(1)
                                       .map(NodeDetails::getHostAddress)
                                       .collect(Collectors.toList());
  }

  /**
   * Checks if the given host address is part of the cluster.
   *
   * @param hostAddress The host address to check membership for.
   * @return true if the host address is part of the cluster, false otherwise.
   */
  public static boolean isHostAddressInCluster(final String hostAddress) {
    final List<NodeDetails> nodes = ClusterDetailsEventProcessor.getNodesDetails();

    if (nodes.size() > 0) {
      for (NodeDetails node : nodes) {
        if (node.getHostAddress()
                .equals(hostAddress)) {
          return true;
        }
      }
    }

    return false;
  }
}
