package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import java.util.List;

/**
 * Utility class to get details about the nodes in the cluster.
 */
public class ClusterUtils {
  public static boolean isHostAddressInCluster(final String hostAddress, final List<InstanceDetails> clusterInstances) {
    if (clusterInstances.size() > 0) {
      for (InstanceDetails node : clusterInstances) {
        if (node.getInstanceIp().equals(hostAddress)) {
          return true;
        }
      }
    }
    return false;
  }
}
