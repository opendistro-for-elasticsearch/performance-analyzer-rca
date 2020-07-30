package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.AppContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import java.util.List;

/**
 * Utility class to get details about the nodes in the cluster.
 */
public class ClusterUtils {
  public static boolean isHostIdInCluster(final InstanceDetails.Id hostId, final List<InstanceDetails> clusterInstances) {
    return clusterInstances
            .stream()
            .anyMatch(
                    x -> hostId.equals(x.getInstanceId())
            );
  }
}
