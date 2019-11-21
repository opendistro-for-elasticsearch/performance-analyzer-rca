package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterLevelMetricsReader;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterLevelMetricsReader.NodeDetails;

import java.util.HashMap;
import java.util.Map;

public class NodeStateManager {
    private static final long MS_IN_S = 1000;
    private static final long MS_IN_FIVE_SECONDS = 5 * MS_IN_S;
    private static final String SEPARATOR = ".";

    private Map<String, Long> lastReceivedTimestampMap = new HashMap<>();

    public void updateReceiveTime(String host, String graphNode) {
        final long currentTimeStamp = System.currentTimeMillis();
        final String compositeKey = graphNode + SEPARATOR + host;
        lastReceivedTimestampMap.put(compositeKey, currentTimeStamp);
    }

    public long getLastReceivedTimestamp(String graphNode, String host) {
        final String compositeKey = graphNode + SEPARATOR + host;
        if (lastReceivedTimestampMap.containsKey(compositeKey)) {
            return lastReceivedTimestampMap.get(compositeKey);
        }

        // Return a value that is in the future so that it doesn't cause
        // side effects.
        return System.currentTimeMillis() + MS_IN_FIVE_SECONDS;
    }

    public boolean isRemoteHostInCluster(final String remoteHost) {
        final NodeDetails[] nodes = ClusterLevelMetricsReader.getNodes();

        if (nodes.length > 0) {
            for (NodeDetails node : nodes) {
                if (node.getHostAddress()
                        .equals(remoteHost)) {
                    return true;
                }
            }
        }

        return false;
    }
}
