package com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.security.InvalidParameterException;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;

public class MetricsRestUtil {
    private static final String WEBSERVICE_BIND_HOST_NAME = "webservice-bind-host";
    private static final Logger LOG = LogManager.getLogger(MetricsRestUtil.class);
    private static final int INCOMING_QUEUE_LENGTH = 1;
    private static final String QUERY_URL = "/_opendistro/_performanceanalyzer/metrics";

    public String nodeJsonBuilder(ConcurrentHashMap<String, String> nodeResponses) {
        StringBuilder outputJson = new StringBuilder();
        outputJson.append("{");
        Set<String> nodeSet = nodeResponses.keySet();
        String[] nodes = nodeSet.toArray(new String[nodeSet.size()]);
        if (nodes.length > 0) {
            outputJson.append("\"");
            outputJson.append(nodes[0]);
            outputJson.append("\": ");
            outputJson.append(nodeResponses.get(nodes[0]));
        }

        for (int i = 1; i < nodes.length; i++) {
            outputJson.append(", \"");
            outputJson.append(nodes[i]);
            outputJson.append("\" :");
            outputJson.append(nodeResponses.get(nodes[i]));
        }

        outputJson.append("}");
        return outputJson.toString();
    }

    public List<String> parseArrayParam(Map<String, String> params, String name, boolean optional) {
        if (!optional) {
            if (!params.containsKey(name) || params.get(name).isEmpty()) {
                throw new InvalidParameterException(String.format("%s parameter needs to be set", name));
            }
        }

        if (params.containsKey(name) && !params.get(name).isEmpty()) {
            return Arrays.asList(params.get(name).split(","));
        }
        return new ArrayList<>();
    }
}
