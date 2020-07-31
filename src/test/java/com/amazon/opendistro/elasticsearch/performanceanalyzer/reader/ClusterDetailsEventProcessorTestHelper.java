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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.reader;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides.ConfigOverrides;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.NodeRole;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.util.JsonConverter;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.List;

public class ClusterDetailsEventProcessorTestHelper extends AbstractReaderTests {

  private static final String SEPARATOR = System.getProperty("line.separator");
  List<String> nodeDetails;

  public ClusterDetailsEventProcessorTestHelper() throws SQLException, ClassNotFoundException {
    super();
    nodeDetails = new ArrayList<>();
  }

  public void addNodeDetails(String nodeId, String address, boolean isMasterNode) {
    nodeDetails.add(createNodeDetailsMetrics(nodeId, address, isMasterNode));
  }

  public void addNodeDetails(String nodeId, String address, NodeRole nodeRole, boolean isMasterNode) {
    nodeDetails.add(createNodeDetailsMetrics(nodeId, address, nodeRole, isMasterNode));
  }

  public static ClusterDetailsEventProcessor.NodeDetails newNodeDetails(final String nodeId, final String address,
                                                                        final boolean isMasterNode) {
    return createNodeDetails(nodeId, address, isMasterNode);
  }

  public ClusterDetailsEventProcessor generateClusterDetailsEvent() {
    if (nodeDetails.isEmpty()) {
      return new ClusterDetailsEventProcessor();
    }
    Event testEvent = generateTestEvent();
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.processEvent(testEvent);
    return clusterDetailsEventProcessor;
  }

  public Event generateTestEvent() {
    return generateTestEventWithOverrides(new ConfigOverrides());
  }

  public Event generateTestEventWithOverrides(ConfigOverrides overrides) {
    StringBuilder stringBuilder = new StringBuilder()
        .append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds());
    stringBuilder.append(SEPARATOR);
    stringBuilder.append(JsonConverter.writeValueAsString(overrides));
    stringBuilder.append(SEPARATOR);
    stringBuilder.append(System.currentTimeMillis());
    nodeDetails.stream().forEach(
        node -> {
          stringBuilder.append(SEPARATOR)
                       .append(node);
        }
    );
    return new Event("", stringBuilder.toString(), 0);
  }
}
