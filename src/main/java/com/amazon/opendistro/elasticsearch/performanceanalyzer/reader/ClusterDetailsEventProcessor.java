/*
 * Copyright <2019> Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;
import java.util.HashSet;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ClusterDetailsEventProcessor implements EventProcessor {
  private static final Logger LOG = LogManager.getLogger(ClusterDetailsEventProcessor.class);

  @Override
  public void initializeProcessing(long startTime, long endTime) {}

  @Override
  public void finalizeProcessing() {}

  @Override
  public void processEvent(Event event) {
    String[] lines = event.value.split(System.lineSeparator());
    if (lines.length < 2) {
      // We expect at-least 2 lines as the first line is always timestamp
      // and there must be at least one ElasticSearch node in a cluster.
      LOG.error(
          "ClusterDetails contain less items than expected. " + "Expected 2, found: {}",
          event.value);
      return;
    }
    // An example node_metrics data is something like this for a two node cluster:
    // {"current_time":1566414001749}
    // {"ID":"4sqG_APMQuaQwEW17_6zwg","HOST_ADDRESS":"10.212.73.121"}
    // {"ID":"OVH94mKXT5ibeqvDoAyTeg","HOST_ADDRESS":"10.212.78.83"}
    //
    // The line 0 is timestamp that can be skipped. So we allocated size of
    // the array is one less than the list.

    ClusterLevelMetricsReader.NodeDetails[] tmpNodesDetails =
        new ClusterLevelMetricsReader.NodeDetails[lines.length - 1];
    int tmpNodesDetailsIdx = 0;

    // Just to keep track of duplicate node ids.
    Set<String> ids = new HashSet<>();

    for (int i = 1; i < lines.length; ++i) {
      ClusterLevelMetricsReader.NodeDetails nodeDetails =
          new ClusterLevelMetricsReader.NodeDetails(lines[i]);

      // Include nodeIds we haven't seen so far.
      if (ids.add(nodeDetails.getId())) {
        tmpNodesDetails[tmpNodesDetailsIdx] = nodeDetails;
        tmpNodesDetailsIdx += 1;
      } else {
        LOG.info("node id {}, logged twice.", nodeDetails.getId());
      }
    }
    ClusterLevelMetricsReader.setNodesDetails(tmpNodesDetails);
  }

  @Override
  public boolean shouldProcessEvent(Event event) {
    return event.key.contains(PerformanceAnalyzerMetrics.sNodesPath);
  }

  @Override
  public void commitBatchIfRequired() {}
}
