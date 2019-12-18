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

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.PerformanceAnalyzerMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ClusterDetailsEventProcessor.NodeDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader_writer_shared.Event;

import java.sql.SQLException;
import java.util.List;

import org.junit.Test;

public class ClusterDetailsEventProcessorTests extends AbstractReaderTests {

  public ClusterDetailsEventProcessorTests() throws SQLException, ClassNotFoundException {
    super();
  }

  @Test
  public void testProcessEvent() throws Exception {

    String nodeId1 = "s7gDCVnCSiuBgHoYLji1gw";
    String address1 = "10.212.49.140";

    String nodeId2 = "Zn1QcSUGT--DciD1Em5wRg";
    String address2 = "10.212.52.241";

    StringBuilder stringBuilder = new StringBuilder()
        .append(PerformanceAnalyzerMetrics.getJsonCurrentMilliSeconds())
        .append(System.getProperty("line.separator"))
        .append(createNodeDetailsMetrics(nodeId1, address1))
        .append(System.getProperty("line.separator"))
        .append(createNodeDetailsMetrics(nodeId2, address2));

    Event testEvent = new Event("", stringBuilder.toString(), 0);
    ClusterDetailsEventProcessor clusterDetailsEventProcessor = new ClusterDetailsEventProcessor();
    clusterDetailsEventProcessor.processEvent(testEvent);

    List<NodeDetails> nodes = ClusterDetailsEventProcessor.getNodesDetails();

    assertEquals(nodeId1, nodes.get(0).getId());
    assertEquals(address1, nodes.get(0).getHostAddress());

    assertEquals(nodeId2, nodes.get(1).getId());
    assertEquals(address2, nodes.get(1).getHostAddress());
  }


}
