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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Time;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit.HighHeapUsageClusterFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.flowunit.HighHeapUsageFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageOldGenRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageYoungGenRca;
import java.util.Arrays;
import java.util.Collections;

public class DummyGraph extends AnalysisGraph {

  public static final String LOCUS = "locus";
  public static final String DATA_NODE = "data-node";
  public static final String MASTER_NODE = "master-node";

  @Override
  public void construct() {
    Metric heapUsed = new Heap_Used(5);
    Metric gcEvent = new GC_Collection_Event(5);
    Metric heapMax = new Heap_Max(5);
    Metric gc_Collection_Time = new GC_Collection_Time(5);

    heapUsed.addTag(LOCUS, DATA_NODE);
    gcEvent.addTag(LOCUS, DATA_NODE);
    heapMax.addTag(LOCUS, DATA_NODE);
    gc_Collection_Time.addTag(LOCUS, DATA_NODE);
    addLeaf(heapUsed);
    addLeaf(gcEvent);
    addLeaf(heapMax);
    addLeaf(gc_Collection_Time);

    Rca<HighHeapUsageFlowUnit> highHeapUsageOldGenRca = new HighHeapUsageOldGenRca(5, heapUsed, gcEvent, heapMax);
    highHeapUsageOldGenRca.addTag(LOCUS, DATA_NODE);
    highHeapUsageOldGenRca.addAllUpstreams(Arrays.asList(heapUsed, gcEvent, heapMax));

    Rca<HighHeapUsageClusterFlowUnit> highHeapUsageClusterRca =
        new HighHeapUsageClusterRca(5, highHeapUsageOldGenRca);
    highHeapUsageClusterRca.addTag(LOCUS, MASTER_NODE);
    highHeapUsageClusterRca.addAllUpstreams(Collections.singletonList(highHeapUsageOldGenRca));

    Rca<ResourceFlowUnit> highHeapUsageYoungGenRca =
        new HighHeapUsageYoungGenRca(5, heapUsed, gc_Collection_Time);
    highHeapUsageYoungGenRca.addTag(LOCUS, DATA_NODE);
    highHeapUsageYoungGenRca.addAllUpstreams(Arrays.asList(heapUsed, gc_Collection_Time));
  }
}
