package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageRca;
import java.util.Arrays;

public class DummyGraph extends AnalysisGraph {

  public static final String LOCUS = "locus";
  public static final String DATA_NODE = "data-node";
  public static final String MASTER_NODE = "master-node";

  @Override
  public void construct() {
    Metric heapUsed = new Heap_Used(5);
    Metric gcEvent = new GC_Collection_Event(5);
    Metric heapMax = new Heap_Max(5);

    heapUsed.addTag(LOCUS, DATA_NODE);
    gcEvent.addTag(LOCUS, DATA_NODE);
    heapMax.addTag(LOCUS, DATA_NODE);
    addLeaf(heapUsed);
    addLeaf(gcEvent);
    addLeaf(heapMax);

    HighHeapUsageRca highHeapUsageRca = new HighHeapUsageRca(5, heapUsed, gcEvent, heapMax);
    highHeapUsageRca.addTag(LOCUS, DATA_NODE);
    highHeapUsageRca.addAllUpstreams(Arrays.asList(heapUsed, gcEvent, heapMax));

    HighHeapUsageClusterRca highHeapUsageClusterRca =
        new HighHeapUsageClusterRca(5, highHeapUsageRca);
    highHeapUsageClusterRca.addTag(LOCUS, MASTER_NODE);
    highHeapUsageClusterRca.addAllUpstreams(Arrays.asList(highHeapUsageRca));
  }
}
