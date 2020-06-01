package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_AllocRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Paging_MajfltRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Sched_Waittime;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.ShardSizeInBytes;


public class AnalysisGraphTest extends AnalysisGraph {

  @Override
  public void construct() {
    Metric cpuUtilization = new CPU_Utilization(5);
    Metric heapUsed = new Sched_Waittime(5);
    Metric pageMaj = new Paging_MajfltRate(5);
    Metric heapAlloc = new Heap_AllocRate(5);
    Metric shardSize = new ShardSizeInBytes(5);

    addLeaf(cpuUtilization);
    addLeaf(heapUsed);
    addLeaf(pageMaj);
    addLeaf(heapAlloc);
    addLeaf(shardSize);

    System.out.println(this.getClass().getName() + " graph constructed..");
  }
}
