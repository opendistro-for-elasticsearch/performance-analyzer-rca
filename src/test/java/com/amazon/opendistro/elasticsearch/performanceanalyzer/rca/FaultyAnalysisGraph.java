package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.DummyGraph.DATA_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.DummyGraph.LOCUS;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Symptom;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_AllocRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Paging_MajfltRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Sched_Waittime;
import java.util.Arrays;

public class FaultyAnalysisGraph extends AnalysisGraph {

    @Override
    public void construct() {
        Metric cpuUtilization = new CPU_Utilization(5);
        Metric heapUsed = new Sched_Waittime(5);
        Metric pageMaj = new Paging_MajfltRate(5);
        Metric heapAlloc = new Heap_AllocRate(5);

        addLeaf(cpuUtilization);
        addLeaf(heapUsed);
        addLeaf(pageMaj);
        addLeaf(heapAlloc);

        Symptom s1 = new HighCpuSymptom(5, cpuUtilization, heapUsed);
        s1.addTag(LOCUS, DATA_NODE);
        s1.addAllUpstreams(Arrays.asList(cpuUtilization, heapUsed));

        System.out.println(this.getClass().getName() + " graph constructed..");
    }

    class HighCpuSymptom extends Symptom {
        Metric cpu;
        Metric heapUsed;

        public HighCpuSymptom(long evaluationIntervalSeconds, Metric cpu, Metric heapUsed) {
            super(evaluationIntervalSeconds);
            this.cpu = cpu;
            this.heapUsed = heapUsed;
        }

        @Override
        public SymptomFlowUnit operate() {
            int x = 5 / 0;
            return new SymptomFlowUnit(0L);
        }
    }
}
