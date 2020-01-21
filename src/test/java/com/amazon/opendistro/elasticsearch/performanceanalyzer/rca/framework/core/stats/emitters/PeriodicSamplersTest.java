package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.emitters;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collectors.StatsCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.RcaStatsReporter;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.collectors.samplers.SampleCollector;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.stats.measurements.sampled.LivenessMeasurements;
import java.util.HashMap;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class PeriodicSamplersTest {
    class TestStatsCollector extends StatsCollector {
        TestStatsCollector() {
            super("stats", 1, new HashMap<>());
        }
        public StringBuilder getRcaMetrics() {
            return collectRcaStats(true);
        }
    }
    @Test
    public void emitters() {
        TestStatsCollector tsc = new TestStatsCollector();
        SampleCollector resourceSampler = new SampleCollector(LivenessMeasurements.values());

        RcaStatsReporter statsReporter = new RcaStatsReporter();
        statsReporter.addCollector(resourceSampler);
        statsReporter.setReady();

        PeriodicSamplers periodicSamplers = new PeriodicSamplers(resourceSampler);
        periodicSamplers.run();

        StringBuilder sb = tsc.getRcaMetrics();
        System.out.println(sb);
    }

}