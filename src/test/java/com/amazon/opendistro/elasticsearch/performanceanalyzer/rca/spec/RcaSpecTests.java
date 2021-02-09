/*
 * Copyright 2019-2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec;

import static org.hamcrest.CoreMatchers.is;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertThat;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Symptom;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.hotheap.HighHeapUsageOldGenRca;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import org.junit.Before;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RcaSpecTests {

  @Before
  public void cleanup() {
    Stats.getInstance().reset();
    Stats.clear();
  }

  @Test
  public void testCreation() {

    class SymptomX extends Symptom {
      SymptomX(long evaluationIntervalMins) {
        super(evaluationIntervalMins);
      }

      @Override
      public SymptomFlowUnit operate() {
        return SymptomFlowUnit.generic();
      }
    }

    class RcaX extends Rca {
      RcaX() {
        super(5);
      }

      @Override
      public ResourceFlowUnit operate() {
        return ResourceFlowUnit.generic();
      }

      @Override
      public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {}
    }

    class AnalysisGraphX extends AnalysisGraph {
      @Override
      public void construct() {
        Metric metric1 = new CPU_Utilization(5);
        addLeaf(metric1);

        SymptomX symptom = new SymptomX(5);

        List<Node<?>> lsym = new ArrayList<>();
        lsym.add(metric1);
        symptom.addAllUpstreams(lsym);

        RcaX rca = new RcaX();
        lsym = new ArrayList<>();
        lsym.add(symptom);
        rca.addAllUpstreams(lsym);
      }
    }

    AnalysisGraphX fieldx = new AnalysisGraphX();
    fieldx.construct();
  }

  @Test
  public void testElastiSearchAnalysisFlowField() {
    ElasticSearchAnalysisGraph field = new ElasticSearchAnalysisGraph();
    field.construct();
  }

  // @Test(expected = RuntimeException.class)
  public void testAddToFlowFieldBeforeAddingAsDependency() {
    Metric heapUsed = new Heap_Used(5);
    HighHeapUsageOldGenRca highHeapUsageOldGenRca =
        new HighHeapUsageOldGenRca(1, heapUsed, null, null, null);
    highHeapUsageOldGenRca.addAllUpstreams(Collections.singletonList(heapUsed));
  }

  @Test
  public void testGraphId() {
    class TestMetric1 extends Metric {

      TestMetric1() {
        super("test-metric1", 5);
      }
    }

    class TestMetric2 extends Metric {

      TestMetric2() {
        super("test-metric2", 5);
      }
    }

    class TestMetric3 extends Metric {

      TestMetric3() {
        super("test-metric3", 5);
      }
    }

    class TestMetric4 extends Metric {

      TestMetric4() {
        super("test-metric4", 5);
      }
    }

    class TestMetric5 extends Metric {

      TestMetric5() {
        super("test-metric5", 5);
      }
    }

    class TestSymptom1 extends Symptom {

      TestSymptom1() {
        super(1);
      }

      @Override
      public SymptomFlowUnit operate() {
        return null;
      }
    }

    class TestSymptom2 extends Symptom {

      TestSymptom2() {
        super(1);
      }

      @Override
      public SymptomFlowUnit operate() {
        return null;
      }
    }

    class TestSymptom3 extends Symptom {

      TestSymptom3() {
        super(1);
      }

      @Override
      public SymptomFlowUnit operate() {
        return null;
      }
    }

    class TestRCA1 extends Rca {

      TestRCA1() {
        super(5);
      }

      @Override
      public ResourceFlowUnit operate() {
        return null;
      }

      @Override
      public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {}
    }

    class TestRCA2 extends Rca {

      TestRCA2() {
        super(5);
      }

      @Override
      public ResourceFlowUnit operate() {
        return null;
      }

      @Override
      public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {}
    }

    class TestRCA3 extends Rca {

      TestRCA3() {
        super(2);
      }

      @Override
      public ResourceFlowUnit operate() {
        return null;
      }

      @Override
      public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {}
    }

    class TestGraph extends AnalysisGraph {

      @Override
      public void construct() {
        Metric m1 = new TestMetric1();
        Metric m2 = new TestMetric2();
        Metric m3 = new TestMetric3();
        Metric m4 = new TestMetric4();
        Metric m5 = new TestMetric5();

        Symptom s1 = new TestSymptom1();
        Symptom s2 = new TestSymptom2();
        Symptom s3 = new TestSymptom3();

        Rca r1 = new TestRCA1();
        Rca r2 = new TestRCA2();
        Rca r3 = new TestRCA3();

        addLeaf(m1);
        addLeaf(m2);
        addLeaf(m3);
        addLeaf(m4);
        addLeaf(m5);

        // spotless:off
        /*
           m1      m2      m3    m4      m5
            \     /       /      /        |
             \   /      /       /        s3
              s1       /       /          |
                \    /        /          r3
                 \  /        /
                  s2        /
                   |       /
                   r1     /
                     \   /
                      r2
        */
        // spotless:on

        s1.addAllUpstreams(Arrays.asList(m1, m2));
        s2.addAllUpstreams(Arrays.asList(s1, m3));
        r1.addAllUpstreams(Collections.singletonList(s2));
        r2.addAllUpstreams(Arrays.asList(r1, m4));

        s3.addAllUpstreams(Collections.singletonList(m5));
        r3.addAllUpstreams(Collections.singletonList(s3));
      }
    }

    AnalysisGraph field = new TestGraph();
    field.construct();
    field.validateAndProcess();

    Stats stats = Stats.getInstance();

    assertEquals(2, stats.getGraphsCount());
    assertEquals(11, stats.getTotalNodesCount());
    assertEquals(5, stats.getLeafNodesCount());
    assertEquals(5, stats.getLeavesAddedToAnalysisFlowField());

    List<List<String>> expectedG1 =
        Arrays.asList(
            Arrays.asList("TestMetric1", "TestMetric2", "TestMetric3", "TestMetric4"),
            Collections.singletonList("TestSymptom1"),
            Collections.singletonList("TestSymptom2"),
            Collections.singletonList("TestRCA1"),
            Collections.singletonList("TestRCA2"));

    List<List<String>> expectedG2 =
        Arrays.asList(
            Collections.singletonList("TestMetric5"),
            Collections.singletonList("TestSymptom3"),
            Collections.singletonList("TestRCA3"));

    int idx1 = 0;
    for (ConnectedComponent g : stats.getConnectedComponents()) {
      List<List<String>> expected;
      if (0 == idx1) {
        expected = expectedG1;
      } else {
        expected = expectedG2;
      }
      int idx2 = 0;
      for (List<Node<?>> parallelExecutables : g.getAllNodesByDependencyOrder()) {
        parallelExecutables.sort(
            (o1, o2) -> o1.getClass().getSimpleName().compareTo(o2.getClass().getSimpleName()));
        List<String> actual =
            parallelExecutables.stream()
                .map(n -> n.getClass().getSimpleName())
                .collect(Collectors.toList());
        assertThat(actual, is(expected.get(idx2)));
        ++idx2;
      }
      ++idx1;
    }
  }
}
