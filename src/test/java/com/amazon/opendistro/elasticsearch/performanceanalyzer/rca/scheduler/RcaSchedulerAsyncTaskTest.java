/*
 *  Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 *  Licensed under the Apache License, Version 2.0 (the "License").
 *  You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Symptom;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Paging_MajfltRate;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Sched_Waittime;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.MetricsDBProviderTestHelper;
import java.nio.file.Paths;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.concurrent.Executors;
import org.junit.Assert;
import org.junit.Test;

public class RcaSchedulerAsyncTaskTest {
  List<String> completionOrderList = Collections.synchronizedList(new ArrayList<>());

  static void sleepWithInterruptHandler(int limit) {
    try {
      Thread.sleep(limit);
    } catch (InterruptedException e) {
      e.printStackTrace();
    }
  }

  class SleepingSymptom extends Symptom {
    private final String myname;
    private final int sleepFor;

    public SleepingSymptom(String myname) {
      this(myname, 5000);
    }

    public SleepingSymptom(String myname, int sleepFor) {
      super(1);
      this.myname = myname;
      this.sleepFor = sleepFor;
    }

    @Override
    public String name() {
      return myname;
    }

    @Override
    public SymptomFlowUnit operate() {
      sleepWithInterruptHandler(sleepFor);
      completionOrderList.add(myname);
      return null;
    }
  }

  class AnalysisGraphT extends AnalysisGraph {
    @Override
    public void construct() {
      // metric nodes (level0)
      Metric m1 = new CPU_Utilization(1);
      Metric m2 = new Sched_Waittime(1);
      Metric m3 = new Paging_MajfltRate(1);
      addLeaf(m1);
      addLeaf(m2);
      addLeaf(m3);

      // Symptoms for level 1
      Symptom s11 = new SleepingSymptom("s11", 100);
      Symptom s12 = new SleepingSymptom("s12", 500);
      Symptom s13 = new SleepingSymptom("s13", 700);
      s11.addAllUpstreams(Arrays.asList(m1, m2, m3));
      s12.addAllUpstreams(Arrays.asList(m1, m2, m3));
      s13.addAllUpstreams(Arrays.asList(m1, m2, m3));

      // Symptoms for level 2
      Symptom s21 = new SleepingSymptom("s21", 400);
      Symptom s22 = new SleepingSymptom("s22", 100);
      Symptom s23 = new SleepingSymptom("s23", 700);
      s21.addAllUpstreams(Arrays.asList(s11, s12, s13));
      s22.addAllUpstreams(Arrays.asList(s11, m2));
      s23.addAllUpstreams(Arrays.asList(s11, s12, s13));

      // Symptoms for level 3
      Symptom s31 = new SleepingSymptom("s31", 100);
      Symptom s32 = new SleepingSymptom("s32", 100);
      Symptom s33 = new SleepingSymptom("s33", 200);
      s31.addAllUpstreams(Arrays.asList(s21, s22, s23));
      s32.addAllUpstreams(Arrays.asList(s22));
      s33.addAllUpstreams(Arrays.asList(s21, s22, s23));
    }
  }

  class RcaSchedulerTaskT extends RCASchedulerTask {
    private static final int THREADS = 3;

    public RcaSchedulerTaskT(List<ConnectedComponent> connectedComponents) throws Exception {
      super(
          1000,
          Executors.newFixedThreadPool(THREADS),
          connectedComponents,
          new MetricsDBProviderTestHelper(true),
          null,
          new RcaConf(Paths.get(RcaConsts.TEST_CONFIG_PATH, "rca.conf").toString()),
          null);
    }

    @Override
    protected void preWait() {
      completionOrderList.add("waiting");
    }

    @Override
    protected void postCompletion(long runStartTime) {
      completionOrderList.add("complete");
    }
  }

  // This tests the order of execution of the nodes. This is to test two things:
  // - The scheduler thread waits for all the graphs nodes to complete execution.
  // - Although, S32, is in the third level, but because its dependencies have already completed,
  //   so instead of waiting for all nodes in a level to run, the scheduler runs whichever can run.
  @Test
  public void testAsyncTaskletRun() throws Exception {
    AnalysisGraph analysisGraph = new AnalysisGraphT();
    List<ConnectedComponent> connectedComponents =
        RcaUtil.getAnalysisGraphComponents(analysisGraph);

    RCASchedulerTask rcaSchedulerTask = new RcaSchedulerTaskT(connectedComponents);
    rcaSchedulerTask.run();
    Assert.assertEquals(
        completionOrderList,
        Arrays.asList(
            "waiting", "s11", "s22", "s32", "s12", "s13", "s21", "s23", "s31", "s33", "complete"));
  }
}
