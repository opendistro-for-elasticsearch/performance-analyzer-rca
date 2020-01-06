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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import static org.junit.Assert.assertEquals;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.GradleTaskForRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Metric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Symptom;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.CPU_Utilization;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.spec.helpers.AssertHelper;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.Executors;
import org.junit.Ignore;
import org.junit.Test;
import org.junit.experimental.categories.Category;

@Category(GradleTaskForRca.class)
public class RCASchedulerTaskTests {

  /**
   * Given a dependency graph, this test checks that the intents are sent to the right nodes. And
   * after the receipt of the intent, the data is sent to the nodes that have expressed the intent.
   */
  @Test
  public void getLocallyExecutableNodes() {
    final String LOCUS_KEY = "locus";
    final String EARTH_KEY = "earth";
    final String MOON_KEY = "moon";
    final String SKY_LABS_KEY = "sky-labs";
    AnalysisGraph graph =
        new AnalysisGraph() {
          /*
                         ------------------
                        |      CPU        |
                        |       |         |
                        |      \|/        |
                        |     EARTH       |
                        |-------|---------|
                             |    |
                          |       |
                       |          |
                     |            |
            -------|----------    |
           |      \|/        |    |
           |      MOON       |    |
           |                 |    |
           |-----------------|    |
                   \              |
                     \            |
                       \          |
                         \        |
                           \      |
                             \    |
                        |------\--|--------|
                        | cpu   \ |        |
                        |    \   \|        |
                        |     \/  \/       |        Skylabs consumes the locally generated metric
                        |      skylabs     |        CPU and also the metrics from remote nodes moon
                        |------------------|        and earth.

             In this graph:
              IntentFlow:
                  - Moon -> Earth
                  - SkyLabs -> Earth
                  - SkyLabs -> Moon
              DataFlow:
                  - Earth -> Moon
                  - Earth -> SkyLabs
                  - Moon -> Skylabs
              The following code tests this.
          */

          @Override
          public void construct() {
            Metric metric =
                new CPU_Utilization(1) {
                  @Override
                  public MetricFlowUnit gather(Queryable queryable) {
                    return MetricFlowUnit.generic();
                  }
                };
            Symptom earthSymptom =
                new Symptom(1) {
                  @Override
                  public SymptomFlowUnit operate() {
                    return SymptomFlowUnit.generic();
                  }

                  @Override
                  public String name() {
                    return EARTH_KEY;
                  }
                };
            Symptom moonSymptom =
                new Symptom(1) {
                  @Override
                  public SymptomFlowUnit operate() {
                    return SymptomFlowUnit.generic();
                  }

                  public String name() {
                    return MOON_KEY;
                  }
                };

            Metric skyLabCpu =
                new CPU_Utilization(1) {
                  @Override
                  public MetricFlowUnit gather(Queryable queryable) {
                    return MetricFlowUnit.generic();
                  }
                };
            Symptom skyLabsSymptom =
                new Symptom(1) {
                  @Override
                  public SymptomFlowUnit operate() {
                    return SymptomFlowUnit.generic();
                  }

                  public String name() {
                    return SKY_LABS_KEY;
                  }
                };

            addLeaf(metric);
            addLeaf(skyLabCpu);
            earthSymptom.addAllUpstreams(Collections.singletonList(metric));
            moonSymptom.addAllUpstreams(Collections.singletonList(earthSymptom));
            skyLabsSymptom.addAllUpstreams(
                new ArrayList<Node<?>>() {
                  {
                    add(earthSymptom);
                    add(moonSymptom);
                    add(skyLabCpu);
                  }
                });

            metric.addTag(LOCUS_KEY, EARTH_KEY);
            earthSymptom.addTag(LOCUS_KEY, EARTH_KEY);
            moonSymptom.addTag(LOCUS_KEY, MOON_KEY);
            skyLabCpu.addTag(LOCUS_KEY, SKY_LABS_KEY);
            skyLabsSymptom.addTag(LOCUS_KEY, SKY_LABS_KEY);
          }
        };

    RcaConf earthConf =
        new RcaConf() {
          @Override
          public Map<String, String> getTagMap() {
            return new HashMap<String, String>() {
              {
                this.put(LOCUS_KEY, EARTH_KEY);
              }
            };
          }
        };

    RcaConf moonConf =
        new RcaConf() {
          @Override
          public Map<String, String> getTagMap() {
            return new HashMap<String, String>() {
              {
                this.put(LOCUS_KEY, MOON_KEY);
              }
            };
          }
        };

    RcaConf skyLabsConf =
        new RcaConf() {
          @Override
          public Map<String, String> getTagMap() {
            return new HashMap<String, String>() {
              {
                this.put(LOCUS_KEY, SKY_LABS_KEY);
              }
            };
          }
        };

    class WireHopperDerived extends WireHopper {
      DataMsg dataMsg;
      List<IntentMsg> intentMsgs;
      int intextIdx = 0;

      private WireHopperDerived(IntentMsg intentMsg, DataMsg dataMsg) {
        this(Collections.singletonList(intentMsg), dataMsg);
      }

      private WireHopperDerived(List<IntentMsg> intentMsg, DataMsg dataMsg) {
        super(null, null, null, null);
        this.intentMsgs = intentMsg;
        this.dataMsg = dataMsg;
      }

      @Override
      public void sendData(DataMsg dataMsg) {
        assertEquals(dataMsg.getSourceNode(), this.dataMsg.getSourceNode());
        AssertHelper.compareLists(dataMsg.getDestinationNode(), this.dataMsg.getDestinationNode());
      }

      @Override
      public void sendIntent(IntentMsg intentMsg) {
        assertEquals(
            intentMsg.getRequesterNode(), this.intentMsgs.get(intextIdx).getRequesterNode());
        assertEquals(
            intentMsg.getDestinationNode(), this.intentMsgs.get(intextIdx).getDestinationNode());
        intextIdx++;
      }
    }

    List<ConnectedComponent> connectedComponents = RcaUtil.getAnalysisGraphComponents(graph);

    class RCASchedulerTaskMock extends RCASchedulerTask {
      private RCASchedulerTaskMock(RcaConf conf, WireHopper wireHopper) {
        super(
            100,
            Executors.newFixedThreadPool(2),
            connectedComponents,
            null,
            null,
            conf,
            wireHopper);
      }
    }

    RCASchedulerTaskMock earthTask =
        new RCASchedulerTaskMock(
            earthConf,
            new WireHopperDerived(
                Collections.emptyList(),
                new DataMsg(
                    EARTH_KEY,
                    new ArrayList<String>() {
                      {
                        add(MOON_KEY);
                        add(SKY_LABS_KEY);
                      }
                    },
                    null)));

    RCASchedulerTaskMock moonTask =
        new RCASchedulerTaskMock(
            moonConf,
            new WireHopperDerived(
                new IntentMsg(MOON_KEY, EARTH_KEY, null),
                new DataMsg(
                    MOON_KEY,
                    new ArrayList<String>() {
                      {
                        add(SKY_LABS_KEY);
                      }
                    },
                    null)));
    RCASchedulerTaskMock skyLabsTask =
        new RCASchedulerTaskMock(
            skyLabsConf,
            new WireHopperDerived(
                new ArrayList<IntentMsg>() {
                  {
                    add(new IntentMsg(SKY_LABS_KEY, EARTH_KEY, null));
                    add(new IntentMsg(SKY_LABS_KEY, MOON_KEY, null));
                  }
                },
                null));

    earthTask.run();

    moonTask.run();

    skyLabsTask.run();
  }

  @Test
  public void mergeLists() {
    List<List<String>> l1 =
        new ArrayList<List<String>>() {
          {
            add(
                new ArrayList<String>() {
                  {
                    add("1");
                    add("2");
                    add("3");
                  }
                });
            add(
                new ArrayList<String>() {
                  {
                    add("a");
                    add("b");
                    add("c");
                  }
                });

          }
        };
    List<List<String>> l2 =
        new ArrayList<List<String>>() {
          {
            add(
                new ArrayList<String>() {
                  {
                    add("10");
                    add("20");
                    add("30");
                  }
                });
            add(
                new ArrayList<String>() {
                  {
                    add("x");
                    add("y");
                    add("z");
                  }
                });
            add(
                new ArrayList<String>() {
                  {
                    add("xx");
                    add("yy");
                    add("zz");
                  }
                });
          }
        };

    List<List<String>> expected =
        new ArrayList<List<String>>() {
          {
            add(
                new ArrayList<String>() {
                  {
                    add("10");
                    add("20");
                    add("30");
                    add("1");
                    add("2");
                    add("3");
                  }
                });
            add(
                new ArrayList<String>() {
                  {
                    add("x");
                    add("y");
                    add("z");
                    add("a");
                    add("b");
                    add("c");
                  }
                });
            add(
                new ArrayList<String>() {
                  {
                    add("xx");
                    add("yy");
                    add("zz");
                  }
                });
          }
        };
    List<List<String>> ret = RCASchedulerTask.mergeLists(l1, l2);
    assertEquals(expected.size(), ret.size());
    for (int i = 0; i < expected.size(); i++) {
      AssertHelper.compareLists(expected.get(i), ret.get(i));
    }

    // Test for merging with isEmpty list.
    ret = RCASchedulerTask.mergeLists(l1, Collections.emptyList());

    assertEquals(l1.size(), ret.size());
    for (int i = 0; i < ret.size(); i++) {
      AssertHelper.compareLists(l1.get(i), ret.get(i));
    }
  }
}
