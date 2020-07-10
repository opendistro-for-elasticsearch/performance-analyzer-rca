/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.rcaGraphs;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Defaults.RCA_CONF_DATA_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Defaults.RCA_CONF_ELECTED_MASTER_NODE;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Defaults.RCA_CONF_STANDBY_MASTER_NODE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.Cluster;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaConfAnnotation;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaItClass;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaItMethod;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.TestClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;

@RcaItClass(clusterType = TestClusterType.MULTI_NODE_DEDICATED_MASTER)
@RcaGraph(
    analysisGraphClass = SimpleGraph.AnalysisGraphX.class,
    rcaConfElectedMaster = @RcaConfAnnotation(file = RCA_CONF_ELECTED_MASTER_NODE),
    rcaConfStadByMaster = @RcaConfAnnotation(file = RCA_CONF_STANDBY_MASTER_NODE),
    rcaConfDataNode = @RcaConfAnnotation(file = RCA_CONF_DATA_NODE)
)
public class SimpleGraph {
  Cluster cluster;

  @RcaItMethod
  public void simple() {
    System.out.println("I am a simple method");
  }

  public void anotherMethod() {
    System.out.println("I am another method");

  }

  public static class AnalysisGraphX extends ElasticSearchAnalysisGraph {
    @Override
    public void construct() {
      super.constructResourceHeatMapGraph();
    }
  }

  public void setCluster(Cluster cluster) {
    this.cluster = cluster;
  }
}
