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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaConsts.RcaTagConstants;
import java.lang.reflect.InvocationTargetException;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RcaUtil {

  private static final Logger LOG = LogManager.getLogger(RcaUtil.class);

  private static AnalysisGraph getAnalysisGraphImplementor(RcaConf rcaConf)
      throws ClassNotFoundException, NoSuchMethodException, IllegalAccessException,
      InvocationTargetException, InstantiationException {
    return (AnalysisGraph)
        Class.forName(rcaConf.getAnalysisGraphEntryPoint()).getDeclaredConstructor().newInstance();
  }

  public static List<ConnectedComponent> getAnalysisGraphComponents(RcaConf rcaConf)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
      InstantiationException, IllegalAccessException {
    AnalysisGraph graph = getAnalysisGraphImplementor(rcaConf);
    return getAnalysisGraphComponents(graph);
  }

  public static List<ConnectedComponent> getAnalysisGraphComponents(String analysisGraphClass)
      throws ClassNotFoundException, NoSuchMethodException, InvocationTargetException,
      InstantiationException, IllegalAccessException {
    AnalysisGraph graph =
        (AnalysisGraph) Class.forName(analysisGraphClass).getDeclaredConstructor().newInstance();
    graph.construct();
    graph.validateAndProcess();
    return Stats.getInstance().getConnectedComponents();
  }

  public static List<ConnectedComponent> getAnalysisGraphComponents(AnalysisGraph graph) {
    graph.construct();
    graph.validateAndProcess();
    return Stats.getInstance().getConnectedComponents();
  }

  public static boolean doTagsMatch(Node<?> node, RcaConf conf) {
    Map<String, String> rcaTagMap = conf.getTagMap();
    for (Map.Entry<String, String> tag : node.getTags().entrySet()) {
      String rcaConfTagvalue = rcaTagMap.get(tag.getKey());
      return tag.getValue() != null
          && Arrays.asList(tag.getValue().split(",")).contains(rcaConfTagvalue);
    }
    return true;
  }

  public static boolean shouldExecuteLocally(Node<?> node, RcaConf conf) {
    final Map<String, String> confTagMap = conf.getTagMap();
    final Map<String, String> nodeTagMap = node.getTags();

    if (confTagMap != null && nodeTagMap != null) {
      final String hostLocus = confTagMap.get(RcaTagConstants.TAG_LOCUS);
      final String nodeLoci = nodeTagMap.get(RcaTagConstants.TAG_LOCUS);
      if (nodeLoci != null && !nodeLoci.isEmpty()) {
        List<String> nodeLociStrings = Arrays.asList(nodeLoci.split(RcaTagConstants.SEPARATOR));
        return nodeLociStrings.contains(hostLocus);
      }
    }

    // By default, if no tags are specified, execute the nodes locally.
    return true;
  }
}
