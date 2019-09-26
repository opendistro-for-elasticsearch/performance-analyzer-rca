package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.AnalysisGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Stats;
import java.lang.reflect.InvocationTargetException;
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
    graph.construct();
    graph.validateAndProcess();
    return Stats.getInstance().getConnectedComponents();
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

  public static boolean doTagsMatch(Node node, RcaConf conf) {
    Map<String, String> rcaTagMap = conf.getTagMap();
    for (Map.Entry tag : node.getTags().entrySet()) {
      String rcaConfTagvalue = rcaTagMap.get(tag.getKey());
      if (!tag.getValue().equals(rcaConfTagvalue)) {
        return false;
      }
    }
    return true;
  }
}
