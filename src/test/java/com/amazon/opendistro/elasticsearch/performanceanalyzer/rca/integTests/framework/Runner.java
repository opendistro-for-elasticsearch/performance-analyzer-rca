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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaController;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.RcaControllerHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaConfAnnotation;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaItClass;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.RcaItMethod;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.TestClusterType;
import com.google.common.reflect.ClassPath;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Method;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.sql.Timestamp;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.junit.Assert;
import org.junit.Test;

/**
 * This runs all the integration tests <b>serially</b>.
 * This only considers classes annotated with {@code RcaItClass} and then for each such class, it runs all the methods
 * annotated with {@code RcaItMethod}. Before running the method, it runs a special method named {@code setUp}, if one
 * exists. And after the completion of the method, it runs another special method named {@code tearDown}, if one exists.
 */
public class Runner {
  public static final String INTEG_TEST_PACKAGE = "com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests";
  public static final String SETUP_METHOD_NAME = "setUp";
  public static final String TEAR_DOWN_METHOD_NAME = "tearDown";
  public static final String SET_CLUSTER_METHOD = "setCluster";

  private static final String IT_DIR = "/tmp/rcaIt";

  private static final Logger LOG = LogManager.getLogger(Runner.class);

  private Map<TestClusterType,
      // The second order map is for http vs https clusters. true means use https.
      Map<Boolean, List<Class>>> testClassesForClusterTypes;

  private String DATA_NODE_RCA_CONF;
  private String STANDBY_MASTER_NODE_RCA_CONF;
  private String ELECTED_MASTER_RCA_CONF;

  @Test
  public void runner() throws Exception {
    testClassesForClusterTypes = new HashMap<>();
    for (TestClusterType type : TestClusterType.values()) {
      Map<Boolean, List<Class>> securitySetting = new HashMap<>();
      securitySetting.put(true, new ArrayList<>());
      securitySetting.put(false, new ArrayList<>());

      testClassesForClusterTypes.put(type, securitySetting);
    }

    //
    SimpleDateFormat sdf = new SimpleDateFormat("yyyy.MM.dd.HH.mm.ss");
    Timestamp timestamp = new Timestamp(System.currentTimeMillis());
    String formattedTime = sdf.format(timestamp);
    Path rcaItDir = Paths.get(IT_DIR, formattedTime);

    collectClassesByClusterType(INTEG_TEST_PACKAGE);
    for (Map.Entry<TestClusterType, Map<Boolean, List<Class>>> entry: testClassesForClusterTypes.entrySet()) {
      TestClusterType clusterType = entry.getKey();
      Map<Boolean, List<Class>> securityDetailsMap = entry.getValue();

      for (Map.Entry<Boolean, List<Class>> entry2: securityDetailsMap.entrySet()) {
        if (!entry2.getValue().isEmpty()) {

          File clusterDir = rcaItDir.toFile();
          File parent = clusterDir.getParentFile();
          if (!clusterDir.exists() && !clusterDir.mkdirs()) {
            throw new IllegalStateException("Couldn't create dir: " + parent);
          }

          boolean httpsEnabled = entry2.getKey();
          boolean rcaEnabled = true;
          Cluster cluster = new Cluster(clusterType, httpsEnabled, rcaEnabled, clusterDir);
          for (Class cls : entry2.getValue()) {
            executeAllIntegrationTestMethods(cls, cluster);
          }
          cluster.deleteCluster();
        }
      }
    }
  }

  private void collectClassesByClusterType(String packageName) throws IOException {
    ClassLoader cl = ClassLoader.getSystemClassLoader();
    ClassPath cp = ClassPath.from(cl);

    Set<ClassPath.ClassInfo> classes = cp.getTopLevelClassesRecursive(packageName);

    for (ClassPath.ClassInfo info : classes) {
      Class cls = info.load();
      if (cls.isAnnotationPresent(RcaItClass.class)) {
        RcaItClass annot = (RcaItClass) cls.getAnnotation(RcaItClass.class);
        TestClusterType clusterType = annot.clusterType();
        boolean httpsEnabled = annot.useHttps();

        testClassesForClusterTypes.get(clusterType).get(httpsEnabled).add(cls);
      }
    }
  }

  private Method getAndMakeAccessibleIfMethodExists(Class cls, String methodName) {
    try {
      Method method = cls.getMethod(methodName);
      method.setAccessible(true);
      return method;
    } catch (NoSuchMethodException exception) {
      return null;
    }
  }

  private RcaConf getRcaConf(RcaConfAnnotation rcaConfAnnotation) {
    RcaConf rcaConf = null;
    File rcaConfFile = Paths.get(rcaConfAnnotation.file()).toFile();
    if (rcaConfFile.exists()) {
      rcaConf = new RcaConf(rcaConfFile.toString());
    }

    return rcaConf;
  }

  private List<ConnectedComponent> getRcaGraphFromAnnotation(RcaGraph rcaGraph) throws
      ClassNotFoundException,
      NoSuchMethodException,
      InstantiationException,
      IllegalAccessException,
      InvocationTargetException {
    RcaConf rcaConfElectedMaster = getRcaConf(rcaGraph.rcaConfElectedMaster());
    ELECTED_MASTER_RCA_CONF = rcaGraph.rcaConfElectedMaster().file();
    Objects.requireNonNull(rcaConfElectedMaster);

    RcaConf rcaConfStandByMaster = getRcaConf(rcaGraph.rcaConfStadByMaster());
    STANDBY_MASTER_NODE_RCA_CONF = rcaGraph.rcaConfStadByMaster().file();
    Objects.requireNonNull(rcaConfStandByMaster);

    RcaConf rcaConfDataNode = getRcaConf(rcaGraph.rcaConfDataNode());
    DATA_NODE_RCA_CONF = rcaGraph.rcaConfDataNode().file();
    Objects.requireNonNull(rcaConfDataNode);

    return RcaUtil.getAnalysisGraphComponents(rcaConfDataNode);
  }

  private void executeAllIntegrationTestMethods(final Class cls, final Cluster cluster) throws
      Exception {
    Object obj = cls.getDeclaredConstructor().newInstance();

    try {
      Method setClusterMethod = cls.getMethod(SET_CLUSTER_METHOD, Cluster.class);
      setClusterMethod.setAccessible(true);
      setClusterMethod.invoke(obj, cluster);
    } catch (NoSuchMethodException ex) {
      // This test class hasn't defined a method setCluster(Cluster). SO probably it does not need access to the cluster
      // object. Which is fine. We move on to the method execution.
    }

    Method setUp = getAndMakeAccessibleIfMethodExists(cls, SETUP_METHOD_NAME);
    Method tearDown = getAndMakeAccessibleIfMethodExists(cls, TEAR_DOWN_METHOD_NAME);

    cluster.setUpCluster();
    List<ConnectedComponent> connectedComponents = null;
    if (cls.isAnnotationPresent(RcaGraph.class)) {
      RcaGraph rcaGraph = (RcaGraph) cls.getAnnotation(RcaGraph.class);
      connectedComponents = getRcaGraphFromAnnotation(rcaGraph);
      RcaControllerHelper.set(DATA_NODE_RCA_CONF, STANDBY_MASTER_NODE_RCA_CONF, ELECTED_MASTER_RCA_CONF);
      cluster.updateGraph(connectedComponents);
      cluster.startClusterThreads();
    } else {
      Assert.fail("RcaGraph annotation needs to be specified.");
    }

    for (Method method : cls.getDeclaredMethods()) {
      if (method.isAnnotationPresent(RcaItMethod.class)) {
        method.setAccessible(true);
        cluster.unPauseCluster();
        executeItMethod(obj, method, setUp, tearDown, connectedComponents, cluster);
        cluster.pauseCluster();
      }
    }
  }

  private void executeItMethod(Object obj,
                               Method method,
                               Method setUp,
                               Method tearDown,
                               List<ConnectedComponent> connectedComponents,
                               final Cluster cluster) throws
      Exception {
    if (method.isAnnotationPresent(RcaGraph.class)) {
      RcaGraph rcaGraph = (RcaGraph) method.getAnnotation(RcaGraph.class);
      connectedComponents = getRcaGraphFromAnnotation(rcaGraph);
      RcaControllerHelper.set(DATA_NODE_RCA_CONF, STANDBY_MASTER_NODE_RCA_CONF, ELECTED_MASTER_RCA_CONF);
      cluster.pauseCluster();
      cluster.updateGraph(connectedComponents);
      cluster.unPauseCluster();
    }

    if (setUp != null) {
      setUp.invoke(obj);
    }

    try {
      method.invoke(obj);
    } catch (Exception ex) {
      StringBuilder sb = new StringBuilder();
      sb
          .append("Method '")
          .append(method.getName())
          .append("' in class '")
          .append(obj.getClass().getSimpleName())
          .append("' threw an exception.");

      LOG.error(sb.toString(), ex);
    }

    if (tearDown != null) {
      tearDown.invoke(obj);
    }
  }
}
