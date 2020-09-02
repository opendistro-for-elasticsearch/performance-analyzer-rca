/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.any;
import static org.mockito.ArgumentMatchers.isA;
import static org.mockito.Mockito.times;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Publisher;

import java.util.ArrayList;
import java.util.List;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterRcaPublisher;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummaryListener;
import org.junit.Test;
import org.mockito.Mockito;

public class PluginControllerTest {

  @Test
  public void testInit() {
    List<Class<? extends Plugin>> frameworkPlugins = new ArrayList<Class<? extends Plugin>>() {{
      add(TestActionListener.class);
      add(TestSummaryListener.class);
      add(TestPlugin.class);
    }};
    PluginControllerConfig pluginControllerConfig = Mockito.mock(PluginControllerConfig.class);
    Mockito.when(pluginControllerConfig.getFrameworkPlugins()).thenReturn(frameworkPlugins);
    Publisher publisher = Mockito.mock(Publisher.class);
    ClusterRcaPublisher summaryPublisher = Mockito.mock(ClusterRcaPublisher.class);

    PluginController pluginController = new PluginController(pluginControllerConfig, publisher, summaryPublisher);
    pluginController.initPlugins();

    List<Plugin> plugins = pluginController.getPlugins();
    assertEquals(3, plugins.size());

    // Only action listeners registered with publisher
    Mockito.verify(publisher, times(1)).addActionListener(any());
    Mockito.verify(publisher, times(1)).addActionListener(isA(TestActionListener.class));
    Mockito.verify(summaryPublisher, times(1)).addClusterSummaryListener(any());
    Mockito.verify(summaryPublisher, times(1)).addClusterSummaryListener(isA(TestSummaryListener.class));
  }

  @Test(expected = IllegalStateException.class)
  public void testPrivateConstructorPlugin() {
    List<Class<? extends Plugin>> frameworkPlugins = new ArrayList<Class<? extends Plugin>>() {{
      add(TestPrivateConstructorPlugin.class);
    }};
    PluginControllerConfig pluginControllerConfig = Mockito.mock(PluginControllerConfig.class);
    Mockito.when(pluginControllerConfig.getFrameworkPlugins()).thenReturn(frameworkPlugins);
    Publisher publisher = Mockito.mock(Publisher.class);
    PluginController pluginController = new PluginController(pluginControllerConfig, publisher, null);
    pluginController.initPlugins();
  }

  @Test(expected = IllegalStateException.class)
  public void testMultiConstructorPlugin() {
    List<Class<? extends Plugin>> frameworkPlugins = new ArrayList<Class<? extends Plugin>>() {{
      add(TestMultiConstructorPlugin.class);
    }};
    PluginControllerConfig pluginControllerConfig = Mockito.mock(PluginControllerConfig.class);
    Mockito.when(pluginControllerConfig.getFrameworkPlugins()).thenReturn(frameworkPlugins);
    Publisher publisher = Mockito.mock(Publisher.class);
    PluginController pluginController = new PluginController(pluginControllerConfig, publisher, null);
    pluginController.initPlugins();
  }

  @Test(expected = IllegalStateException.class)
  public void testNonDefaultConstructorPlugin() {
    List<Class<? extends Plugin>> frameworkPlugins = new ArrayList<Class<? extends Plugin>>() {{
      add(TestNonDefaultConstructorPlugin.class);
    }};
    PluginControllerConfig pluginControllerConfig = Mockito.mock(PluginControllerConfig.class);
    Mockito.when(pluginControllerConfig.getFrameworkPlugins()).thenReturn(frameworkPlugins);
    Publisher publisher = Mockito.mock(Publisher.class);
    PluginController pluginController = new PluginController(pluginControllerConfig, publisher, null);
    pluginController.initPlugins();
  }

  public static class TestActionListener extends Plugin implements ActionListener {

    @Override
    public void actionPublished(Action action) {
      assert true;
    }

    @Override
    public String name() {
      return "Test_Action_Listener";
    }
  }

  public static class TestSummaryListener extends Plugin implements ClusterSummaryListener {

    @Override
    public void summaryPublished(ClusterSummary clusterSummary) { assert true; }

    @Override
    public String name() {
      return "Test_Action_Listener";
    }
  }

  public static class TestPlugin extends Plugin {

    @Override
    public String name() {
      return "Test_Plugin";
    }
  }

  public static class TestPrivateConstructorPlugin extends Plugin {

    private TestPrivateConstructorPlugin() {
      assert true;
    }

    @Override
    public String name() {
      return "Test_Private_Constructor_Plugin";
    }
  }

  public static class TestMultiConstructorPlugin extends Plugin {

    private String name;

    public TestMultiConstructorPlugin() {
      this.name = "Test_Multi_Constructor_Plugin";
    }

    public TestMultiConstructorPlugin(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }
  }

  public static class TestNonDefaultConstructorPlugin extends Plugin {

    private String name;

    public TestNonDefaultConstructorPlugin(String name) {
      this.name = name;
    }

    @Override
    public String name() {
      return name;
    }
  }
}
