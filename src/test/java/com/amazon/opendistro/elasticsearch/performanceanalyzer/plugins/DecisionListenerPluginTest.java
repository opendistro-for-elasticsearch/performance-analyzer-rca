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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class DecisionListenerPluginTest {
    private static DecisionListenerPlugin plugin;

    @Before
    public void setup (){
        plugin = new DecisionListenerPlugin();
        Assert.assertNull(plugin.getPluginConfig());
        Assert.assertNull(plugin.getKafkaProducer());
    }

    @Test
    public void testInit(){
        String pluginConfPath = Paths.get(ConfConsts.CONFIG_DIR_TEST_PATH, ConfConsts.PLUGINS_CONF_TEST_FILENAMES).toString();
        PluginConfig pluginConfig = new PluginConfig(pluginConfPath);
        Assert.assertEquals("localhost:9092", pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_BOOTSTRAP_SERVER_KEY));
        Assert.assertEquals("test", pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_TOPIC_KEY));
        Assert.assertEquals("test_url", pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.WEBHOOKS_URL_KEY));
    }

    @Test
    public void testAction(){
        Action action = Mockito.mock(Action.class);
        DecisionListenerPlugin plugin = Mockito.mock(DecisionListenerPlugin.class);
        plugin.actionPublished(action);
        Mockito.verify(plugin, times(1)).actionPublished(action);
    }

    @Test
    public void testHttpConnection() throws Exception {
        class UrlWrapper {
            URL url;
            public UrlWrapper(String spec) throws MalformedURLException {
                url = new URL(spec);
            }
            public HttpURLConnection openConnection() throws IOException {
                return (HttpURLConnection) url.openConnection();
            }
        }
        UrlWrapper u = PowerMockito.mock(UrlWrapper.class);
        String summary = "test";
        String urlStr = "http://www.example.com";
        PowerMockito.whenNew(URL.class).withArguments(urlStr).thenReturn(u.url);
        HttpURLConnection con = PowerMockito.mock(HttpURLConnection.class);
        PowerMockito.when(u.openConnection()).thenReturn(con);
        PowerMockito.when(con.getResponseCode()).thenReturn(200);
        Assert.assertTrue(DecisionListenerPlugin.sendHttpPostRequest(summary, urlStr));
    }

//    @Test
//    public void testActionPublished() throws Exception {
//        Action action = Mockito.mock(Action.class);
//        Mockito.when(action.summary()).thenReturn("testAction");
//        PluginConfig pluginConfig = Mockito.mock(PluginConfig.class);
//        KafkaProducer<String, String> kafkaProducer = Mockito.mock(KafkaProducer.class);
//        PowerMockito.whenNew(PluginConfig.class).withNoArguments().thenReturn(pluginConfig);
//        PowerMockito.whenNew(KafkaProducer.class).withNoArguments().thenReturn(kafkaProducer);
//        plugin.actionPublished(action);
//        Mockito.verify(plugin, times(1)).makeSingletonKafkaProducer();
//        Mockito.verify(plugin, times(1)).makeSingletonPluginConfig();
//
//    }
}

