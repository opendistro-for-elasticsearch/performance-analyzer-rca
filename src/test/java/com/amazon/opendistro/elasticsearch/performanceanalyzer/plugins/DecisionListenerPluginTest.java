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
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decision;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.ConfConsts;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.config.PluginConfig;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.Mockito;
import org.mockito.junit.MockitoRule;
import org.powermock.api.mockito.PowerMockito;


import java.io.IOException;
import java.net.HttpURLConnection;
import java.net.MalformedURLException;
import java.net.URL;
import java.nio.file.Paths;

import static org.mockito.Mockito.times;
import static org.mockito.Mockito.when;


public class DecisionListenerPluginTest {
    static PluginConfig pluginConfig;
    static DecisionListenerPlugin plugin;

    @Test
    public void testSetup (){
        plugin = new DecisionListenerPlugin();
        Assert.assertNull(plugin.getPluginConfig());
        Assert.assertNull(plugin.getKafkaProducer());

        String pluginConfPath = Paths.get(ConfConsts.CONFIG_DIR_TEST_PATH, ConfConsts.PLUGINS_CONF_TEST_FILENAMES).toString();
        pluginConfig = new PluginConfig(pluginConfPath);
        Assert.assertEquals("localhost:9092", pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_BOOTSTRAP_SERVER_KEY));
        Assert.assertEquals("test", pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.KAFKA_TOPIC_KEY));
        Assert.assertEquals("http://www.example.com", pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.WEBHOOKS_URL_KEY));
    }

    @Test
    public void testInit() {
        DecisionListenerPlugin plugin = new DecisionListenerPlugin();
        plugin.initialize();
        Assert.assertNotNull(plugin.getKafkaProducer());
        Assert.assertNotNull(plugin.getPluginConfig());
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
        String urlStr = pluginConfig.getKafkaDecisionListenerConfig(ConfConsts.WEBHOOKS_URL_KEY);
        PowerMockito.whenNew(URL.class).withArguments(urlStr).thenReturn(u.url);
        HttpURLConnection con = PowerMockito.mock(HttpURLConnection.class);
        PowerMockito.when(u.openConnection()).thenReturn(con);
        PowerMockito.when(con.getResponseCode()).thenReturn(200);
        Assert.assertTrue(DecisionListenerPlugin.sendHttpPostRequest(summary, urlStr));
    }
}
