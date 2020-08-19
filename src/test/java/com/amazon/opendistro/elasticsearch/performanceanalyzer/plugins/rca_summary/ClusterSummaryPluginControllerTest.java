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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.rca_summary;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins.Plugin;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.rca_publisher.ClusterSummaryListener;
import org.junit.Test;

import static org.junit.Assert.*;

public class ClusterSummaryPluginControllerTest {

    @Test
    public void testInit(){
        assert true;
        //TODO: test controller config load up
        //TODO: test config Init
        //TODO: Test belongs to generic Summary
        //TODO: Test invoke times
    }

    public static class TestSummaryListener <T extends GenericSummary> extends Plugin implements ClusterSummaryListener<T> {
        @Override
        public String name() {
            return "Test_Summary_Listener";
        }

        @Override
        public void summaryPublished(ClusterSummary<T> clusterSummary) {
            assert true;
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