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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import java.util.ArrayList;
import java.util.List;

/**
 * POJO for config overrides. The class contains two sets of overrides, one for enabling and one
 * for disabling.
 */
public class ConfigOverrides {
    private Overrides enable;
    private Overrides disable;

    public ConfigOverrides() {
        this.enable = new Overrides();
        this.disable = new Overrides();
    }

    public Overrides getEnable() {
        return enable;
    }

    public void setEnable(Overrides enable) {
        this.enable = enable;
    }

    public Overrides getDisable() {
        return disable;
    }

    public void setDisable(Overrides disable) {
        this.disable = disable;
    }

    /**
     * Class containing the overridable attributes of the system. Currently, overriding the
     * enabled/disabled state for RCAs, deciders, actions and collectors are supported. More attributes can
     * be added as needed.
     */
    public static class Overrides {
        private List<String> rcas;
        private List<String> deciders;
        private List<String> actions;
        private List<String> collectors;

        public Overrides() {
            this.rcas = new ArrayList<>();
            this.deciders = new ArrayList<>();
            this.actions = new ArrayList<>();
            this.collectors = new ArrayList<>();
        }

        public List<String> getRcas() {
            return rcas;
        }

        public void setRcas(List<String> rcas) {
            this.rcas = rcas;
        }

        public List<String> getDeciders() {
            return deciders;
        }

        public void setDeciders(List<String> deciders) {
            this.deciders = deciders;
        }

        public List<String> getActions() {
            return actions;
        }

        public void setActions(List<String> actions) {
            this.actions = actions;
        }

        public List<String> getCollectors() {
            return collectors;
        }

        public void setCollectors(List<String> collectors) {
            this.collectors = collectors;
        }
    }
}
