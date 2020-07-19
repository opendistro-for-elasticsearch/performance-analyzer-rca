package com.amazon.opendistro.elasticsearch.performanceanalyzer.config.overrides;

import java.util.ArrayList;
import java.util.List;

/**
 * POJO for config overrides.
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

    public static class Overrides {
        private List<String> rcas;
        private List<String> deciders;
        private List<String> actions;

        public Overrides() {
            this.rcas = new ArrayList<>();
            this.deciders = new ArrayList<>();
            this.actions = new ArrayList<>();
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
    }
}
