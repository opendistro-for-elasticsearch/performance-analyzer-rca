package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericContext;

public class SymptomContext extends GenericContext {
    public enum State {
        HEALTHY("healthy"), UNHEALTHY("unhealthy"), CONTENDED("contended"), UNKNOWN("unknown");
        private String stateName;
        private State(String stateName){
            this.stateName = stateName;
        }

        @Override
        public String toString(){
            return this.stateName;
        }
    }

    private State state;

    public SymptomContext(State state){
        this.state = state;
    }

    public State getState() {
        return this.state;
    }
    public void setState(State state) {
        this.state = state;
    }
    public boolean isUnhealthy() {
        return this.state == State.UNHEALTHY || this.state == State.CONTENDED;
    }
    public static SymptomContext generic() {
        return new SymptomContext(State.UNKNOWN);
    }

    @Override
    public String toString() {
        return this.state.toString();
    }
}