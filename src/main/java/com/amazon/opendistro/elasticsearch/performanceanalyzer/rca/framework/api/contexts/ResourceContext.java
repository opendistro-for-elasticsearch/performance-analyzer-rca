package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Resources;

public class ResourceContext extends GenericContext {
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

    public enum Resource {
        CPU("CPU"), MEM("Memory"), JVM("JVM"), DISK("disk"), CACHE("CACHE"), NIC("NIC"), HEAP("heap"), SHARD("shard"), UNKNOWN("unknown");
        private String resourceName;
        private Resource(String resourceName){
            this.resourceName = resourceName;
        }

        @Override
        public String toString(){
            return this.resourceName;
        }
    }

    private State state;
    private Resource resource;

    public ResourceContext(Resource resource, State state){
        this.resource = resource;
        this.state = state;
    }

    public State getState() {
        return this.state;
    }
    public void setState(State state) {
        this.state = state;
    }
    public Resource getResource() {
        return this.resource;
    }
    public void setResource(Resource resource) {
        this.resource = resource;
    }
    public boolean isUnhealthy() {
        return this.state == State.UNHEALTHY || this.state == State.CONTENDED;
    }
    public boolean isUnknown() { return this.state == State.UNKNOWN; }

    public static ResourceContext generic() {
        return new ResourceContext(Resource.UNKNOWN, State.UNKNOWN);
    }

    public FlowUnitMessage.ResourceContextMessage buildContextMessage() {
        final FlowUnitMessage.ResourceContextMessage.Builder contextMessageBuilder = FlowUnitMessage.ResourceContextMessage.newBuilder();
        contextMessageBuilder.setState(state.ordinal());
        contextMessageBuilder.setResource(resource.ordinal());
        return contextMessageBuilder.build();
    }

    public static ResourceContext buildResourceContextFromMessage(FlowUnitMessage.ResourceContextMessage message) {
        return new ResourceContext(Resource.values()[message.getResource()], State.values()[message.getState()]);
    }

    @Override
    public String toString() {
        return this.resource + " " + this.state;
    }
}