package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.List;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;

public abstract class GenericFlowUnit {
    private long timeStamp;
    private boolean empty = true;
    private List<List<String>> data = null;

    // Creates an empty flow unit.
    public GenericFlowUnit(long timeStamp) {
        this.timeStamp = timeStamp;
    }
    public GenericFlowUnit(long timeStamp, List<List<String>> data) {
        this(timeStamp);
        this.data = data;
        this.empty = false;
    }

    public long getTimeStamp() {
        return timeStamp;
    }

    public boolean isEmpty() {
        return empty;
    }
    public List<List<String>> getData() {
        return data;
    }

    public abstract FlowUnitMessage buildFlowUnitMessage(final String graphNode, final String esNode);
}