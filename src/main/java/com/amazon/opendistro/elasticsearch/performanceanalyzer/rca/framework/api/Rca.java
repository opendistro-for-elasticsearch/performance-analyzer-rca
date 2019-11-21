package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import java.util.List;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;

public abstract class Rca extends NonLeafNode {
    private static final Logger LOG = LogManager.getLogger(Rca.class);
    protected List<ResourceFlowUnit> flowUnitList;

    public Rca(long evaluationIntervalSeconds) {
        super(0, evaluationIntervalSeconds);
    }

    public  List<ResourceFlowUnit> fetchFlowUnitList() {
        return this.flowUnitList;
    }

    public void setGernericFlowUnitList() {
        this.flowUnitList = Collections.singletonList(ResourceFlowUnit.generic());
    }

    public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
        LOG.debug("rca: Executing fromLocal: {}", this.getClass().getSimpleName());
        this.flowUnitList = Collections.singletonList(this.operate());
        for (final ResourceFlowUnit flowUnit : flowUnitList) {
            if (!flowUnit.isEmpty() && flowUnit.getData() != null && !flowUnit.getData().isEmpty()) {
                    args.getPersistable().write(this, flowUnit);
            }
        }
    }

    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        final List<FlowUnitWrapper> flowUnitWrappers = args.getWireHopper()
                .readFromWire(args.getNode());
        flowUnitList = new ArrayList<>();
        LOG.debug("rca: Executing fromWire: {}", this.getClass().getSimpleName());
        for(FlowUnitWrapper messageWrapper : flowUnitWrappers) {
            flowUnitList.add(ResourceFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
        }
    }
}
