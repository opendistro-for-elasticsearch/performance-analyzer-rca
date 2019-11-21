package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import java.util.List;
import java.util.Collections;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.NonLeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.SymptomFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;

public abstract class Symptom extends NonLeafNode {
    private static final Logger LOG = LogManager.getLogger(Symptom.class);
    protected List<SymptomFlowUnit> flowUnitList;

    public Symptom(long evaluationIntervalSeconds) {
        super(0, evaluationIntervalSeconds);
    }

    public  List<SymptomFlowUnit> fetchFlowUnitList() {
        return this.flowUnitList;
    }

    public void setGernericFlowUnitList() {
        this.flowUnitList = Collections.singletonList(SymptomFlowUnit.generic());
    }

    public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
        LOG.debug("rca: Executing handleRca: {}", this.getClass().getSimpleName());
        this.flowUnitList = Collections.singletonList(this.operate());
    }

    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        //TODO
    }
}
