package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import java.util.Collections;
import java.util.List;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Gatherable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Operable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;

public class GraphNodeOperations {
    private static final Logger LOG = LogManager.getLogger(GraphNodeOperations.class);

    static void readFromLocal(FlowUnitOperationArgWrapper args) {
        args.getNode().generateFlowUnitListFromLocal(args);
    }

    // This is the abstraction for when the data arrives on the wire from a remote dependency.
    static void readFromWire(FlowUnitOperationArgWrapper args) {
        //flowUnits.forEach(i -> LOG.info("rca: Read from wire: {}", i));
        args.getNode().generateFlowUnitListFromWire(args);
    }
}
