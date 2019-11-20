package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.MetricFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.LeafNode;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.FlowUnitWrapper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler.FlowUnitOperationArgWrapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;

public abstract class Metric extends LeafNode {
    static final String[] metricList;
    static {
        AllMetrics.OSMetrics[] osMetrics = AllMetrics.OSMetrics.values();
        metricList = new String[osMetrics.length];
        for(int i=0; i < osMetrics.length; ++i) {
            metricList[i] = osMetrics[i].name();
        }
    }

    private String name;
    private static final Logger LOG = LogManager.getLogger(Metric.class);
    protected List<MetricFlowUnit> flowUnitList;

    public Metric(String name, long evaluationIntervalSeconds) {
        super(0, evaluationIntervalSeconds);
        this.name = name;
    }

    @Override
    public String name() {
        return name;
    }

    public MetricFlowUnit gather(Queryable queryable) {
        LOG.debug("Trying to gather metrics for {}", name);
        MetricsDB db;
        try {
            db = queryable.getMetricsDB();
        } catch (Exception e) {
            //TODO: Emit log/stats that gathering failed.
            LOG.error("RCA: Caught an exception while getting the DB {}", e.getMessage());
            return MetricFlowUnit.generic();
        }
        // LOG.debug("RCA: Metrics from MetricsDB {}", result);
        try {
            List<List<String>> result = queryable.queryMetrics(db, name);
            //return new FlowUnit(queryable.getDBTimestamp(db), result, Collections.emptyMap());
            return new MetricFlowUnit(queryable.getDBTimestamp(db), result);
        } catch (Exception e) {
            LOG.error("Metric exception: {}", e.getMessage());
            return MetricFlowUnit.generic();
        }
        // LOG.debug("RCA: Metrics from MetricsDB {}", result);
    }

    public void setGernericFlowUnitList() {
        this.flowUnitList = Collections.singletonList(MetricFlowUnit.generic());
    }

    public  List<MetricFlowUnit> fetchFlowUnitList() {
        return this.flowUnitList;
    }


    public void generateFlowUnitListFromLocal(FlowUnitOperationArgWrapper args) {
        this.flowUnitList = Collections.singletonList(gather(args.getQueryable()));
    }

    public void generateFlowUnitListFromWire(FlowUnitOperationArgWrapper args) {
        final List<FlowUnitWrapper> flowUnitWrappers = args.getWireHopper()
                .readFromWire(args.getNode());
        flowUnitList = new ArrayList<>();
        LOG.debug("rca: Executing fromWire: {}, received : {}", this.getClass().getSimpleName(), flowUnitWrappers.size());
        for(FlowUnitWrapper messageWrapper : flowUnitWrappers) {
            flowUnitList.add(MetricFlowUnit.buildFlowUnitFromWrapper(messageWrapper));
        }
    }
}
