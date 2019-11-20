package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.contexts.ResourceContext;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.collect.ImmutableList;

import java.util.ArrayDeque;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Collections;
import java.util.concurrent.ConcurrentMap;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;


/**
 * This RCA runs on the elected master only and it subscribes all high heap RCA from data nodes within the entire cluster.
 * This can help to reduce the network bandwidth/workload on master node and push computation related workload on data node itself.
 * The RCA uses a cache to keep track of the last three metrics from each node and will mark the node as unhealthy
 * if the last three consecutive flowunits are unhealthy. And if any node is unthleath, the entire cluster will
 * be considered as unhealthy and send out corresponding flowunits to downstream nodes.
 */

public class HighHeapUsageClusterRca extends Rca {
    private static final Logger LOG = LogManager.getLogger(HighHeapUsageRca.class);
    private static final int RCA_PERIOD = 12;
    private static final int UNHEALTHY_FLOWUNIT_THRESHOLD = 3;
    private static final int CACHE_EXPIRATION_TIMEOUT = 10;
    protected int counter;
    private final Rca highHeapUsageRca;
    private final LoadingCache<String, ImmutableList<ResourceContext.State>> nodeStateCache;

    public <R extends Rca> HighHeapUsageClusterRca(long evaluationIntervalSeconds, final R highHeapUsageRca) {
        super(evaluationIntervalSeconds);
        this.highHeapUsageRca = highHeapUsageRca;
        counter = 0;
        nodeStateCache = CacheBuilder.newBuilder()
                .maximumSize(1000)
                .expireAfterWrite(CACHE_EXPIRATION_TIMEOUT, TimeUnit.MINUTES)
                .build(
                        new CacheLoader<String, ImmutableList<ResourceContext.State>>() {
                            public ImmutableList<ResourceContext.State> load(String key)  {
                                return ImmutableList.copyOf(new ArrayList<>());
                            }
                        });
    }

    private List<String> getUnhealthyNodeList() {
        List<String> unhealthyNodeList = new ArrayList<>();
        ConcurrentMap<String, ImmutableList<ResourceContext.State>> currentMap = this.nodeStateCache.asMap();
        for (String nodeId : RcaUtil.fetchCurrentDataNodeIdList()) {
            ImmutableList<ResourceContext.State> nodeStateList = currentMap.get(nodeId);
            if (nodeStateList == null) {
                unhealthyNodeList.add(nodeId);
            }
            else {
                int unhealthyNodeCnt = 0;
                for (ResourceContext.State state : nodeStateList) {
                    if (state == ResourceContext.State.UNHEALTHY) {
                        unhealthyNodeCnt++;
                    }
                }
                if (unhealthyNodeCnt >= UNHEALTHY_FLOWUNIT_THRESHOLD) {
                    unhealthyNodeList.add(nodeId);
                }
            }
        }
        return unhealthyNodeList;
    }

    private synchronized void readComputeWrite (String nodeId, ResourceContext.State state) throws ExecutionException {
        ArrayDeque<ResourceContext.State> nodeStateDeque = new ArrayDeque<>(this.nodeStateCache.get(nodeId));
        nodeStateDeque.addFirst(state);
        if (nodeStateDeque.size() > UNHEALTHY_FLOWUNIT_THRESHOLD) {
            nodeStateDeque.removeLast();
        }
        this.nodeStateCache.put(nodeId, ImmutableList.copyOf(nodeStateDeque));
    }

    @Override
    public ResourceFlowUnit operate() {
        List<ResourceFlowUnit> highHeapUsageRcaFlowUnits = highHeapUsageRca.fetchFlowUnitList();
        counter += 1;
        for (ResourceFlowUnit highHeapUsageRcaFlowUnit : highHeapUsageRcaFlowUnits) {
            //TODO: flowunit.isEmpty() is set only when the flowunit is empty. unknown state should be allowed
            if (highHeapUsageRcaFlowUnit.isEmpty() || highHeapUsageRcaFlowUnit.getResourceContext().isUnknown()) { continue; }
            List<List<String>> highHeapUsageRcaData = highHeapUsageRcaFlowUnit.getData();
            if(!highHeapUsageRcaFlowUnits.isEmpty() && !highHeapUsageRcaData.isEmpty()) {
                //TODO: List<List<>> needs to be changed
                String nodeId = highHeapUsageRcaData.get(1).get(0);
                try {
                    readComputeWrite(nodeId, highHeapUsageRcaFlowUnit.getResourceContext().getState());
                }
                catch (ExecutionException e) {
                    LOG.debug("ExecutionException occurs when retrieving key {}", nodeId);
                }
            }
        }
        if (counter == RCA_PERIOD) {
            List<List<String>> ret = new ArrayList<>();
            List<String> unhealthyNodeList = getUnhealthyNodeList();
            counter = 0;
            LOG.debug("Unhealthy node id list : {}", unhealthyNodeList);
            if (unhealthyNodeList.size() > 0) {
                String row = unhealthyNodeList
                        .stream()
                        .collect(Collectors.joining(" "));
                ret.addAll(Arrays.asList(Collections.singletonList("Unhealthy node(s)"), Collections.singletonList(row)));
                return new ResourceFlowUnit(System.currentTimeMillis(), ret,
                        new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.UNHEALTHY));
            }
            else {
                ret.addAll(Arrays.asList(Collections.singletonList("Unhealthy node(s)"), Collections.singletonList("All nodes are healthy")));
                return new ResourceFlowUnit(System.currentTimeMillis(), ret,
                        new ResourceContext(ResourceContext.Resource.HEAP, ResourceContext.State.HEALTHY));
            }
        } else {
            // we return an empty FlowUnit RCA for now. Can change to healthy (or previous known RCA state)
            LOG.debug("Empty FlowUnit returned for {}", this.getClass().getName());
            return new ResourceFlowUnit(System.currentTimeMillis(), ResourceContext.generic());
        }
    }
}
