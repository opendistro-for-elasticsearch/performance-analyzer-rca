package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.ThreadPoolType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Resource.CPU;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Resource.HEAP;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Resource.NETWORK;

public class QueueCapacity implements Action {

    public static final String NAME = "queue_capacity";
    public static final int COOL_OFF_PERIOD = 300;

    private int currentCapacity;
    private int desiredCapacity;
    private ThreadPoolType threadPoolType;
    private NodeKey esNode;

    private Map<ThreadPoolType, Integer> lowerBound = new HashMap<>();
    private Map<ThreadPoolType, Integer> upperBound = new HashMap<>();

    public QueueCapacity(NodeKey esNode, ThreadPoolType threadPool, int currentCapacity, boolean increase) {
        setBounds();
        int STEP_SIZE = 50;
        this.esNode = esNode;
        this.threadPoolType = threadPool;
        this.currentCapacity = currentCapacity;
        int desiredCapacity = increase ? currentCapacity + STEP_SIZE : currentCapacity - STEP_SIZE;
        setDesiredCapacity(desiredCapacity);
    }

    @Override
    public boolean isActionable() {
        return desiredCapacity != currentCapacity;
    }

    @Override
    public int coolOffPeriodInSeconds() {
        return COOL_OFF_PERIOD;
    }

    @Override
    public List<NodeKey> impactedNodes() {
        return Collections.singletonList(esNode);
    }

    @Override
    public Map<NodeKey, ImpactVector> impact() {
        ImpactVector impactVector = new ImpactVector();
        if (desiredCapacity > currentCapacity) {
            impactVector.increasesPressure(HEAP, CPU, NETWORK);
        } else if (desiredCapacity < currentCapacity) {
            impactVector.decreasesPressure(HEAP, CPU, NETWORK);
        }
        return Collections.singletonMap(esNode, impactVector);
    }

    @Override
    public void execute() {
        // Making this a no-op for now
        // TODO: Modify based on downstream agent API calls
        assert true;
    }

    private void setBounds() {
        // This is intentionally not made static because different nodes can
        // have different bounds based on instance types
        // TODO: Move configuration values to rca.conf

        // Write thread pool for bulk write requests
        this.lowerBound.put(ThreadPoolType.WRITE, 100);
        this.upperBound.put(ThreadPoolType.WRITE, 1000);

        // Search thread pool
        this.lowerBound.put(ThreadPoolType.SEARCH, 1000);
        this.upperBound.put(ThreadPoolType.SEARCH, 3000);
    }

    private void setDesiredCapacity(int desiredCapacity) {
        this.desiredCapacity = Math.min(desiredCapacity, upperBound.get(threadPoolType));
        this.desiredCapacity = Math.max(desiredCapacity, lowerBound.get(threadPoolType));
    }

    public int getCurrentCapacity() {
        return currentCapacity;
    }

    public int getDesiredCapacity() {
        return desiredCapacity;
    }

    public ThreadPoolType getThreadPoolType() {
        return threadPoolType;
    }
}
