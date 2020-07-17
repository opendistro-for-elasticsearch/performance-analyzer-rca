package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.collections.TimeExpiringSet;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Dimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ImpactVector.Impact;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.TimeUnit;

/**
 * A {@link FlipFlopDetector} whose recorded actions expire after a given period of time.
 *
 * <p>This class defines a flip flop as an {@link Impact#DECREASES_PRESSURE}s followed by an
 * {@link Impact#INCREASES_PRESSURE}s to be a flip flops.
 *
 * <p>This class stores a {@link TimeExpiringSet} of {@link ImpactVector}s per {@link NodeKey}
 * that are used to determine these flip flops.
 */
public class TimedFlipFlopDetector implements FlipFlopDetector {
    private Map<NodeKey, TimeExpiringSet<ImpactVector>> flipFlopMap;
    private long expiryDuration;
    private TimeUnit expiryUnit;

    public TimedFlipFlopDetector(long duration, TimeUnit unit) {
        flipFlopMap = new HashMap<>();
        this.expiryDuration = duration;
        this.expiryUnit = unit;
    }

    /**
     * Tests if (a,b) is a flip flopping sequence of impacts.
     *
     * <p>Only an increase following a decrease is considered a flip flop
     *
     * @param a The first impact that would be applied
     * @param b The subsequent impact that would be applied
     * @return Whether or not (a,b) is a flip flopping sequence of impacts
     */
    protected boolean isFlipFlopImpact(Impact a, Impact b) {
        return a.equals(Impact.DECREASES_PRESSURE) && b.equals(Impact.INCREASES_PRESSURE);
    }

    /**
     * Returns true if the impact for a given Dimension in v is a flip flop Impact when compared to
     * the impact for a given dimension in u
     *
     * <p>e.g. for u = (HEAP: INCREASE, CPU: DECREASE), v = (HEAP: DECREASE, CPU: INCREASE)
     * (u,v) is a flip flop vector because a CPU: DECREASE followed by a CPU: INCREASE is a flip
     * flop impact
     *
     * @param u The first impact vector that would be applied
     * @param v The subsequent impact vector that would be applied
     * @return true if the impact for a given Dimension in v is a flip flop Impact when compared to
     *      the impact for a given dimension in u
     */
    protected boolean isFlipFlopVector(ImpactVector u, ImpactVector v) {
        Map<Dimension, Impact> currentImpact = v.getImpact();
        for (Map.Entry<Dimension, Impact> impactEntry : u.getImpact().entrySet()) {
            Dimension dim = impactEntry.getKey();
            Impact vImpact = currentImpact.get(dim);
            if (isFlipFlopImpact(impactEntry.getValue(), vImpact)) {
                return true;
            }
        }
        return false;
    }

    /**
     * Records an action's various {@link ImpactVector}s so that they may be used for future flip
     * flop tests
     *
     * @param action The action to record
     */
    @Override
    public void recordAction(Action action) {
        for (Map.Entry<NodeKey, ImpactVector> entry : action.impact().entrySet()) {
            flipFlopMap.compute(entry.getKey(), (k, v) -> {
                if (v == null) {
                    v = new TimeExpiringSet<>(expiryDuration, expiryUnit);
                }
                v.add(entry.getValue());
                return v;
            });
        }
    }

    /**
     * Returns true if for any NodeKey, ImpactVector pair (k, v) in action, v clashes with any of
     * the {@link ImpactVector}s currently associated with k.
     *
     * @param action The {@link Action} to test
     * @return true if applying the action would cause a flip flop
     */
    @Override
    public boolean isFlipFlop(Action action) {
        for (Map.Entry<NodeKey, ImpactVector> entry : action.impact().entrySet()) {
            TimeExpiringSet<ImpactVector> previousImpacts = flipFlopMap.get(entry.getKey());
            if (previousImpacts == null) {
                continue;
            }
            // Weakly-consistent iteration over the previousImpacts
            // If one of these impacts expires during our iteration we may incorrectly determine
            // action to be a flip flop until the subsequent call of this function. This is OK for
            // our use case since we're always erring on the side of stability
            for (ImpactVector impactVector : previousImpacts) {
                if (isFlipFlopVector(impactVector, entry.getValue())) {
                    return true;
                }
            }
        }
        return false;
    }
}
