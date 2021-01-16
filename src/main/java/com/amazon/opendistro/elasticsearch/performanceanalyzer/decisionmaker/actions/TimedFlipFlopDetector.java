/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

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
     * Tests if (prev, curr) is a flip flopping sequence of impacts.
     *
     * <p>Only an increase following a decrease is considered a flip flop. Therefore, if
     * prev decreases pressure and curr increases pressure, then (prev, curr) is a flip flop.
     *
     * @param prev The {@link Impact} that curr is compared against
     * @param curr The {@link Impact} that you'd like to test and apply
     * @return Whether or not (prev, curr) is a flip flopping sequence of impacts
     */
    protected boolean isFlipFlopImpact(Impact prev, Impact curr) {
        return prev.equals(Impact.DECREASES_PRESSURE) && curr.equals(Impact.INCREASES_PRESSURE);
    }

    /**
     * Returns true if the impact for any given Dimension in prev is a flip flop Impact when compared to
     * the impact for a given dimension in prev
     *
     * <p>e.g. for prev = (HEAP: INCREASE, CPU: DECREASE), curr = (HEAP: DECREASE, CPU: INCREASE)
     * (prev, curr) is a flip flop vector because a CPU: DECREASE followed by a CPU: INCREASE is a flip
     * flop impact. Note that (HEAP: DECREASE) followed by (CPU: INCREASE) is not a flip flop
     * because HEAP =/= CPU.
     *
     * @param prev The first {@link ImpactVector}. Its Impacts appear on the LHS of calls to
     *             {@link this#isFlipFlopImpact(Impact, Impact)}
     * @param curr The second {@link ImpactVector}. Its Impacts appear on the RHS of calls to
     *            {@link this#isFlipFlopImpact(Impact, Impact)}.
     * @return true if the impact for any given Dimension in curr is a flip flop Impact when compared to
     *      the impact for a given dimension in prev
     */
    protected boolean isFlipFlopVector(ImpactVector prev, ImpactVector curr) {
        Map<Dimension, Impact> currentImpact = curr.getImpact();
        for (Map.Entry<Dimension, Impact> impactEntry : prev.getImpact().entrySet()) {
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
