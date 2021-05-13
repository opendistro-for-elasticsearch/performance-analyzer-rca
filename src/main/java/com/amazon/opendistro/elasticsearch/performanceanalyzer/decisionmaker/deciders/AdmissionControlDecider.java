/*
 * Copyright 2021 Amazon.com, Inc. or its affiliates. All Rights Reserved.
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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admission_control.AdmissionControlRca.REQUEST_SIZE;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.AdmissionControlAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.ResourceEnum;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotResourceSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admission_control.AdmissionControlClusterRca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.NodeKey;
import java.time.Instant;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Objects;

public class AdmissionControlDecider extends Decider {

    private int counter = 0;
    private static final String NAME = "AdmissionControl";
    private AdmissionControlClusterRca admissionControlClusterRca;

    public AdmissionControlDecider(
            long evalIntervalSeconds,
            int decisionFrequency,
            AdmissionControlClusterRca admissionControlClusterRca) {
        super(evalIntervalSeconds, decisionFrequency);
        this.admissionControlClusterRca = admissionControlClusterRca;
    }

    @Override
    public String name() {
        return NAME;
    }

    @Override
    public Decision operate() {
        Decision decision = new Decision(Instant.now().toEpochMilli(), NAME);
        counter += 1;
        if (counter < decisionFrequency) {
            return decision;
        }
        counter = 0;

        List<AdmissionControlAction> heapBasedActions = getHeapBasedActions();
        heapBasedActions.stream()
                .max(Comparator.comparingDouble(AdmissionControlAction::getDesiredValue))
                .ifPresent(decision::addAction);

        return decision;
    }

    private List<AdmissionControlAction> getHeapBasedActions() {
        List<AdmissionControlAction> heapBasedActions = new ArrayList<>();
        admissionControlClusterRca.getFlowUnits().stream()
                .filter(ResourceFlowUnit::hasResourceSummary)
                .flatMap(clusterRcaFlowUnits -> clusterRcaFlowUnits.getSummary().getHotNodeSummaryList().stream())
                .forEach(hotNodeSummary -> {
                        hotNodeSummary.getHotResourceSummaryList().stream()
                                .filter(this::isHeapResource)
                                .map(hotResourceSummary -> getAction(hotNodeSummary, hotResourceSummary))
                                .filter(Objects::nonNull)
                                .forEach(heapBasedActions::add);
                        });
        return heapBasedActions;
    }

    private boolean isHeapResource(HotResourceSummary hotResourceSummary) {
        return hotResourceSummary.getResource().getResourceEnum() == ResourceEnum.HEAP;
    }

    private AdmissionControlAction getAction(
            HotNodeSummary hotNodeSummary, HotResourceSummary hotResourceSummary) {
        double currentHeapPercent = hotResourceSummary.getValue();
        double desiredThreshold = hotResourceSummary.getThreshold();
        NodeKey esNode = new NodeKey(hotNodeSummary.getNodeID(), hotNodeSummary.getHostAddress());
        AdmissionControlAction action =
                AdmissionControlAction.newBuilder(esNode, REQUEST_SIZE, getAppContext(), rcaConf)
                        .currentValue(currentHeapPercent)
                        .desiredValue(desiredThreshold)
                        .build();
        return action.isActionable() ? action : null;
    }
}
