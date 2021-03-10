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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.admission_control;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.Rca;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotClusterSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.summaries.HotNodeSummary;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.cluster.BaseClusterRca;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class AdmissionControlClusterRca extends BaseClusterRca {

    public static final String RCA_TABLE_NAME = AdmissionControlClusterRca.class.getSimpleName();
    private static final Logger LOG = LogManager.getLogger(AdmissionControlClusterRca.class);

    public <R extends Rca<ResourceFlowUnit<HotNodeSummary>>> AdmissionControlClusterRca(final int rcaPeriod, final R nodeRca) {
        super(rcaPeriod, nodeRca);
    }

    @Override
    public ResourceFlowUnit<HotClusterSummary> operate() {
        LOG.info("[AdmissionControl] {}", System.currentTimeMillis());
        return new ResourceFlowUnit<>(System.currentTimeMillis());
    }
}
