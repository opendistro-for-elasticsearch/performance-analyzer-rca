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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.ExceptionsAndErrors;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.metrics.RcaRuntimeMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.ActionsSummary;
import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A listener that persists all actions published by the publisher to rca.sqlite
 */
public class PublisherEventsPersistor {
    public static final String NAME = "publisher_events_persistor";
    private static final Logger LOG = LogManager.getLogger(PublisherEventsPersistor.class);

    private final Persistable persistable;

    public PublisherEventsPersistor(final Persistable persistable) {
        this.persistable = persistable;
    }

    public void persistAction(final Action action) {
        long timestamp = Instant.now().toEpochMilli();
        if (persistable != null) {
            LOG.debug("Action: [{}] published to persistor publisher.", action.name());
            PerformanceAnalyzerApp.RCA_RUNTIME_METRICS_AGGREGATOR.updateStat(
                    RcaRuntimeMetrics.ACTIONS_PUBLISHED, this.name(), 1);
            if (action.impactedNodes() != null) {
                action.impactedNodes().forEach(nodeKey -> {
                    final ActionsSummary actionsSummary = new ActionsSummary();
                    actionsSummary.setActionName(action.name());
                    actionsSummary.setResourceValue(action.getResource().getNumber());
                    actionsSummary.setId(nodeKey.getNodeId().toString());
                    actionsSummary.setIp(nodeKey.getHostAddress().toString());
                    actionsSummary.setActionable(action.isActionable());
                    actionsSummary.setCoolOffPeriod(action.coolOffPeriodInMillis());
                    actionsSummary.setTimestamp(timestamp);
                    try {
                        persistable.write(actionsSummary);
                    } catch (Exception e) {
                        LOG.error("Unable to write publisher events to sqlite", e);
                        PerformanceAnalyzerApp.ERRORS_AND_EXCEPTIONS_AGGREGATOR.updateStat(
                                ExceptionsAndErrors.EXCEPTION_IN_WRITE_TO_SQLITE, this.name(), 1);
                    }
                });
            }
        } else {
            LOG.error("Persistable object is null in the publisher events persistor");
        }
    }

    public String name() {
        return NAME;
    }
}
