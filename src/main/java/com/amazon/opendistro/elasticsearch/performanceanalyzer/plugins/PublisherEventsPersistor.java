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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.plugins;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Action;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.ActionListener;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.ActionsSummary;
import java.time.Instant;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A listener that persists all actions published by the publisher to rca.sqlite
 */
public class PublisherEventsPersistor extends Plugin implements ActionListener {
    public static final String NAME = "publisher_events_persistor_plugin";
    private static final Logger LOG = LogManager.getLogger(PublisherEventsPersistor.class);

    private static Persistable persistable = null;

    @Override
    public void actionPublished(final Action action) {
        long timestamp = Instant.now().toEpochMilli();
        if (persistable != null) {
            LOG.info("Action: [{}] published to persistor publisher.", action.name());
            if (action.impactedNodes() != null) {
                action.impactedNodes().forEach(nodeKey -> {
                    final ActionsSummary actionsSummary = new ActionsSummary();
                    actionsSummary.setValues(
                            action.name(),
                            action.getResource().getNumber(),
                            nodeKey.getNodeId().toString(),
                            nodeKey.getHostAddress().toString(),
                            action.isActionable(),
                            action.coolOffPeriodInMillis(),
                            timestamp);
                    try {
                        persistable.write(actionsSummary);
                        persistable.read(ActionsSummary.class);
                    } catch (Exception e) {
                        LOG.error("Unable to write publisher events to sqlite", e);
                    }
                });
            }
        } else {
            LOG.error("Persistable object is not set in the publisher events persistor plugin");
        }
    }

    @Override
    public String name() {
        return NAME;
    }

    public static void setPersistable(Persistable sqLitePersistor) {
        persistable = sqLitePersistor;
    }
}
