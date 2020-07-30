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
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * A simple listener that logs all actions published by the publisher
 */
public class PublisherEventsLogger extends Plugin implements ActionListener {

  private static final Logger LOG = LogManager.getLogger(PublisherEventsLogger.class);
  public static final String NAME = "publisher_events_logger_plugin";

  @Override
  public void actionPublished(Action action) {
    LOG.info("Action: [{}] published by decision maker publisher.", action.name());
  }

  @Override
  public String name() {
    return NAME;
  }
}
