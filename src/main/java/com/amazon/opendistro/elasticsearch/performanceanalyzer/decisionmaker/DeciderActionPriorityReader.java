/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This class reads user provided Decider Action Priorities from a yaml.
 * Default action priorities reside in the file.
 * Based on the load, the user can configure the action priorities for different deciders.
 */

public class DeciderActionPriorityReader {
    private static final Logger LOG = LogManager.getLogger(DeciderActionPriorityReader.class);
    private static HashMap<Decider, ArrayList<String>> deciderActionPriorityOrder;
    private static String configFilePath;

    public DeciderActionPriorityReader(String configFilePath) {
    this.configFilePath = configFilePath;
    this.deciderActionPriorityOrder = null;
    }

    public static void updateDeciderActionPriorityOrder() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            deciderActionPriorityOrder = mapper.readValue(new File(configFilePath), HashMap.class);
        } catch (Exception e) {
            LOG.error("Could not read the Decider Config File");
            e.printStackTrace();
        }
        return;
    }

    public static ArrayList<String> getActionPriorityOrder(String decider) {
        ArrayList<String> actionPriorities = null;
        try {
            actionPriorities = deciderActionPriorityOrder.get(decider);
        } catch (Exception e) {
            LOG.error("Decider Not found in the Config File");
            e.printStackTrace();
        }
        return actionPriorities;
    }
}
