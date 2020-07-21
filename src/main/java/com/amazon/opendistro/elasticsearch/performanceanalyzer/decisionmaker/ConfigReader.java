package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class ConfigReader {
    private static final Logger LOG = LogManager.getLogger(ConfigReader.class);
    private static HashMap<Decider, ArrayList<String>> deciderActionPriorityOrder;
    private static String configFilePath;

    public ConfigReader(String configFilePath) {
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
