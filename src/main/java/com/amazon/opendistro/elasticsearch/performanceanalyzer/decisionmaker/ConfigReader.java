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

    public static final String CONFIG_FILE_PATH = "./pa_config/deciderConfig.yml";

    public ConfigReader() {
        this.deciderActionPriorityOrder = null;
    }

    public static void updateDeciderActionPriorityOrder() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        try {
            deciderActionPriorityOrder = mapper.readValue(new File(CONFIG_FILE_PATH), HashMap.class);
        } catch (Exception e) {
            LOG.error("Could not read the Decider Config File");
            e.printStackTrace();
        }
        return;
    }

    public static ArrayList<String> getActionPriorityOrder(String decider) {
        return deciderActionPriorityOrder.get(decider);
    }
}
