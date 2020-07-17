package com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.Decider;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.dataformat.yaml.YAMLFactory;
import java.io.File;
import java.util.ArrayList;
import java.util.HashMap;

public class ConfigReader {
    public static HashMap<Decider, ArrayList<String>> getDeciderActionPriorityOrder() {
        ObjectMapper mapper = new ObjectMapper(new YAMLFactory());
        HashMap<Decider, ArrayList<String>> deciderActionPriorityOrder = null;
        try {
            deciderActionPriorityOrder = mapper.readValue(new File("deciderConfig.yml"), HashMap.class);
            return deciderActionPriorityOrder;
        } catch (Exception e) {
            e.printStackTrace();
        }
        return deciderActionPriorityOrder;
    }
}
