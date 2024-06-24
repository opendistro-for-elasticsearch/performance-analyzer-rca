package com.amazon.opendistro.elasticsearch.performanceanalyzer.tools.cortisol;

import org.jooq.tools.json.JSONObject;

public class CortisolHelper {
    public static String buildIndexSettingsJson(final int nPrimaries, final int nReplicas) {
        JSONObject container = new JSONObject();
        JSONObject settings = new JSONObject();
        JSONObject index = new JSONObject();
        index.put("number_of_shards", nPrimaries);
        index.put("number_of_replicas", nReplicas);
        settings.put("index", index);
        container.put("settings", settings);
        return container.toString();
    }
}
