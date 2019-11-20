package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.PerformanceAnalyzerApp;
import com.fasterxml.jackson.core.JsonFactory;
import com.fasterxml.jackson.core.JsonParser;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import java.io.File;
import java.io.IOException;
import java.util.List;
import java.util.Map;


public class RcaConf {
    private String configFileLoc;
    protected ConfJsonWrapper conf;

    private static RcaConf instance;
    private static final Logger LOG = LogManager.getLogger(PerformanceAnalyzerApp.class);

    public RcaConf(String configPath) {
        this.configFileLoc = configPath;
        JsonFactory factory = new JsonFactory();
        factory.enable(JsonParser.Feature.ALLOW_COMMENTS);
        ObjectMapper mapper = new ObjectMapper(factory);
        try {
            this.conf = mapper.readValue(new File(this.configFileLoc), ConfJsonWrapper.class);
        } catch (IOException e) {
            LOG.error(e.getMessage());
        }
    }

    // This should only be used for Tests.
    public RcaConf() {
    }

    public static void clear() {
        instance = null;
    }

    public String getRcaStoreLoc() {
        return conf.getRcaStoreLoc();
    }

    public String getThresholdStoreLoc() {
        return conf.getThresholdStoreLoc();
    }


    public long getNewRcaCheckPeriocicityMins() {
        return conf.getNewRcaCheckPeriocicityMins();
    }

    public long getNewThresholdCheckPeriodicityMins() {
        return conf.getNewThresholdCheckPeriodicityMins();
    }

    public List<String> getPeerIpList() {
        return conf.getPeerIpList();
    }

    public Map<String, String> getTagMap() {
        return conf.getTagMap();
    }

    public Map<String, String> getDatastore() {
        return conf.getDatastore();
    }

    public String getConfigFileLoc() {
        return configFileLoc;
    }

    public String getAnalysisGraphEntryPoint() {
        return conf.getAnalysisGraphEntryPoint();
    }
}
