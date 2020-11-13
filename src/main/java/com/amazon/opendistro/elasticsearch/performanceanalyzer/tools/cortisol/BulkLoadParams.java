package com.amazon.opendistro.elasticsearch.performanceanalyzer.tools.cortisol;

/**
 * Class containing the metadata for the bulk stress test.
 */
public class BulkLoadParams {
    private int durationInSeconds;
    private int docsPerSecond;
    private int docsPerRequest = 1;
    private String mappingTemplateJson = "";
    private String trackLocation = "";

    public int getDurationInSeconds() {
        return durationInSeconds;
    }

    public void setDurationInSeconds(int durationInSeconds) {
        this.durationInSeconds = durationInSeconds;
    }

    public int getDocsPerSecond() {
        return docsPerSecond;
    }

    public void setDocsPerSecond(int docsPerSecond) {
        this.docsPerSecond = docsPerSecond;
    }

    public int getDocsPerRequest() {
        return docsPerRequest;
    }

    public void setDocsPerRequest(int docsPerRequest) {
        this.docsPerRequest = docsPerRequest;
    }

    public String getMappingTemplateJson() {
        return mappingTemplateJson;
    }

    public void setMappingTemplateJson(String mappingTemplateJson) {
        this.mappingTemplateJson = mappingTemplateJson;
    }

    public String getTrackLocation() {
        return trackLocation;
    }

    public void setTrackLocation(String trackLocation) {
        this.trackLocation = trackLocation;
    }
}
