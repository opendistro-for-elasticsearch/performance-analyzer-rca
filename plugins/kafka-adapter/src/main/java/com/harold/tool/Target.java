package com.harold.tool;

public class Target {
    private String endpoint;
    private String parameter;
    private String bootstrapServer;
    private String url;

    public Target(String endpoint, String parameter){
        this.endpoint = endpoint;
        this.parameter = parameter;
        this.url = "http://"+this.endpoint+"?name="+this.parameter;
    }

    public Target(String endpoint){
        this.endpoint = endpoint;
        this.url = "http://"+this.endpoint;
    }

    public String getUrl(){
        return this.url;
    }

    public void setEndpoint(String endpoint){
        this.endpoint = endpoint;
    }

    public String getEndpoint() {
        return this.endpoint;
    }

    public void setParameter(String parameter) {
        this.parameter = parameter;
    }

    public String getParameter() {
        return this.parameter;
    }
}
