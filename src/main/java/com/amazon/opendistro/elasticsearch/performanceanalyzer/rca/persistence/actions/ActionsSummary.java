package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.ValueColumn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Action summary that can be persisted in SQL
 *
 * <p>Table name : ActionsSummary
 *
 * <p>schema :
 * | id(primary key) |     actionName       |  resourceValue    | nodeId    |
 * |      1          | ModifyQueueCapacity  |      1            |  node1    |
 * | nodeIp     | actionable | coolOffPeriod  |     timestamp
 * | 127.0.0.1  |  true      |  300          |   1599257910923
 */
public class ActionsSummary {
    private static final Logger LOG = LogManager.getLogger(ActionsSummary.class);

    @ValueColumn public String actionName;
    @ValueColumn public long resourceValue;
    @ValueColumn public String id;
    @ValueColumn public String ip;
    @ValueColumn public boolean actionable;
    @ValueColumn public long coolOffPeriod;
    @ValueColumn public long timestamp;

    public void setValues(
            String actionName, long resourceValue, String id, String ip, boolean actionable, long coolOffPeriod, long timestamp) {
        this.actionName = actionName;
        this.resourceValue = resourceValue;
        this.id = id;
        this.ip = ip;
        this.actionable = actionable;
        this.coolOffPeriod = coolOffPeriod;
        this.timestamp = timestamp;
    }

    public String getActionName() {
        return actionName;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public long getResourceValue() {
        return resourceValue;
    }

    public void setResourceValue(long resourceValue) {
        this.resourceValue = resourceValue;
    }

    public void setId(String id) {
        this.id = id;
    }

    public String getId() {
        return this.id;
    }

    public void setIp(String ip) {
        this.ip = ip;
    }

    public String getIp() {
        return this.ip;
    }

    public boolean isActionable() {
        return actionable;
    }

    public void setActionable(boolean actionable) {
        this.actionable = actionable;
    }

    public long getCoolOffPeriod() {
        return coolOffPeriod;
    }

    public void setCoolOffPeriod(long coolOffPeriod) {
        this.coolOffPeriod = coolOffPeriod;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
