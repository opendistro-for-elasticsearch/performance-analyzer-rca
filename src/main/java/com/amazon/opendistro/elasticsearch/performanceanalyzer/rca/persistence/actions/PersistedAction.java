package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.ValueColumn;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * Action summary that can be persisted in SQL
 *
 * <p>Table name : PersistedAction
 *
 * <p>schema :
 * | id(primary key) |     actionName       |   timestamp       |       nodeIds          |
 * |      1          | ModifyQueueCapacity  |  1599257910923    |   node1, node2         |
 * |        nodeIps         | actionable | coolOffPeriod    |  muted    |   summary         |
 * | 127.0.0.1, 127.0.0.2   |  1         |  300             |   0       | actionSummary     |
 */
public class PersistedAction {
    private static final Logger LOG = LogManager.getLogger(PersistedAction.class);

    @ValueColumn public String actionName;
    @ValueColumn public String nodeIds;
    @ValueColumn public String nodeIps;
    @ValueColumn public boolean actionable;
    @ValueColumn public long coolOffPeriod;
    @ValueColumn public boolean muted;
    @ValueColumn public String summary;
    @ValueColumn public long timestamp;

    public String getActionName() {
        return actionName;
    }

    public void setActionName(String actionName) {
        this.actionName = actionName;
    }

    public void setNodeIds(String nodeIds) {
        this.nodeIds = nodeIds;
    }

    public String getNodeIds() {
        return this.nodeIds;
    }

    public void setNodeIps(String nodeIps) {
        this.nodeIps = nodeIps;
    }

    public String getNodeIps() {
        return this.nodeIps;
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

    public boolean isMuted() {
        return muted;
    }

    public void setMuted(boolean muted) {
        this.muted = muted;
    }

    public String getSummary() {
        return summary;
    }

    public void setSummary(String summary) {
        this.summary = summary;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp(long timestamp) {
        this.timestamp = timestamp;
    }
}
