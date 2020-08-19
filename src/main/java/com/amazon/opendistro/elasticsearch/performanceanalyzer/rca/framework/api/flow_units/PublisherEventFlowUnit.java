package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.actions.Consts.Table;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.decisionmaker.deciders.PublisherEvent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.GenericFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.InstanceDetails;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistableObject;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.PersistorBase;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.TableInfo;
import java.sql.SQLException;

/**
 * A flowunit that carries PublisherEvent to downstream. The purpose of creating this flowunit is
 * to leverage the persistor in RCA framework to persist PublisherEvent and its nested actions.
 * This is because only flowunit can be persisted into SQL in RCA framework.
 */
public class PublisherEventFlowUnit extends GenericFlowUnit implements PersistableObject {
  private final PublisherEvent publisherEvent;

  public PublisherEventFlowUnit(long timeStamp) {
    this(timeStamp, null);
  }

  public PublisherEventFlowUnit(long timeStamp, PublisherEvent publisherEvent) {
    super(timeStamp);
    this.publisherEvent = publisherEvent;
  }

  @Override
  public boolean isEmpty() {
    return publisherEvent == null || (!publisherEvent.isPersistable());
  }

  @Override
  public FlowUnitMessage buildFlowUnitMessage(String graphNode, InstanceDetails.Id esNode) {
    throw new IllegalStateException(this.getClass().getSimpleName() + " not expected to be passed over wire");
  }

  @Override
  public String table() {
    return Table.PUBLISHER_EVENT_TABLE;
  }

  @Override
  public void write(PersistorBase persistorBase, TableInfo referenceTableInfo) throws SQLException {
    if (isEmpty()) {
      return;
    }
    publisherEvent.write(persistorBase, referenceTableInfo);
  }
}
