package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.grpc.FlowUnitMessage;
import com.google.protobuf.GeneratedMessageV3;
import java.util.ArrayList;
import java.util.List;
import org.jooq.Field;

public abstract class GenericSummary {

  public GenericSummary() {
    nestedSummaryList = new ArrayList<>();
  }

  protected List<GenericSummary> nestedSummaryList;

  public List<GenericSummary> getNestedSummaryList() {
    return nestedSummaryList;
  }

  public void setNestedSummaryList(List<GenericSummary> nestedSummaryList) {
    this.nestedSummaryList = nestedSummaryList;
  }

  public void addNestedSummaryList(GenericSummary nestedSummary) {
    this.nestedSummaryList.add(nestedSummary);
  }

  public abstract <T extends GeneratedMessageV3> T buildSummaryMessage();

  public abstract void buildSummaryMessageAndAddToFlowUnit(FlowUnitMessage.Builder messageBuilder);

  public abstract List<Field<?>> getSqlSchema();

  public abstract List<Object> getSqlValue();
}
