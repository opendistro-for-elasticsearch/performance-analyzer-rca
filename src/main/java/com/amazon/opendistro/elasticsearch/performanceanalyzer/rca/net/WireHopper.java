package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;

public class WireHopper {
  public void sendIntent(IntentMsg msg) {}

  public void sendData(DataMsg dataMsg) {}

  public FlowUnit readFromWire(String nodeName) {
    // TODO Needs to be filled. Currently just sending empty flowUnit.
    return FlowUnit.generic();
  }
}
