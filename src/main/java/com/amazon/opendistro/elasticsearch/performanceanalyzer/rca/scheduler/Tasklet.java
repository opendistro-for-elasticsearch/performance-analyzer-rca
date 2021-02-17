/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.NetPersistor;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import com.google.common.annotations.VisibleForTesting;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Consumer;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a task abstraction on top of a Node in the Analysis framework. This is the smallest
 * runnable unit.
 */
public class Tasklet {
  private static final Logger LOG = LogManager.getLogger(Tasklet.class);

  protected List<Tasklet> predecessors;

  private Node<?> node;

  // This member should be ideally final, but to be able to change it for the
  // tests, we are making it non-final.
  private Queryable db;
  private final Persistable persistable;
  private final Map<Node<?>, List<Node<?>>> remotelyDesirableNodeSet;
  private final WireHopper hopper;
  private final NetPersistor netPersistor;
  private int ticks;
  private Consumer<FlowUnitOperationArgWrapper> exec;
  private boolean isNet = false;

  /**
   * A tasklet is always built on top of a Node.
   *
   * @param predecessorNode The node the tasklet wraps.
   * @param persistable An object that implements the persistable interface.
   * @param remotelyDesirableNodeSet The set of upstream nodes that are needed by remote downstream
   *     nodes.
   * @param hopper The object that is an abstraction for all cross-network activities.
   */
  Tasklet(
      final Node<?> predecessorNode,
      final Queryable db,
      final Persistable persistable,
      final Map<Node<?>, List<Node<?>>> remotelyDesirableNodeSet,
      final WireHopper hopper,
      final Consumer<FlowUnitOperationArgWrapper> exec) {
    this.node = predecessorNode;
    this.persistable = persistable;
    this.remotelyDesirableNodeSet = remotelyDesirableNodeSet;
    this.hopper = hopper;
    this.netPersistor = null;
    this.predecessors = new ArrayList<>();
    this.db = db;
    this.exec = exec;
    this.ticks = 0;
    this.isNet = false;
  }

  void resetTicks() {
    ticks = 0;
  }

  Tasklet addPredecessor(Tasklet tasklet) {
    this.predecessors.add(tasklet);
    return this;
  }

  public CompletableFuture<Void> execute(
      ExecutorService executorPool, Map<Tasklet, CompletableFuture<Void>> taskletToFutureMap) {
    ticks += 1;
    if (ticks % node.getEvaluationIntervalSeconds() != 0) {
      // If its not time to run this tasklet, return an isEmpty flowUnit.
      node.setEmptyFlowUnitList();
      node.setEmptyLocalFlowUnit();
      return CompletableFuture.supplyAsync(() -> null);
    }

    // Create a list of the Futures that corresponds to my predecessor nodes.
    List<CompletableFuture<Void>> predecessorResultFutures =
        predecessors.stream().map(p -> taskletToFutureMap.get(p)).collect(Collectors.toList());

    // Create a future that will wait for all the predecessors to complete.
    CompletableFuture<Void> completedPredecessorTasks =
        CompletableFuture.allOf(predecessorResultFutures.toArray(new CompletableFuture[0]));

    // Now execute me and send the response to remote if there are subscribers.
    CompletableFuture<Void> retCompletableFuture =
        completedPredecessorTasks.thenAcceptAsync(
            a -> {
              exec.accept(new FlowUnitOperationArgWrapper(node, db, persistable, hopper));
              sendToRemote();
            },
            executorPool);
    LOG.debug("RCA: Finished creating executable future for tasklet: {}", node.name());
    return retCompletableFuture;
  }

  private void sendToRemote() {
    if (remotelyDesirableNodeSet.containsKey(node)) {
      LOG.debug("Publishing to subscribers: {}", node.name());
      DataMsg dataMsg =
          new DataMsg(
              node.name(),
              remotelyDesirableNodeSet.get(node).stream()
                  .map(Node::name)
                  .collect(Collectors.toList()),
              node.getFlowUnits());
      hopper.sendData(dataMsg);
    }
  }

  @Override
  public String toString() {
    return "Tasklet for node: " + node.name() + ", with executable Func: " + exec;
  }

  public Node<?> getNode() {
    return node;
  }

  @VisibleForTesting
  public void setDb(final Queryable db) {
    this.db = db;
  }
}
