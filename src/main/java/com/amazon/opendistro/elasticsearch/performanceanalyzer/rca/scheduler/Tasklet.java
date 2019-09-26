package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.scheduler;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.FlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.DataMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.function.Function;
import java.util.stream.Collectors;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * This is a task abstraction on top of a Node in the Analysis framework. This is the smallest
 * runnable unit.
 */
public class Tasklet {
  private static final Logger LOG = LogManager.getLogger(Tasklet.class);
  private final Queryable db;
  private final Persistable persistable;
  private final Map<Node, List<Node>> remotelyDesirableNodeSet;
  private final WireHopper hopper;
  protected Map<Tasklet, CompletableFuture<TaskletResult>> predecessorToFutureMap;
  protected List<Tasklet> predecessors;
  private Node node;
  private int ticks;
  private Function<OperationArgWrapper, FlowUnit> exec;

  /**
   * A tasklet is always built on top of a Node.
   *
   * @param predecessorNode The node the tasklet wraps.
   * @param persistable Supports persistence of RCAs
   * @param remotelyDesirableNodeSet The set of nodes requested by some remote RCA runtime
   * @param hopper The network handler for RCA runtime.
   */
  Tasklet(
      final Node predecessorNode,
      final Queryable db,
      final Persistable persistable,
      final Map<Node, List<Node>> remotelyDesirableNodeSet,
      final WireHopper hopper,
      final Function<OperationArgWrapper, FlowUnit> exec) {
    this.node = predecessorNode;
    this.persistable = persistable;
    this.remotelyDesirableNodeSet = remotelyDesirableNodeSet;
    this.hopper = hopper;
    this.predecessors = new ArrayList<>();
    this.db = db;
    this.exec = exec;
    this.ticks = 0;
  }

  void resetTicks() {
    ticks = 0;
  }

  Tasklet addPredecessor(Tasklet tasklet) {
    this.predecessors.add(tasklet);
    return this;
  }

  void setPredecessorToFutureMap(
      final Map<Tasklet, CompletableFuture<TaskletResult>> predecessorToFutureMap) {
    this.predecessorToFutureMap = predecessorToFutureMap;
  }

  public CompletableFuture<TaskletResult> execute(ExecutorService executorPool) {
    ticks += 1;
    if (ticks % node.getEvaluationIntervalSeconds() != 0) {
      // If its not time to run this tasklet, return an isEmpty flowUnit.
      return CompletableFuture.supplyAsync(
          () -> new TaskletResult(node.getClass(), FlowUnit.generic()));
    }

    // Map<Class, FlowUnit> dependencyDataMap = new HashMap<>();

    List<CompletableFuture<TaskletResult>> predecessorResultFutures =
        predecessors.stream().map(p -> predecessorToFutureMap.get(p)).collect(Collectors.toList());

    CompletableFuture<Void> completedPredecessorTasks =
        CompletableFuture.allOf(predecessorResultFutures.toArray(new CompletableFuture[0]));

    CompletableFuture<List<TaskletResult>> futureTasklets =
        completedPredecessorTasks.thenApplyAsync(
            f ->
                predecessorResultFutures.stream()
                    .map(CompletableFuture::join)
                    .collect(Collectors.toList()),
            executorPool);

    CompletableFuture<TaskletResult> retCompletableFuture =
        futureTasklets.thenApplyAsync(
            taskletResultsList -> {
              Map<Class, FlowUnit> dependencyDataMap =
                  taskletResultsList.stream()
                      .collect(
                          Collectors.toMap(TaskletResult::getNode, TaskletResult::getFlowUnit));
              LOG.info("RCA: Executing the function for node {}", node.name());
              FlowUnit flowUnit =
                  exec.apply(
                      new OperationArgWrapper(node, db, persistable, hopper, dependencyDataMap));
              Objects.requireNonNull(
                  flowUnit,
                  String.format(
                      "ERR: Node '%s' returns a NULL flow unit. If you have nothing to "
                          + "return, try returning FlowUnit.generic() instead.",
                      node.name()));
              sendToRemote(flowUnit);
              LOG.debug("RCA: ========== END {} ==== Flow unit '{}'", node.name(), flowUnit);
              return new TaskletResult(node.getClass(), flowUnit);
            },
            executorPool);
    LOG.info("RCA: Finished creating executable future for tasklet: {}", node.name());
    return retCompletableFuture;
  }

  private void sendToRemote(FlowUnit flowUnit) {
    if (remotelyDesirableNodeSet.containsKey(node)) {
      DataMsg dataMsg =
          new DataMsg(
              node.name(),
              remotelyDesirableNodeSet.get(node).stream()
                  .map(Node::name)
                  .collect(Collectors.toList()),
              flowUnit);
      hopper.sendData(dataMsg);
    }
  }

  @Override
  public String toString() {
    return "Tasklet for node: " + node.name() + ", with executable Func: " + exec;
  }
}
