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

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.ConnectedComponent;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Node;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.Queryable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core.RcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.util.RcaUtil;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.messages.IntentMsg;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.net.WireHopper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.Persistable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

public class RCASchedulerTask implements Runnable {
  private static final Logger LOG = LogManager.getLogger(RCASchedulerTask.class);

  /**
   * This is a wrapper class for return type of createTaskletAndSendIntent method. This is required
   * because this method usually returns the tasklet created for a given graphNode. Occasionally, it
   * can return tasklets for remotely executable node. This happens when a locally executable node,
   * needs data from a remote node.
   */
  private static class CreatedTasklets {
    /** Tasklet for the locally executable node. */
    Tasklet taskletForCurrentNode;

    /** List of tasklets corresponding to one or many remote nodes. */
    List<Tasklet> remoteTasklets;

    CreatedTasklets(Tasklet taskletForCurrentNode) {
      this.taskletForCurrentNode = taskletForCurrentNode;
      this.remoteTasklets = new ArrayList<>();
    }
  }

  /** Maximum ticks after which the counter will be reset. */
  private int maxTicks;

  /** To keep track of the number of executions of the thread. */
  private int currTick;

  /** The thread pool to execute the tasklets. */
  private final ExecutorService executorPool;

  /**
   * List of locally executable nodes whose data might be needed by downstream remote nodes. We keep
   * track of such nodes, so that the data can be sent to the network thread (wireHopper) as soon as
   * it is generated, to be delivered to the remote node that needs it. The key is a locally
   * evaluable node, the Value is the list of remote nodes that would like the data.
   */
  private final Map<Node<?>, List<Node<?>>> remotelyDesirableNodeSet;

  /**
   * A levelled list of tasklets across all connected components. Level 0's results are needs to
   * execute level 1 and so on.
   */
  private final List<List<Tasklet>> locallyExecutableTasklets;

  // TODO: Q/ : maybe the max Ticks should be determined by what the max periodicity the user has
  // specified for a
  //  graph node? If this periodicity is lower than that, then some nodes may never get executed. I
  // guess, who
  //  should provide the max ticks - the framework or the Runtime ? Maybe an agreement between the
  // two is better.
  RCASchedulerTask(
      int maxTicks,
      final ExecutorService executorPool,
      final List<ConnectedComponent> connectedComponents,
      final Queryable db,
      final Persistable persistable,
      final RcaConf conf,
      final WireHopper hopper) {
    this.maxTicks = maxTicks;
    this.executorPool = executorPool;
    this.remotelyDesirableNodeSet = new HashMap<>();
    Map<Node<?>, Tasklet> nodeTaskletMap = new HashMap<>();

    List<List<Tasklet>> dependencyOrderedLocallyExecutables = Collections.emptyList();
    for (ConnectedComponent component : connectedComponents) {
      List<List<Tasklet>> orderedTasklets =
          getLocallyExecutableNodes(
              component.getAllNodesByDependencyOrder(),
              conf,
              hopper,
              db,
              persistable,
              nodeTaskletMap);

      // Merge the list across connected components.
      dependencyOrderedLocallyExecutables =
          mergeLists(orderedTasklets, dependencyOrderedLocallyExecutables);
    }
    this.locallyExecutableTasklets =
        Collections.unmodifiableList(dependencyOrderedLocallyExecutables);
    LOG.debug("rca: locally executable tasklet size: {}", locallyExecutableTasklets.size());
  }

  /**
   * Merge two list of lists level wise, that is the level0 list of the first list is merged with
   * the level 0 of the second list, level 1 of the first list is merged with the level 1 of the
   * second list and so on.
   *
   * @param l1 first list
   * @param l2 second list
   * @return The modified larger list merged with the other.
   */
  public static <T> List<List<T>> mergeLists(List<List<T>> l1, List<List<T>> l2) {
    if (l2.size() > l1.size()) {
      return mergeLists(l2, l1);
    }
    // At this point l1 is the list with more levels.
    for (int idx = 0; idx < l2.size(); idx++) {
      l1.get(idx).addAll(l2.get(idx));
    }
    return l1;
  }

  /**
   * From the ordered list of nodes in a connected component, create an ordered list of Tasklets.
   *
   * <p>For the list of list of nodes in a connected component, peel off layer by later (The top
   * later has no predecessors), and then for each node in a layer, check if it is to be executed
   * locally by matching tags added to the node at creation time and the tags present in the
   * rca.conf, if this is to be executed locally, scavenge though the list of upstream nodes for it,
   * and see if there is one or more, that are remote, if so, send an intent to get their data. if
   * the node is not to be executed locally, then look through its upstream nodes to see if there is
   * one that is to be executed locally. If so, then the upstream node's data will be needed by this
   * node. So, keep track of it, so that the scheduler remembers to send it every time new data is
   * generated for the upstream node.
   *
   * @param orderedNodes list of list of nodes in a connected component
   * @param conf The rcaConf object, used for tag matching to determine if a node will be executed
   *     locally.
   * @param hopper The network listener object, used to send intent to receive remotely generated
   *     data and to send data to remote nodes which needs data generated on this node.
   * @param db A abstraction which is used by the metric nodes to get data from PA reader.
   * @param persistable This object is used to write the results of the RCA evaluation to a
   *     persistent store.
   * @param nodeTaskletMap This is a helper structure, to retrieve the Tasklet corresponding to a
   *     graph node.
   * @return a level ordered list of Tasklets.
   */
  private List<List<Tasklet>> getLocallyExecutableNodes(
      List<List<Node<?>>> orderedNodes,
      RcaConf conf,
      WireHopper hopper,
      Queryable db,
      Persistable persistable,
      Map<Node<?>, Tasklet> nodeTaskletMap) {
    // This is just used for membership check in the createTaskletAndSendIntent. If a node is
    // present here, then the tasklet corresponding to it will be doing local evaluation or else,
    // the tasklet will read data from the read API provided by the wirehopper.
    Set<Node<?>> locallyExecutableSet = new HashSet<>();

    // The list to be returned.
    List<List<Tasklet>> dependencyOrderedLocallyExecutable = new ArrayList<>();

    for (List<Node<?>> levelNodes : orderedNodes) {
      List<Tasklet> locallyExecutableInThisLevel = new ArrayList<>();
      for (Node<?> node : levelNodes) {
        if (RcaUtil.doTagsMatch(node, conf)) {
          // This node will be executed locally, so add it to the set to keep track of this.
          locallyExecutableSet.add(node);

          // Now we gather all the remote dependencies if there are any and request an intent to
          // consume their data.
          CreatedTasklets newTasklets =
              createTaskletAndSendIntent(
                  node, locallyExecutableSet, hopper, db, persistable, nodeTaskletMap);
          nodeTaskletMap.put(node, newTasklets.taskletForCurrentNode);
          locallyExecutableInThisLevel.add(newTasklets.taskletForCurrentNode);

          // If there are remote upstream nodes, then we should add their proxy virtual
          // tasklets.
          if (!newTasklets.remoteTasklets.isEmpty()) {
            if (dependencyOrderedLocallyExecutable.isEmpty()) {
              // We are in a situation where although the current node is the first node to be
              // executed locally, but all the upstream dependencies are remote. So, we want to
              // add all the remote nodes in a level of their own, in this case level 0.
              dependencyOrderedLocallyExecutable.add(newTasklets.remoteTasklets);
            } else {
              int lastIdx = dependencyOrderedLocallyExecutable.size() - 1;

              // Here, we don't want to add the list of nodes as a whole but each individual node
              // in the list.
              dependencyOrderedLocallyExecutable.get(lastIdx).addAll(newTasklets.remoteTasklets);
            }
          }
        } else {
          // If the node is not executed locally, we check the predecessors of this node, to
          // see if they are to be evaluated locally. If so, then this node will need that
          // predecessor's data to evaluate itself. We want to keep track of this, so that we can
          // send the data through the wireHopper to whoever might need it.
          LOG.debug("rca: tag NOT matched for node: {}", node.name());
          for (Node<?> upstreamNode : node.getUpstreams()) {
            if (locallyExecutableSet.contains(upstreamNode)) {
              // This node was executed locally.
              if (remotelyDesirableNodeSet.containsKey(upstreamNode)) {
                remotelyDesirableNodeSet.get(upstreamNode).add(node);
              } else {
                List<Node<?>> list = new ArrayList<>();
                list.add(node);
                remotelyDesirableNodeSet.put(upstreamNode, list);
              }
            }
          }
        }
      }

      // We don't want to add isEmpty lists.
      if (!locallyExecutableInThisLevel.isEmpty()) {
        dependencyOrderedLocallyExecutable.add(locallyExecutableInThisLevel);
      }
    }
    return dependencyOrderedLocallyExecutable;
  }

  /**
   * Sending intent is a request to an upstream node to send its data whenever new data points are
   * generated. The intent is sent to the locally running networking thread which carries it forward
   * to the destination. An intent is only sent if one or more of the upstream dependencies are not
   * locally executed. In addition to sending intent, this method also creates a tasklet
   * corresponding to the node. If this node needs data from remote nodes, then it also creates
   * tasklet representations of the remote nodes. The remote node tasklets, are used to read the
   * data on the wire corresponding to the remote node, when it is available.
   *
   * @param graphNode The locally running graph node for which we want to get the data from upstream
   * @param locallyExecutableNodeSet A set of inspected nodes so far that are to be executed locally
   * @param hopper The network proxy
   * @param db This object is used to query the database to get the metrics; for reads.
   * @param persistable The instance of the database where RCAs are to be persisted; for writes.
   * @param nodeTaskletMap A table to get the tasklet corresponding to a graph node.
   */
  protected CreatedTasklets createTaskletAndSendIntent(
      Node<?> graphNode,
      Set<Node<?>> locallyExecutableNodeSet,
      WireHopper hopper,
      Queryable db,
      Persistable persistable,
      Map<Node<?>, Tasklet> nodeTaskletMap) {
    Tasklet tasklet;
    tasklet =
        new Tasklet(
            graphNode,
            db,
            persistable,
            remotelyDesirableNodeSet,
            hopper,
            GraphNodeOperations::readFromLocal);
    CreatedTasklets ret = new CreatedTasklets(tasklet);

    for (Node<?> upstreamNode : graphNode.getUpstreams()) {
      // A tasklet should exist for each upstream dependency. Based on whether this is
      // locally available or not, a different execution function will be passed in.
      if (locallyExecutableNodeSet.contains(upstreamNode)) {
        // This upstream node is executed locally. So it should be in the nodeTaskletMap.
        tasklet.addPredecessor(nodeTaskletMap.get(upstreamNode));
      } else {
        // If we are here, then it means that the upstream node required to evaluate
        // this node, is not locally executed. Hence, we have to send an intent to get the
        // node's data from the remote node.
        LOG.debug(
            "rca: Node '{}' sending intent to consume node: '{}'",
            graphNode.name(), upstreamNode.name());
        IntentMsg msg =
            new IntentMsg(graphNode.name(), upstreamNode.name(), upstreamNode.getTags());
        hopper.sendIntent(msg);

        // This node is not locally present. So, we will add a virtual Tasklet that reads
        // the result where the wirehopper dumps it and constructs the Tasklet for us.
        Tasklet remoteTasklet =
            new Tasklet(
                upstreamNode,
                db,
                persistable,
                remotelyDesirableNodeSet,
                hopper,
                GraphNodeOperations::readFromWire);
        LOG.debug("Tasklet created for REMOTE node '{}' with readFromWire", graphNode.name());
        tasklet.addPredecessor(remoteTasklet);
        ret.remoteTasklets.add(remoteTasklet);
      }
    }
    return ret;
  }

  public void run() {
    currTick = currTick + 1;

    Map<Tasklet, CompletableFuture<TaskletResult>> taskletFutureMap = new HashMap<>();
    LOG.debug("RCA: ========== STRT Tick {} ====== ", currTick);
    for (List<Tasklet> taskletsAtThisLevel : locallyExecutableTasklets) {
      for (Tasklet tasklet : taskletsAtThisLevel) {
        tasklet.setPredecessorToFutureMap(taskletFutureMap);
        CompletableFuture<TaskletResult> taskletFuture = tasklet.execute(executorPool);
        taskletFutureMap.put(tasklet, taskletFuture);
      }
    }
    LOG.debug("RCA: Finished creating tasks ..");
    // Now we will wait for the results to show up.
    taskletFutureMap.values().forEach(CompletableFuture::join);

    // TODO: Do proper exception handling.
    // No one is calling get on the the last set of Tasklets.

    if (currTick == maxTicks) {
      currTick = 0;
      locallyExecutableTasklets.forEach(l -> l.forEach(Tasklet::resetTicks));
      LOG.debug("Finished ticking.");
    }
  }
}
