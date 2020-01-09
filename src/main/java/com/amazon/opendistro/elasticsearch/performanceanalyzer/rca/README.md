## Concepts ## 
This package endeavors to provide Root Cause Analysis(RCA) of the myriad
problems that may cripple a system. The RCA is done using the metrics that the
observability code (in the same package) gathers. The RCA execution and
evaluation is done as a data-flow connectedComponent execution. The data flows
from the top into the leaf nodes; each node makes sense of the data that it gets
from the upstream nodes and synthesizes the evaluation in terms of a FlowUnit
that is passed to its downstream nodes, which in turn do their own evaluation
and finally comes out one or more RCAs. So to re-iterate, this system consumes
metrics and provides RCAs. The output or RCA is essentially a non-empty set
of resources that are behaving anomalously when the system was noticed to be in
an unhealthy state. It will be an empty set if nothing is found to be wrong
and the system is in perfect health based on the sensors available.
All components are Nodes, in this data-flow connectedComponent. Nodes have
different role based on how they are defined. Before we go into the details of
the RCA framework and its evaluation, we will go through some concepts that
will run through the entirety of this discussion.

### Metrics ###
This is the input to the system. These are essentially key-value pairs. For
details on what metrics are available, please take a look at the  online
 reference [here](https://opendistro.github.io/for-elasticsearch-docs/docs/pa/reference/).

### Symptoms ###

A symptom is a boolean question about the state of a resource. When we notice that
the system is running slow, we might be interested in questions such as "is
 the CPU contended" ? In this question, CPU is the resource and the state we are
interested here is contended. So, the symptom is a 2-tuple of a resource and the
state. Similar questions can be whether the disk has a low service-rate and so
forth. Answer to this question returns a value from the set
{True, False, InsufficientData}. True and False are self explanatory but in what
cases can this return InsufficientData ? We might want to know the write
-throughput but not enough data has been written in the last few minutes for us to
conclude the state with high probability. In such cases the Symptom for would
output InsufficientData. Symptoms consume one or more metrics and may or may
not consume other symptoms to to arrive at a conclusion. As astute reader might
notice that when we often say a CPU is high or something as such, we often
compare the current value against a threshold. Worry not, we do support quite
an elaborate threshold definition that can be used to evaluate a symptom. It is
discussed further down in the doc. Threshold is not a node of the data-flow
 and so we will defer the discussion.

### RCA ###
This is what we have been waiting for. A root cause is a non-empty set of
resources in the cluster that is diagnosed in a not-healthy state at a given
point in time. One thing to keep in mind is that the state changes based on
whether the problems are transitory others are not. So, for this system, the
output of RCA will be a set of one or many 2-tuples containing the resource
and the state that it is diagnosed to be in. In terms of data-flow, input to the
RCA node can be
- one or more metric(s)
- one or more symptom(s)
- one or more rca(s)

## Framework ##
Now that we are passed the basic concepts, lets get the details on the framework.
At the top of the dataflow node types is the node. A node can be further
categorized into `Leaf` and `NonLeaf` nodes. Recall that the metrics are the
leaves and they have no dependency on other nodes. So `Metrics` extend the
 ***Leaf*** node. `RCA`s and `Symptom`s cannot be at top as they need data to
evaluate. So `RCA` and `Symptom` extend from the `NonLeaf` nodes. Because
***RCA***s and ***Symptom*** needs to be evaluated, they implement the
Evaluable and as a virtue of which anytime you create a class that extends
from `Symptom` or `Rca` the responsibility falls on you to implement the
`evaluate` method.


### Analysis Graph ###
This is the plot of how RCAs will be evaluated. The way to think about RCA is a
dataflow graphs. The data enters through the top and RCAs come out at the
bottom. Its an inverted DAG where the metrics are the leaf nodes. Every
downstream node depends on one or more upstream nodes. The Metric-nodes being
the leaf of the inverted connectedComponent, have no dependencies themselves.

### Node Tagging ###
Whether a graph node is executed at a location or not is determined by a
combination of two things, what tags the node has and what tag is present in
the rca.conf. Before execution of a node, all the node tags (keys:values) are
matched with the tags in the rca.conf. A node will usually have less tags
than the rca.conf or else it will not execute. The way tag matching works
is for each key:value pair in the tag lust of the node, the key is looked up
for in the rca.conf and the corresponding values are matched. If a match
happens, then we move on to the next tag of the node and so on. Only if
all the tags of the node matches (rca.conf can have extra tags and that
does not affect the tag matching), then the run-time goes on to execute the
node. If the node has some extra tags that are not present in the rca.conf, 
then it is considered as a no-match. A node that has no tags associated
 with it, will execute in all locations. 

<u>Note</u>: One thing to note is that if a node does not run on any location
, then none of its dependencies can run on that location because the data required will
not be available. So if a node does not run on a location, the runtime also
turns off all its dependents in that location. 

Node tagging is also how the runtime decides whether a node's data is
required by a remote location and how a remote location knows that the data
it needs to evaluate a local node is supposed to be obtained from a remote
upstream node. This is a good segue into the  the intent based message passing.



#### Intent Based Message passing between Network separated Graph Nodes
This section describes the interfacing of the runtime and the network thread for
message passing between the graph nodes that reside on different physical
machines. The underlying invariant is that the data always flows downstream and
the control or intent always flows upstream. The RCA runtime in all the
locations have the entire Analysis graph. So when a runtime present in a
downstream location figures out that it needs data from an upstream location,
it initiates an intent to get the data from the upstream node. This intent
message contains the Graph Node name that is requesting the data, and the tags
associated with that graph node. This message is passed to the local network
thread. The network thread broadcasts this to all the peers upstream. The
receivers of this message adds an entry to a table with this information and
then initiates a long lived connection with the downstream node that expressed
the intention. When the runtime at the data generation side, based on tag
-matching, figures out that a given node cannot run locally, then it send it the
way of the network thread. This message has the data and also the Graph node and
the tags of the node that is expected to receive it. Given this information, the
network thread looks up its local table and see if there is a downstream node
that has expressed an intension to receive such a message; if so, then the
network thread sends the message to the interested parties. If there are no
takers for it then the network thread drops it on the floor.
On the message receiver side, when the network thread receives such a message,
it puts it in a location and provides a handle to the Runtime. In the next
execution of the tasks, the Runtime uses this data for evaluation. 

 
## Specification ##
This is the way of specifying the Analysis Flow Field or constructing it by
creating the Metrics, Symptoms and RCA objects and defining their upstreams
. This is defining what will run to calculate the RCA and this is provided by
the user of the RCA system. Specifying RCA is a three step process:
 
 1. Create all the Symptoms and RCA classes.
 2. Define the threshold and their environment based overrides using the
    <threshold-name>.json file.
 3. Extend the AnalysisFlowField class and override the constructor method.
    In this method, instantiate the Symptom and RCA classes and define their
    upstream dependencies. An upstream dependency can be a Metric or a
    Symptom or an RCA. This is essentially a full connectedComponent
    specification.
 4. For each of the class that extends the Symptom or RCA class, fill in the
    evaluate method. An evaluate method can be arbitrary Java code making use
     of the dependencies defined in step 3 and thresholds.

    ```java

    // 1. Create your classes by extending the Symptoms and Rca classes.
    class MySymptom extends Symptom {
        @Override
        public BooleanFlowUnit evaluate(Map<Class, List<FlowUnit>> dependencies) {
            return NULL;
        }
    }

    class MyRca extends Rca {
        @Override
        public FlowUnit evaluate(Map<Class, List<FlowUnit>> dependencies) {
            return NULL;
        }
    }

    // 2. Extend the AnalysisFLowField and fill in the construct method.
    class MyAnalysisFlowField extends AnalysisFlowField {
        @Override
        public void construct() {
            // 1. Instantiate all the Metrics and Symptoms and RCAs here.
            // 2. Add the metrics to the flow field by calling addLeaf()
            // 2. Connect node to other nodes by stating its dependencies by
            // callling .addAllUpstreams()
        }
    }

    operate
    // with some meaningful calculations using the static methods from
    // NumericAggregator and/or BooleanAggregator
    ```


## Runtime Evaluation of RCAs ##
The RCAs defined in the specification are executed by the RCA agent on the nodes
of the cluster. This is how the runtime instances the AnalysisFlowField and
 evaluates it:

  1. Call the construct() method on the class that overrides the
     AnalysisFlowField.
  2. call the validateAndProcess() on the same object to verify the
      connectedComponent creation and instantiation of some
       connectedComponent data structures.
  3. Call the Stats.getInstance().getGraphs() to get all the graphs in the flowField.
  4. foreach connectedComponent as returned in 3. call the
     getAllNodesByDependencyOrder() to get a hierarchical list of Nodes
     to evaluate.
  5. For each node, call the  getUpstreams(), and create a list of samples of
     the dependencySpec thus returned.
  6. With all such dependencies, call the evaluate() method of the node.

  ```java
  class Runtime {
      AnalysisFlowField flowField = new MyAnalysisFlowField();
      flowField.construct();
      flowField.validateAndProcess();
      for(Graph connectedComponent: Stats.getInstance().getGraphs()) {
          for(List<Node> list: connectedComponent.getAllNodesByDependencyOrder()) {
              for (Node node: list) {
                  Map<Class, List<FlowUnit>> map = new HashMap();
                  for (DependencySpec dep: node.getUpstreams()) {
                      // Gather the dependencies

                      // create a list with number of samples as mentioned in DependencySpec.windowLength.
                      // map.put(DependencySpec.dependencyClass.class, ArrayList<>());
                  }
                  node.evaluate(map);
              }
          }
      }
  }
  ```

