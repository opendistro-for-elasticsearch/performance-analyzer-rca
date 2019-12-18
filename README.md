## Performance Analyzer RCA

The Performance Analyzer RCA is a framework that builds on the Performance Analyzer engine to
support Root Cause Analysis (RCA) of performance and reliability problems in Elasticsearch
clusters. This framework executes real time root cause analyses using Performance Analyzer
metrics. Root cause analysis can significantly improve operations, administration and
provisioning of Elasticsearch clusters, and it can enable Elasticsearch client teams to tune
their workloads to reduce errors.

## RCA Overview
The RCA framework is a distributed data-flow graph where the data enters from the top, as
Performance Analyzer metrics referred to as Leaf nodes in the code, the intermediate nodes
perform calculations and out comes the RCAs. To be clear, there is one graph but it cam have
multiple connected-components depending on which metric nodes are common between them. So, one
connected component can emit multiple RCAs. Its **not** one graph per RCA.

The flow of data happens from top to bottom. For a node at level _k_, all nodes that are in levels
`[0, k-1]` are referred to as upstream nodes and all nodes in levels `[k+1, N-1]` are referred to as
 downstream nodes. _Metric_ nodes are always `level 0` or `leaf` nodes.

### Terminologies


__RCA__: An RCA is a function over zero or more metrics and zero or more upstream symptoms and zero
or more upstream RCAs. Simple put, an _RCA_ provide the state of a resource - healthy or unhealthy
or contended or unbalanced.

__Symptom__: A symptom is a mathematical computation over one or many metrics and zero or more
upstream _Symptoms_. It can calculate things like moving average over k samples and state whether
the value was above or below the threshold. Ideally the output of a Symptom node will be binary -
`presence`(true) or `absence`(false).

__Metrics__: Metrics are the Performance Analyzer(PA) metrics. But say you want to fit this on top
of metrics whih you are already collecting and are different from the  PA metrics, you should be
able to plug them in with no hastle. We will describe --> IS THIS INCOMPLETE?

### Components

__Framework__: The framework gives you the bells ans whistles to construct the graph. The top level
class for this is the `AnalysisGraph` class. In order to create the RCA graph, one needs to derive
from this class and override the `construct` method. The fully qualified path to the class should be
added as value to the key `analysis-graph-implementor` in `pa_config/rca.conf`. The runtime will
call the `construct` method of this class to construct the graph before it hands it over to the
scheduler to execute it. The leaf nodes or metric nodes are added by caling the method `addLeaf`.
At an arbitrary level, for a given node, all the upstream nodes should be specified at once by
calling `addAllUpstreams`. Details of how an RCA graph can be found in the walkthrough section.

__Scheduler__: The scheduler is the orchestrator of RCA. On a cluster, it constructs the graph,
spins up threads and calls the `operate` method on each graph node. It takes care of executing nodes
in order so that for a given node all the upstream data is available and also nodes in a given level
in parallel. The code for them can be found in `RCAScheduler` and `RCASchedulerTask` classes. 

__WireHopper__: This is the networking-orchastrator for the framework. It abstracts out the fact
that not all graph nodes are executed in the same physical machine. During the bootstrap of the
scheduler, the wirehopper helps send the intent to the remote nodes that their data will be needed
by the bootstraping machine. While the RCA is running, for each evaluated node that has remote
subscribers, wirehopper transports the data to them. It uses GRpc internally for that. The code for
the same can be found in the `WireHopper` class.

__Tasklets__: Tasklets is essentially a Task wrapper on top of graph nodes to make them operate
seamlessly with the Java async APIs. The tasklet'e `execute` method does the main share of the work.
For each node, it waits for all the upstream nodes to complete execution and then executes itself
and if the output is desired by one or many remote machines, it takes the help of wireHopper to send
them over.

__Flowunits__: A flow-unit is a wrapper around the data that travels between the graph nodes. A flow
unit contains informations such as the timestamp when the data was generated and the data itself in
a relation format (rows and columns of strings).

- __MetricFlow__: Used for transportation of the metrics between the `Metric` nodes and the RCA or
Symptom nodes.

- __ResourceFLowUnit__: This is an abstraction on the Resource Health that is passed between nodes.

__Resources__: As started, the RCA framework gives us the state of each resource. Now this
definition is incomplete unless we define which resources we account for. The resources are broken
into these layers:

- Network
	- TCP
- Hardware
	- CPU
	- Memory
	- Disks
	- NICs
- OS
	- Scheduler
	- Memory Manager
- JVM
	- Heap
	- Garbage Collector
	- JIT
- ElasticSearch
	- Indices
	- Shards
	- Locks
	- Queues
	- Threads

__Context__: The context gives us the detail why the resource was deemed unhealthy. So it might
contain the data like the current value and the threshold it was compared againt. A context can be
anything that helps us later, to understand why the resource was flagged as unhealthy.

__Thresholds__: Thresholds are static values against which we compare the current values to evaluate
the symptoms and the RCAs. Though the framework gives you full flexibility of the Java language, you
can set thresholds as constant members of the RCA/Symptom classes but putting them as threshold-json
has advantages: 

-  Changing the Class constants will require re-deploying the jar but the threshold file changes can
be picked up dynamically.

-  The threshold file let's you pick different threasholds based on instance type, disk types or an
arbitrary tags.

__Tags__: The RCA system does not understand ElasticSearch concepts such as data nodes and
master nodes. Tags are simple key-value pairs that are specified in the rca.conf. The RCA
scheduler, when it starts, reads it and assumes those to be its tags. When it evaluates the graph
nodes, if the tags of the graph node matches its own tags, it evaluates the nodes locally, if not,
it thinks this graph node is to be executed on some remote machine (remote graph node). If one such
remote node is upstream, then it send intent to consume its data. If such a node is downstream,
then, it sends the evaluated result of that graph node to all its subscribers. 

## Walkthrough of an RCA

[Link to the blog]

[Link to the design RFC]

## Building, Deploying, and Running the RCA Framework
Please refer to the [Install Guide](./INSTALL.md) for detailed information on building, installing and running the RCA framework.

## Current Limitations
* This is alpha code and is in development.
* We don't have 100% unit test coverage yet and will continue to add new unit tests. We invite developers from the larger Open Distro community to contribute and help improve test coverage and give us feedback on where improvements can be made in design, code and documentation.
* Currently we have tested and verified RCA artifacts for Docker images. We will be testing and verifying these artifacts for RPM and Debian builds as well.

## Code of Conduct

This project has adopted an [Open Source Code of Conduct](https://opendistro.github.io/for-elasticsearch/codeofconduct.html).


## Security Issue Notifications

If you discover a potential security issue in this project we ask that you notify AWS/Amazon Security via our [vulnerability reporting page](http://aws.amazon.com/security/vulnerability-reporting/). Please do **not** create a public GitHub issue.


## License

This library is licensed under the Apache 2.0 License.
