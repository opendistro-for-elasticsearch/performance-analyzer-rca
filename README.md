## Performance Analyzer RCA

The Performance Analyzer RCA is a framework that builds on the Performance Analyzer engine to
support Root Cause Analysis (RCA) of performance and reliability problems in Elasticsearch
clusters. This framework executes real time root cause analyses using Performance Analyzer
metrics. Root cause analysis can significantly improve operations, administration and
provisioning of Elasticsearch clusters, and it can enable Elasticsearch client teams to tune
their workloads to reduce errors.

## RCA Overview
The RCA framework is modeled as a distributed data-flow graph where data flows downstream 
from the leaf nodes to the root. Leaf nodes of the graph represent `Performance Analyzer metrics`
on which intermediate computations are performed. The intermediate nodes can be RCAs or other derived 
symptoms which helps in computation of the final RCA. The framework operates on a single analysis graph
which is composed of multiple connected-components. Each connected component contains one top-level RCA at its root.

### Terminologies

__DataStream a.k.a FlowUnit__: A flow-unit is a data object that is exchanged betweeen nodes. Each unit contains a timestamp and the data generated at that timestamp. Data flows downstream from the leaf nodes to the root.

__RCA__: An RCA is a function which operates on multiple datastreams. These datastreams can have zero or more metrics, symptoms or other RCAs. 

__Symptom__: A symptom is an intermediate function operating on metric or symptom datastreams. For example - A high CPU utilization symptom can calculate moving average over k samples and categorize CPU utilization(high/low) 
based on a threshold. Typically, output of a Symptom node will be binary - `presence`(true) or `absence`(false).


__Metrics__: Metric nodes query Performance Analyzer(PA) metrics and expose them as a continuous data stream to downstream nodes. They can also be extended to pull custom metrics from other data sources.


### Components

__Framework__: The RCA runtime operates on an `AnalysisGraph`. Extend this class and override the `construct` method if you wish to deploy new RCAs. Specify the path to the class in the `analysis-graph-implementor` section of `pa_config/rca.conf`.  The `addLeaf` and `addAllUpstreams` helper methods make it convenient to specify dependencies between nodes of the graph.


__Scheduler__: The scheduler executes the `operate` method on each graph node in topological order as defined in the analysis graph. Nodes with no dependency can be executed in parallel. Use flow units to share data between RCA nodes and avoid shared objects as these can cause data races and performance bottlenecks.

__WireHopper__: This is the networking-orchestrator for the framework. It abstracts out the fact
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

__Resources__: As stated, the RCA framework gives us the state of each resource. Now this

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
