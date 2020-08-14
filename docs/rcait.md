# Rca Integration test framework

## Scope
To be able to test scenarios where multiple RCA Schedulers are running on different hosts of a
cluster and be able to inject a custom RCA graph, specify the metrics that should flow through
the rca-dataflow-graph and then be able to test the results of the RCAs and decisions based on
the RCAs by either querying the rca.sqlite files on a particular host or by hitting the REST
endpoint on a particular host.

## Out of Scope
This framework will not facilitate testing the PerformanceAnalyzer Reader component, writer
component and how PerformanceAnalyzer interacts with ElasticSearch. 

## How to write your own tests using this framework ?
The RCA-IT is composed of various annotatons that you can use to configure the 
test environment you want your tests to run on.

Annotation|Required|Level|Parameters|Repeatable|Description
----------|-----|----------|----------|---------|------
RunWith|Yes|Class|Runner class|No|This is to suggest the junit that something other than the default runner must be used
Category|Yes|Class|RcaItMarker.class|No|This is to categorize all the RCA-IT tests so that all integration tests can be run by using the gradle target `rcaIt`
AClusterType|Yes|Class|value|No|The cluster type that would be used to run this integration test. Types supported can be found in `enum ClusterType`
ARcaGraph|Yes|Class or Method|value|No|The RCA graph that will be executed by the RCA scheduler.
AMetric|Yes|Class or Method|name, dimensionNames, tables|Yes|The metrics that will flow through the edges of the RCA graph. This is probably the most complicated annotation and is better understood with an example. Please checkout `class RcaItPocDedicated`.
Expect|No|Method|what, on, timeoutSeconds, validator, forRca|Yes|If you want the framework to test for an expected condition at steady state to pass or fail a test.
AErrorPatternIgnored|No|Method|pattern, reason|Yes|If you want the checking framework to bypass certain pattern in error logs.

__@RunWith(RcaItNotEncryptedRunner.class)__

The above specifies the runner for the junit test class and in this case, it says to junit
to offload it to one of the RCA-IT runners - _RcaItNotEncryptedRunner_. All RCA-IT tests must
use this annotation for them to be run by this integ test framework.

__@Category(RcaItMarker.class)__

All test classes must also be marked with this masker interface.
    
__@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)__

This annotation tells the RCA-IT to use `a multi-node cluster with no dedicated master nodes
`. The kinds of clusters supported today are:   `SINGLE_NODE`, `MULTI_NODE_CO_LOCATED_MASTER
` and `MULTI_NODE_DEDICATED_MASTER`. This is a required annotation and must be specified at
 the class level.

__@ARcaGraph(MyRcaGraph.class)__

This helps us specify the Graph that we will be using for this test. It can be a graph that
exists or the one specially contrived for this test.
    
__@AMetric__ 
This helps us specify the metrics that will be pured over the RCA graph. It has multiple sub-fields.

Let's see an example:
```java
@AMetric(name = CPU_Utilization.class,
    dimensionNames = {SHARDID_VALUE, INDEX_NAME_VALUE, OPERATION_VALUE, SHARD_ROLE_VALUE},
    tables = {
        @ATable(hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(dimensionValues = {"0", "logs", "bulk", "p"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 0.0),
                @ATuple(dimensionValues = {"1", "logs", "bulk", "r"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 80.0),
                @ATuple(dimensionValues = {"2", "logs", "bulk", "p"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 10.0)
            }
        ),
        @ATable(hostTag = HostTag.DATA_1,
            tuple = {
                @ATuple(dimensionValues = {"0", "logs", "bulk", "r"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 50.0),
                @ATuple(dimensionValues = {"1", "logs", "bulk", "p"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 5.0),
                @ATuple(dimensionValues = {"2", "logs", "bulk", "r"},
                    sum = 0.0, avg = 0.0, min = 0.0, max = 11.0)
            }
        )
    }
)
```

- _name_ : The Metric graph node that will be emitting these values. The expected parameter is one of the metrics classes
 in `com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics`. The
 metrics class that you specify, should have a `static final` field called `NAME` (`CPU_Utilization`)
 and that will be used to determine the name of the metric table.
- _dimensionNames_ : For the dimension names for a metric, please refer to the docs 
[here](https://opendistro.github.io/for-elasticsearch-docs/docs/pa/reference/). Each of the dimension
will be a column in the rca.sqlite table.
- _tables_ : This specifies one or more table(s). The table should be a 5 second snapshot
 of the metrics, similar to what exists in metricsdb files. The table is an array type
 , therfore it gives you the flexibility of specifying a different metrics table for
 each node or a group of nodes in the cluster. In the above example, the table one is to be emitted by
 `DATA_0` and table 2 by `DATA_1`. This can be used to push different metrics to the node that
 we want to be marked unhealthy vs all other nodes in the cluster.
    - _hostTag_ :  On which cluster node should emit this metric.
    - _tuple_ : This is an array type that can be used to specify the rows in the table. A
     row should be an (n+4)-tuple where n is a value per dimension of this metric and 4 represents 
     the four aggregate columns - `min`, `max`, `sum` and `avg`.

__@Expect__

This is an optional annotation that can be used only at a method level. This provides an easy
way to validate the result of the test. The annotation has 4 sub-fields:
- what : What are we testing for - data in rca.sqlite file or the response returned by the
 rest API.
- on : On which node should the framework look for, for the expected data.
- forRca : Which particular RCA's value are we testing.
- validator : This is the class that should be defined by the test writer and should
 implement `IValidator` interface. Once the framework gathers the data for the mentioned RCA
 from the given node, the data will be passed to the validator which returns if the check
  passed or not.
    
The Expect annotation is a repeatable type. Therefore, you can expect multiple things from
the test at steady-state. So you can have two expectations one for the RCA on data node and
the other on the master. If the expectations are not true for the ith iteration, then the
framework, will re-run them for the i+1 the iteration till a timeout. The timeout is
configurable to any number of seconds using the field `timeoutSeconds` but has the default
of 60 seconds. 
    
__@AErrorPatternIgnored__
The RCA-IT will fail a test if any error is logged. If you think that some of the errors logged
during your test is okay, then you can use this annotation to explicitly ignore them. The
annotation takes two parameters - `pattern` the pattern to ignore and `reason` stating why it
would be okay to ignore such an error. The reason is most for the reader of the test.

A test class can get access to the programmaticAPI to get information about hosts in the cluster
or a particular host then the test class can declare a method with name `setTestApi(final TestApi api)`
and the test runner will call this setter to give a reference of the TestApi to the testClass.

### Cluster Types
The integration test framework let's us create three kinds of clusters as mentioned above. But
you need to know the hostTags to pick a host that would publish certain metrics or its the
hostTag by which you identify a host to make a REST request to. This section tells you what are
the different hostTags available for different cluster types.

__Dedicated Master Cluster__

This cluster type is composed of three master nodes and 2 data nodes.

Host Ids | Host Tags
---------|----------
0 | HostTag.ELECTED_MASTER
1 | HostTag.STANDBY_MASTER_0
2 | HostTag.STANDBY_MASTER_1
3 | HostTag.DATA_0
4 | HostTag.DATA_1

__ Co-located Master Cluster __
A co-located master cluster is one where all the data-nodes are master eligible. In the
test framework, this is composed of two nodes.

Host IDs | Host Tags
---------|----------
0 | HostTag.ELECTED_MASTER
1 | HostTag.DATA_0


__ Single Node Cluster__
There is just one node in a single node cluster which is tagged as `HostTag.DATA_0`.

Host IDs | Host Tags
---------|----------
0 | HostTag.DATA_0

### Examples

Some of the examples for how to write the integ tests can be found here:
`src/test/java/com/amazon/opendistro/elasticsearch/performanceanalyzer/rca/integTests/tests/poc/*`

### Running the integ tests
1. All integration tests written using this framework can be run by using the command
`./gradlew rcaIt`

2. If you want to all the tests in an integ test class, you can do something like this:
`./gradlew test --tests RcaItPocDedicated`

3. If you want to run a particular test method in your test class, you can use something like this:
`./gradlew test --tests RcaItPocDedicated.simple`

## Framework deep dive.
This section might be of interest to you if you are trying to enhance the test framework itself
. If you just want to add more integration tests, then you may choose to skip this section.

The framework consists of four main classes:
1. `RcaItRunnerBase` : This is the JUnit Runner that will be used to run all rca-it tests. It
 orchestrates the environment creation, initializing the test class and then executing the methods
 annotated with `@Test`.

2. `TestEnvironment` : The RCA-IT environment is defined by the RCA graph it will be running, the
 metrics that will flow through the dataflow pipelines and the rca.conf that will be used by the
 hosts.

3. `Cluster` and `Host` classes: These class initializes multiple RCAController(s) threads,
each of them represent RCAFramework running on multiple nodes. In constructors, we create all the
objects and create a directory per host where they will dump the _rca.sqlite_ and _rca_enabled_
files. In the second phase a call to `createServersAndThreads` is made which creates all the http
and grpc servers (one per host). Then we start the RCAcontroller thread.
