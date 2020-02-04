# Health of the RCA framework

The classes in this package help you gather measurements such as latency of executions and
occurrences of events, aggregate them and then report them.

## Measurements

Measurements are defined in the  `stats/measurements`. They are the quantities we want to measure.
Each measurement has a statistic associated with it

### Aggregates
They do some calculations on the raw numbers without just reporting them. The supported
statistics are in the `stats/eval/impl`. New aggregates can be written and plugged in without
much effort. Currently supported statistics are:
 1. Count
 1. Max
 1. Min
 1. Mean
 1. NamedCounter
 1. Sample
 1. Sum

With max and min you can choose to send the value along with a key and then the statistic reports
the max or the min and also the key associated with it. The way we use it here is with
calculating the max and min of latencies for calling the operate on the graph nodes. We pass
the name of the graph as the key to it. Therefore, for each run we get not only the maximum
duration to execute `operate()` across all the nodes, but also the the name name of the node
which took the longest.
One other statistic that deserves special mention is the `NamedCounter` type. This also takes a
key with it and finally it gives the count of occurrences for each key. This can be handy in
cases like exception reporting. Instead of enlisting all the exceptions in the Measurement
beforehand, we just have a few, and we can use the key to specify the cause of it and get a
grouping over the common causes for which the exceptions are thrown.


### Samples
These are reported as they were witnessed, the raw numbers. Some of the examples are the JVM free
 space or the current number of live threads in the rca system.

## Emitters

Emitters emit certain measurements. They can be found in two places - scattered all around the
code to track execution latencies and the occurrence of errors or call to an expensive
ElasticSearch API. The second place is the `stats/emitters`. These are fired at every certain
interval.

## Aggregators

They collect the emitted metrics and may compute statistics on them if specified in the
measurement definition.

## Reporter
Reporter asks the Aggregators for the metrics collected so far and generate reports. This is
where the concept of formatters comes in. Reporting is currently handled by `StatsCollector` class.

### Formatters

Formatters are classes that implement the `Formatter`  interface. This is the language the
reporter and aggregators talk in. When the reporter asks for the metrics, it sends across a
formatter with it. The aggregator uses the formatter to format the measurements. Formatters are
located in `stat/format`. The formatter that comes with the repo might not suit your needs and
therefore, you might choose to write another formatter that formats the measurements in
accordance with your metric backend.

## Extending the measurements

If you want to add more measurements, then you can add them to one of the three categories or
 even create one of your own. The three categories we have, stores metrics related to:
 1. `RcaGraphMeasurements` : Measurements related to each run of the RCA graph.
 1. `RcaFrameworkMeasurements`: Measurements are concerned with the general workings of the
  framework outside of the graph execution.
 1. `ExceptionsAndErrors`: To get a count of the errors and exceptions thrown by the system.
 1. `LivenessMeasurements`: These are the list of system samples that are collected periodically
  rather than on occurrence of events.