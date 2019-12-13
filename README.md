## Performance Analyzer RCA

The Performance Analyzer RCA is a framework that builds
on the Performance Analyzer engine to support Root
Cause Analysis (RCA) of performance and reliability
problems in Elasticsearch clusters. This framework
executes real time root cause analyses using Performance Analyzer
metrics. Root cause analysis can significantly improve
operations, administration and provisioning of
Elasticsearch clusters, and it can enable Elasticsearch
client teams to tune their workloads to reduce errors.

## Overview
The RCA framework is a distributed data-flow graph where the data enters from
the top as Performance Analyzer metrics referred to as Leaf nodes in the code,
the intermediate nodes perform calculations and out comes the RCA.

## License

This library is licensed under the Apache 2.0 License.
