# Batch Metrics API

## Background and Problem statement
The metrics API currently allows us to collect the past 5 seconds of performance data from an ODFE cluster. However, this data is aggregated, not raw, performance metrics. Additionally, it is restricting that we only have access to the past 5 seconds of data, rather than data from longer periods of time. Having fine granularity (raw) metrics from longer periods of time would allow users to learn a lot more about the performance of an AES cluster.

## Design
First, we would like to expand the retention period from beyond 5s. Currently, each cluster retains the past 5s of performance data in the form of a sqlite database (metricsdb files). We can naively expand the retention period by simply retaining more of these databases and allowing users to query from them.

![Retaining more metricsdb files](/docs/images/batch-metrics-api-1.png)

*Figure 1. Retaining more metricsdb files*

The second issue is the limited granularity of the data. We can simply expand the granularity by allowing users to query the raw data for each metric rather than responding with an aggregate of that data. One drawback of this approach is that the response size will drastically increase. This prevents us from supplying a “nodes=all” parameter like with the metrics API, as it would require a single node to gather that data from all the other nodes and retain that data in memory before responding to the user. So, users will have to query individual nodes in order to gather performance metrics from those nodes.

All the raw metrics data from a period of time may be too high granularity for some users. One approach to limiting this granularity might be to respond with some aggregate of that data, like with the metrics API. However, a more compute-efficient, practical, and useful approach would be to instead respond with a sample of that raw data. The available datapoints are currently in the form of 5s granularity bins (each metricsdb file collects 5s of data). We can efficiently sample from these bins by providing users with a “samplingperiod” parameter. We would partition the available data according to the sampling period and simply respond with the data from the first bin in each partition.

![Sampling from bins](/docs/images/batch-metrics-api-2.png)

*Figure 2. Sampling from bins*

## API

**performance-analyzer.properties**
* retention-period: The number of minutes worth of metrics data to collect. The configured retention period can be read via the “/_opendistro/performanceanalyzer/batch/config” api. Default=7 (7 minutes), min=1, max = 60.
  * Note, the default is 7 minutes because a typical use-case would be to query for 5 minutes worth of data from the node. In order to do this, a client would actually select a starttime of now-6min and an endtime of now-1min (this one minute offset will give sufficient time for the metrics in the time range to be available at the node). Atop this 6 minutes of retention, we need an extra 1 minute of retention to account for the time that would have passed by the time the query arrives at the node, and for the fact that starttime and endtime will be rounded down to the nearest samplingperiod.

**API**

Queries:

* POST \<endpoint\>:9200/_opendistro/performanceanalyzer/batch/config -H ‘Content-Type: application/json’ -d ‘{“enabled”: true}’
* POST \<endpoint\>:9200/_opendistro/performanceanalyzer/batch/cluster/config -H ‘Content-Type: application/json’ -d ‘{“enabled”: true}’
* GET \<endpoint\>:9200/_opendistro/_performanceanalyzer/_agent/batch?metrics=\<metrics\>&starttime=\<starttime\>&endtime=\<endtime\>&samplingperiod=\<samplingperiod\>
* GET \<endpoint\>:9600/_opendistro/_performanceanalyzer/batch?metrics=\<metrics\>&starttime=\<starttime\>&endtime=\<endtime\>&samplingperiod=\<samplingperiod\>

Parameters:
* metrics - Comma separated list of metrics you are interested in. For a full list of metrics, see Metrics Reference.
* starttime - Unix timestamp (difference between the current time and midnight, January 1, 1970 UTC) in milliseconds determining the oldest data point to return. starttime is inclusive — data points from at or after the starttime will be returned. Note, the starttime and endtime supplied by the user will both be rounded down to the nearest samplingperiod. starttime must be no less than `now - retention_period` and it must be less than the endtime (after the rounding).
* endtime - Unix timestamp in milliseconds determining the freshest data point to return. endtime is exclusive — only datapoints from before the endtime will be returned. endtime must be no greater than the system time at the node, and it must be greater than the startime (after being rounded down to the nearest samplingperiod).
* samplingperiod - Optional parameter indicating the sampling period in seconds (default is 5s). The requested time range will be partitioned according to the sampling period, and data from the first available 5s interval in each partition will be returned to the user. Must be at least 5s, must be less than the retention period, and must be a multiple of 5.

Note, the maximum number of datapoints that a single query can request for via API is capped at 100,800 datapoints. If a query exceeds this limit, an error is returned. Parameters like the starttime, endtime, and samplingperiod can be adjusted on such queries to request for fewer datapoints at a time.

Sample Query:

GET localhost:9600/_opendistro/_performanceanalyzer/batch?metrics=CPU_Utilization,IO_TotThroughput&starttime=1594412250000&endtime=1594412260000&samplingperiod=5

Output
```
{
    "1594412250000": {
        "CPU_Utilization": {
            "fields": [
                {
                    "name": "ShardID",
                    "type": "VARCHAR"
                },
                {
                    "name": "IndexName",
                    "type": "VARCHAR"
                },
                {
                    "name": "Operation",
                    "type": "VARCHAR"
                },
                {
                    "name": "ShardRole",
                    "type": "VARCHAR"
                },
                {
                    "name": "sum",
                    "type": "DOUBLE"
                },
                {
                    "name": "avg",
                    "type": "DOUBLE"
                },
                {
                    "name": "min",
                    "type": "DOUBLE"
                },
                {
                    "name": "max",
                    "type": "DOUBLE"
                }
            ],
            "records": [
                [
                    null,
                    null,
                    "GC",
                    null,
                    0.0039980247804126965,
                    0.0039980247804126965,
                    0.0039980247804126965,
                    0.0039980247804126965
                ],
                [
                    null,
                    null,
                    "other",
                    null,
                    0.0,
                    0.0,
                    0.0,
                    0.0
                ]
            ]
        },
        "IO_TotThroughput": {
            "fields": [
                {
                    "name": "IndexName",
                    "type": "VARCHAR"
                },
                {
                    "name": "ShardID",
                    "type": "VARCHAR"
                },
                {
                    "name": "ShardRole",
                    "type": "VARCHAR"
                },
                {
                    "name": "Operation",
                    "type": "VARCHAR"
                },
                {
                    "name": "sum",
                    "type": "DOUBLE"
                },
                {
                    "name": "avg",
                    "type": "DOUBLE"
                },
                {
                    "name": "min",
                    "type": "DOUBLE"
                },
                {
                    "name": "max",
                    "type": "DOUBLE"
                }
            ],
            "records": [
                [
                    null,
                    null,
                    null,
                    "other",
                    1175.8217964071857,
                    1175.8217964071857,
                    1175.8217964071857,
                    1175.8217964071857
                ]
            ]
        }
    },
    "1594412255000": {
        "CPU_Utilization": {
            "fields": [
                {
                    "name": "ShardID",
                    "type": "VARCHAR"
                },
                {
                    "name": "IndexName",
                    "type": "VARCHAR"
                },
                {
                    "name": "Operation",
                    "type": "VARCHAR"
                },
                {
                    "name": "ShardRole",
                    "type": "VARCHAR"
                },
                {
                    "name": "sum",
                    "type": "DOUBLE"
                },
                {
                    "name": "avg",
                    "type": "DOUBLE"
                },
                {
                    "name": "min",
                    "type": "DOUBLE"
                },
                {
                    "name": "max",
                    "type": "DOUBLE"
                }
            ],
            "records": [
                [
                    null,
                    null,
                    "GC",
                    null,
                    0.0039980247804126965,
                    0.0039980247804126965,
                    0.0039980247804126965,
                    0.0039980247804126965
                ]
            ]
        },
        "IO_TotThroughput": {
            "fields": [
                {
                    "name": "IndexName",
                    "type": "VARCHAR"
                },
                {
                    "name": "ShardID",
                    "type": "VARCHAR"
                },
                {
                    "name": "ShardRole",
                    "type": "VARCHAR"
                },
                {
                    "name": "Operation",
                    "type": "VARCHAR"
                },
                {
                    "name": "sum",
                    "type": "DOUBLE"
                },
                {
                    "name": "avg",
                    "type": "DOUBLE"
                },
                {
                    "name": "min",
                    "type": "DOUBLE"
                },
                {
                    "name": "max",
                    "type": "DOUBLE"
                }
            ],
            "records": []
        }
    }
}
```
