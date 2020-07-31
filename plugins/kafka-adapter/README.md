### PA-RCA Kafka Adapter
***
The PA-RCA kafka Adpater is a configurable plugin that can periodically stream the RCA outputs and push them to a specific Kafka queue. RCA data in the kafka queue will be read by consumers that can them redirect the decisions to a bounded slack bot. All RCA data are then stored in a local elasticsearch cluster via the kafka-elasticsearch sink connector.


### Design RFC
***
RFC


### 