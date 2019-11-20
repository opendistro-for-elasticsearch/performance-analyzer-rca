package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metricsdb.MetricsDB;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.reader.ReaderMetricsProcessor;

public class MetricsDBProvider implements Queryable {
    private static final Logger LOG = LogManager.getLogger(MetricsDBProvider.class);

    @Override
    public MetricsDB getMetricsDB() throws Exception {
        ReaderMetricsProcessor processor = ReaderMetricsProcessor.getInstance();
        if (processor == null) {
            LOG.error("RCA: ReaderMetricsProcessor not initialized");
            throw new Exception("ReaderMetricsProcessor not initialized");
        }
        Map.Entry<Long, MetricsDB> dbEntry = processor.getMetricsDB();
        if (dbEntry == null) {
            LOG.error("RCA: MetricsDB not initialized");
            throw new Exception("Metrics DB not initialized");
        }
        return dbEntry.getValue();
    }

    @Override
    public List<List<String>> queryMetrics(MetricsDB db, String metricName) {
        // LOG.info("RCA: About to get the query metric: {}", metricName);
        try {
            Result<Record> queryResult = db.queryMetric(metricName);
            return parseResult(queryResult);
        } catch (Exception e) {
            LOG.error("kak: Having issues with the DB man! {}", e.getMessage());
            e.printStackTrace();
            return Collections.emptyList();
        }
        // LOG.info("RCA: result {}", queryResult);

    }

    private List<List<String>> parseResult(final Result<Record> queryResult) {
        List<List<String>> retResults = new ArrayList<>();
        List<String> columnNames =
                Arrays.stream(queryResult.fields()).map(Field::getName).collect(Collectors.toList());
        retResults.add(columnNames);

        for (Record r : queryResult) {
            List<String> row = new ArrayList<>();
            for (String col: columnNames) {
                row.add(String.valueOf(r.getValue(col)));
            }
            // LOG.info("RCA: row: \n{}", r);
            retResults.add(row);
        }
        return retResults;
    }

    @Override
    public List<List<String>> queryMetrics(final MetricsDB db, final String metricName, final String dimension, final String aggregation) {
        try {
            return parseResult(db.queryMetric(Collections.singletonList(metricName), Collections.singletonList(aggregation),
                    Collections.singletonList(dimension)));
        } catch (Exception e) {
            LOG.error("Couldn't query the metrics correctly. {}", e.getMessage());
            LOG.error("Exception: ", e);
        }

        return Collections.emptyList();
    }


    @Override
    public long getDBTimestamp(MetricsDB db) {
        return 0;
    }
}

