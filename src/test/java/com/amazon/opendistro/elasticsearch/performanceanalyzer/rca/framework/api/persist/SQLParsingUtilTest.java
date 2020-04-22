package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist;

import static org.junit.Assert.assertEquals;
import static org.mockito.ArgumentMatchers.anyString;
import static org.mockito.ArgumentMatchers.eq;
import static org.mockito.Mockito.when;

import com.google.common.collect.Lists;
import org.jooq.Field;
import org.jooq.Record;
import org.jooq.Result;
import org.jooq.exception.DataTypeException;
import org.junit.Before;
import org.junit.Test;
import org.mockito.Mock;
import org.mockito.MockitoAnnotations;

public class SQLParsingUtilTest {
    private static final String FIELD_NAME = "FIELD_NAME";
    private static final String DATA_FIELD = "DATA_FIELD";

    @Mock
    private Result<Record> result;

    @Mock
    private Field<String> field;

    @Mock
    private Record record;

    @Before
    public void setup() {
        MockitoAnnotations.initMocks(this);
    }

    @Test
    public void testReadDataFromSqlResult() {
        // method should return Double.NaN when result is null
        assertEquals(Double.NaN, SQLParsingUtil.readDataFromSqlResult(null, field, FIELD_NAME, DATA_FIELD), 0);

        // method should return Double.NaN when it encounters an IllegalArgumentException
        when(result.getValues(field)).thenReturn(Lists.newArrayList("no", "matches", "here"));
        assertEquals(Double.NaN, SQLParsingUtil.readDataFromSqlResult(result, field, FIELD_NAME, DATA_FIELD), 0);

        // method should return Double.NaN when it encounters an DataTypeException
        when(result.getValues(field)).thenReturn(Lists.newArrayList("no", FIELD_NAME, "nope"));
        when(result.get(1)).thenReturn(record);
        when(record.getValue(anyString(), eq(Double.class))).thenThrow(new DataTypeException("EXCEPTION"));
        assertEquals(Double.NaN, SQLParsingUtil.readDataFromSqlResult(result, field, FIELD_NAME, DATA_FIELD), 0);

        // method should return the matched record value when all parameters are valid
        when(record.getValue(anyString(), eq(Double.class))).thenReturn(Math.E);
        assertEquals(Math.E, SQLParsingUtil.readDataFromSqlResult(result, field, FIELD_NAME, DATA_FIELD), 0);
    }
}
