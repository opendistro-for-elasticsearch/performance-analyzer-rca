package com.amazon.opendistro.elasticsearch.performanceanalyzer.store.rca;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.flow_units.ResourceFlowUnit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.MetricTestHelper;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.rca.HighHeapUsageYoungGenRca;

import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import org.junit.runner.RunWith;
import org.mockito.Mockito;
import org.powermock.api.mockito.PowerMockito;
import org.powermock.core.classloader.annotations.PowerMockIgnore;
import org.powermock.core.classloader.annotations.PrepareForTest;
import org.powermock.modules.junit4.PowerMockRunner;

@RunWith(PowerMockRunner.class)
@PrepareForTest({HighHeapUsageYoungGenRca.class})
@PowerMockIgnore({"com.sun.org.apache.xerces.*", "javax.xml.*", "org.xml.*", "org.w3c.*", "com.sun.org.apache.xalan.*"})
public class HighHeapUsageYoungGenRcaTest {
  private static final double CONVERT_BYTES_TO_MEGABYTES = Math.pow(1024, 2);
  private static final int RCA_PERIOD = 12;
  private MetricTestHelper heap_Used;
  private MetricTestHelper gc_Collection_Time;
  private HighHeapUsageYoungGenRca youngGenRca;
  private List<String> columnName;

  private void mockFlowUnits(int timeStampInSecond, double heapUsageVal, double gcCollectionTimeVal) {
    Mockito.when(System.currentTimeMillis()).thenReturn(TimeUnit.SECONDS.toMillis(timeStampInSecond));
    //generate empty flowunit and run operate enough times before evaluating RCA
    heap_Used.setEmptyFlowUnitList();
    gc_Collection_Time.setEmptyFlowUnitList();
    for (int i = 0; i < RCA_PERIOD-1; i++) {
      youngGenRca.operate();
    }
    //generate flowunit
    heap_Used.createTestFlowUnits(columnName, Arrays.asList("OldGen", String.valueOf(heapUsageVal*CONVERT_BYTES_TO_MEGABYTES)));
    gc_Collection_Time.createTestFlowUnits(columnName, Arrays.asList("totYoungGC", String.valueOf(gcCollectionTimeVal)));
  }

  @Before
  public void initTestHighHeapYoungGenRca() {
    heap_Used = new MetricTestHelper(5);
    gc_Collection_Time = new MetricTestHelper(5);
    youngGenRca = new HighHeapUsageYoungGenRca(5, heap_Used, gc_Collection_Time);
    columnName = Arrays.asList("MemType", "max");
    PowerMockito.mockStatic(System.class);
  }

  @Test
  public void testHighHeapYoungGenRca() {
    ResourceFlowUnit flowUnit;
    //ts = 0, heap = 0, gc time = 0
    mockFlowUnits(0, 0, 0);
    flowUnit = youngGenRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 1, heap = 450MB, gc time = 200ms
    mockFlowUnits(1, 450, 200);
    flowUnit = youngGenRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 2, heap = 1050MB, gc time = 400ms
    mockFlowUnits(2, 1050, 400);
    flowUnit = youngGenRca.operate();
    Assert.assertFalse(flowUnit.getResourceContext().isUnhealthy());

    //ts = 3, heap = 1550MB, gc time = 650ms
    mockFlowUnits(3, 1550, 650);
    flowUnit = youngGenRca.operate();
    Assert.assertTrue(flowUnit.getResourceContext().isUnhealthy());
  }

}
