package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator;

import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.Constants.INDEX_NAME;
import static com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.cache_tuning.Constants.SHARD_ID;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCInfoDimension;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.GCType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.metrics.AllMetrics.HeapDimension.Constants;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_FieldData_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Max_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Eviction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Hit;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Cache_Request_Size;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Collection_Event;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.GC_Type;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Max;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.metrics.Heap_Used;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.RcaItMarker;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AExpect.Type;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.AMetric;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaConf;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ARcaGraph;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATable;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations.ATuple;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.ClusterType;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.HostTag;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.runners.RcaItNotEncryptedRunner;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator.validator.CollatorAlignedValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.collator.validator.CollatorMisalignedValidator;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.tests.jvmsizing.JvmSizingITConstants;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.persistence.actions.PersistedAction;
import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.store.ElasticSearchAnalysisGraph;
import org.junit.Test;
import org.junit.experimental.categories.Category;
import org.junit.runner.RunWith;

@Category(RcaItMarker.class)
@RunWith(RcaItNotEncryptedRunner.class)
@AClusterType(ClusterType.MULTI_NODE_CO_LOCATED_MASTER)
@ARcaGraph(ElasticSearchAnalysisGraph.class)
@ARcaConf(dataNode = CollatorITConstants.RCA_CONF_PATH + "rca.conf", electedMaster =
    CollatorITConstants.RCA_CONF_PATH + "rca_master.conf")
@AMetric(
    name = Heap_Max.class,
    dimensionNames = {Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 1000000000.0, avg = 1000000000.0, min = 1000000000.0, max = 1000000000.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 1000000000.0, avg = 1000000000.0, max = 1000000000.0, min = 1000000000.0
                )
            }
        )
    }
)
@AMetric(
    name = Heap_Used.class,
    dimensionNames = {Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 950000000.0, avg = 950000000.0, min = 950000000.0, max = 950000000.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE},
                    sum = 950000000.0, avg = 950000000.0, min = 950000000.0, max = 950000000.0
                )
            }
        )
    }
)
@AMetric(
    name = GC_Collection_Event.class,
    dimensionNames = {Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.TOT_FULL_GC_VALUE},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        )
    }
)
@AMetric(
    name = GC_Type.class,
    dimensionNames = {GCInfoDimension.Constants.MEMORY_POOL_VALUE,
        GCInfoDimension.Constants.COLLECTOR_NAME_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE, JvmSizingITConstants.CMS_COLLECTOR},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {GCType.Constants.OLD_GEN_VALUE, JvmSizingITConstants.CMS_COLLECTOR},
                    sum = 10.0, avg = 10.0, max = 10.0, min = 10.0
                )
            }
        )
    }
)
@AMetric(
    name = Cache_FieldData_Size.class,
    dimensionNames = {
        AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
        AllMetrics.CommonDimension.Constants.SHARDID_VALUE
    },
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {INDEX_NAME, SHARD_ID},
                    sum = 8500.0, avg = 8500.0, min = 8500.0, max = 8500.0
                )
            }
        )
    }
)
@AMetric(
    name = Cache_FieldData_Eviction.class,
    dimensionNames = {
        AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
        AllMetrics.CommonDimension.Constants.SHARDID_VALUE
    },
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {INDEX_NAME, SHARD_ID},
                    sum = 1.0, avg = 1.0, min = 1.0, max = 1.0)
            }
        )
    }
)
@AMetric(
    name = Cache_Request_Size.class,
    dimensionNames = {
        AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
        AllMetrics.CommonDimension.Constants.SHARDID_VALUE
    },
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {INDEX_NAME, SHARD_ID},
                    sum = 100.0, avg = 100.0, min = 100.0, max = 100.0)
            }
        )
    }
)
@AMetric(
    name = Cache_Request_Eviction.class,
    dimensionNames = {
        AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
        AllMetrics.CommonDimension.Constants.SHARDID_VALUE
    },
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {INDEX_NAME, SHARD_ID},
                    sum = 1.0, avg = 1.0, min = 1.0, max = 1.0)
            }
        )
    }
)
@AMetric(
    name = Cache_Request_Hit.class,
    dimensionNames = {
        AllMetrics.CommonDimension.Constants.INDEX_NAME_VALUE,
        AllMetrics.CommonDimension.Constants.SHARDID_VALUE
    },
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {INDEX_NAME, SHARD_ID},
                    sum = 1.0, avg = 1.0, min = 1.0, max = 1.0)
            }
        )
    }
)
@AMetric(
    name = Cache_Max_Size.class,
    dimensionNames = {AllMetrics.CacheConfigDimension.Constants.TYPE_VALUE},
    tables = {
        @ATable(
            hostTag = HostTag.DATA_0,
            tuple = {
                @ATuple(
                    dimensionValues = {AllMetrics.CacheType.Constants.FIELD_DATA_CACHE_NAME},
                    sum = 10000.0, avg = 10000.0, min = 10000.0, max = 10000.0),
                @ATuple(
                    dimensionValues = {AllMetrics.CacheType.Constants.SHARD_REQUEST_CACHE_NAME},
                    sum = 100.0, avg = 100.0, min = 100.0, max = 100.0)
            }
        ),
        @ATable(
            hostTag = HostTag.ELECTED_MASTER,
            tuple = {
                @ATuple(
                    dimensionValues = {AllMetrics.CacheType.Constants.FIELD_DATA_CACHE_NAME},
                    sum = 10000.0, avg = 10000.0, min = 10000.0, max = 10000.0),
                @ATuple(
                    dimensionValues = {AllMetrics.CacheType.Constants.SHARD_REQUEST_CACHE_NAME},
                    sum = 100.0, avg = 100.0, min = 100.0, max = 100.0)
            }
        )
    }
)
public class CollatorIT {

  @Test
  @AExpect(
      what = Type.REST_API,
      on = HostTag.ELECTED_MASTER,
      validator = CollatorAlignedValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 190
  )
  public void testCollatorAligned() {
  }

  @Test
  @AExpect(
      what = Type.REST_API,
      on = HostTag.ELECTED_MASTER,
      validator = CollatorMisalignedValidator.class,
      forRca = PersistedAction.class,
      timeoutSeconds = 190
  )
  public void testCollatorMisaligned() {
  }
}
