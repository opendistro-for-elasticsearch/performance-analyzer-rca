package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.annotations;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.configs.ClusterType;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This is a class level annotation that must be present for each of the RCAIt
 * test classes. This specifies the cluster type - single node vs multi-node
 * with dedicated master vs multi-node with co-located master.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target(ElementType.TYPE)
public @interface AClusterType {
  ClusterType value();
}
