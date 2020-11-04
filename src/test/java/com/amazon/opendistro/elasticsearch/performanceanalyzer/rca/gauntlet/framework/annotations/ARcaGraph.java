package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.gauntlet.framework.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This can be used to specify the RCA graph to be used by all nodes of the cluster.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface ARcaGraph {
  Class value();
}
