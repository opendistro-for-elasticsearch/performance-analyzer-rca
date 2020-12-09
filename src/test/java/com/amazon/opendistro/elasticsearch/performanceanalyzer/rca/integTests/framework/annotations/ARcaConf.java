package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.configs.Consts;
import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation can be used to specify an rca.conf file. Usually tests don't need to provide the rca.conf
 * therefore, it uses the rca.conf* files in the test/resources as defaults.
 */
@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface ARcaConf {
  // full path to the rca.conf file to be used by elected master node.
  String electedMaster() default Consts.RCAIT_DEFAULT_RCA_CONF_ELECTED_MASTER_NODE;

  // full path to the rca.conf file to be used by the standby master.
  String standBy() default Consts.RCAIT_DEFAULT_RCA_CONF_STANDBY_MASTER_NODE;

  // full path to the rca.conf file to be used by the data node.
  String dataNode() default Consts.RCAIT_DEFAULT_RCA_CONF_DATA_NODE;

  enum Type {
    ELECTED_MASTER,
    STANDBY_MASTER,
    DATA_NODES
  }
}
