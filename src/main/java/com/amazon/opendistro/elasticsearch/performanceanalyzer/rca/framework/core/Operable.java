package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.core;


import java.util.List;
import java.util.Map;

/*
 * In order to operate an Operable, the input multiple streams and the output is one single stream. Each of the
 * dependencies will have multiple samples and there are expected to be multiple dependencies. Hence, the input type
 * is list of lists.
 */
public interface Operable {
    <T extends GenericFlowUnit> T operate();
}
