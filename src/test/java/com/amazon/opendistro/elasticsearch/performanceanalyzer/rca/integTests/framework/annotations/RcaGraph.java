/*
 * Copyright 2020 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 *  A copy of the License is located at
 *
 *       http://www.apache.org/licenses/LICENSE-2.0
 *
 *  or in the "license" file accompanying this file. This file is distributed
 *  on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 *  express or implied. See the License for the specific language governing
 *  permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.integTests.framework.annotations;

import java.lang.annotation.ElementType;
import java.lang.annotation.Retention;
import java.lang.annotation.RetentionPolicy;
import java.lang.annotation.Target;

/**
 * This annotation should be used to specify the name of the AnalysisGraph class that will be instantiated by the Runner.
 * One can use this at class level if the AnalysisGraph is the same for all the test methods in the class or at a method
 * level to override the one at the class level.
 * All the parameters of this annotation are required.
 */

@Retention(RetentionPolicy.RUNTIME)
@Target({ElementType.TYPE, ElementType.METHOD})
public @interface RcaGraph {
  Class analysisGraphClass();
  RcaConfAnnotation rcaConfElectedMaster();
  RcaConfAnnotation rcaConfStadByMaster();
  RcaConfAnnotation rcaConfDataNode();
}
