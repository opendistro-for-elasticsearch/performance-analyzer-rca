/*
 * Copyright 2019 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */

package com.amazon.opendistro.elasticsearch.performanceanalyzer.rca.framework.api.persist;

import org.jooq.Field;

/**
 * This interface helps writing enums for the field in each RCA table (FlowUnit, summaries etc.)
 * We can call this getField method to read the field object directly without worrying about
 * casting the field's name and data type.
 */
public interface JooqFieldValue {
  String getName();

  Field getField();
}
