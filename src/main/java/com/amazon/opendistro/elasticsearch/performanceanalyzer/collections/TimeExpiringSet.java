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

package com.amazon.opendistro.elasticsearch.performanceanalyzer.collections;

import com.google.common.cache.Cache;
import com.google.common.cache.CacheBuilder;
import java.util.Iterator;
import java.util.concurrent.TimeUnit;
import java.util.function.Consumer;
import javax.annotation.Nonnull;

/**
 * Stores a set of elements which are automatically removed from the Set after a given time period.
 *
 * <p>Subsequent calls to add with the same element refresh the expiry period for that element.
 */
public class TimeExpiringSet<E> implements Iterable<E> {
  private Cache<E, E> cache;

  /**
   * Allocates a new TimeExpiringSet whose elements expire after the given time period
   * @param duration The magnitude of the expiry duration
   * @param unit The unit of the expiry duration
   */
  public TimeExpiringSet(long duration, TimeUnit unit) {
    cache = CacheBuilder.newBuilder()
        .expireAfterWrite(duration, unit)
        .build();
  }

  /**
   * Returns true if e is currently a member of the Set
   * @param e The element to tests
   * @return true if e is currently a member of the Set
   */
  public boolean contains(E e) {
    return cache.getIfPresent(e) != null;
  }

  /**
   * Returns the number of elements currently in the Set
   * @return the number of elements currently in the Set
   */
  public long size() {
    return cache.size();
  }

  /**
   * Returns a weakly-consistent, thread-safe {@link Iterator} over the elements in the Set
   *
   * <p>This means that while the Iterator is thread-safe, if elements expire after the Iterator is
   * created, the changes may not be reflected in the iteration. That is, you may iterate over an
   * element which was invalidated during your iteration. This is okay for many use cases which can
   * tolerate weak consistency.
   *
   * @return a weakly-consistent, thread-safe {@link Iterator} over the elements in the Set
   */
  @Nonnull
  public Iterator<E> iterator() {
    return cache.asMap().keySet().iterator();
  }

  /**
   * Adds an element into the Set
   * @param e the element to add into the Set
   */
  public void add(E e) {
    cache.put(e, e);
  }

  /**
   * Simple weakly-consistent forEach implementation applies the given action to each element
   * @param action The action to apply to each element
   */
  public void forEach(Consumer<? super E> action) {
    iterator().forEachRemaining(action);
  }
}
