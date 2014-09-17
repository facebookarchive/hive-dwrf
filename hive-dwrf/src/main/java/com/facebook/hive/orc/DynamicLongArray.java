//  Copyright (c) 2013, Facebook, Inc.  All rights reserved.

/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.facebook.hive.orc;

import org.apache.hadoop.hive.ql.io.slice.SizeOf;
import org.apache.hadoop.hive.ql.io.slice.Slice;
import org.apache.hadoop.hive.ql.io.slice.Slices;

/**
 * Dynamic int array that uses primitive types and chunks to avoid copying
 * large number of integers when it resizes.
 *
 * The motivation for this class is memory optimization, i.e. space efficient
 * storage of potentially huge arrays without good a-priori size guesses.
 *
 * The API of this class is between a primitive array and a AbstractList. It's
 * not a Collection implementation because it handles primitive types, but the
 * API could be extended to support iterators and the like.
 *
 * NOTE: Like standard Collection implementations/arrays, this class is not
 * synchronized.
 */
final class DynamicLongArray extends DynamicArray {
  static final int DEFAULT_SIZE = SizeOf.SIZE_OF_LONG * 8 * 1024;

  public DynamicLongArray(MemoryEstimate memoryEstimate) {
    this(DEFAULT_SIZE, memoryEstimate);
  }

  public DynamicLongArray(int size, MemoryEstimate memoryEstimate) {
    super(size, memoryEstimate, SizeOf.SIZE_OF_LONG, DEFAULT_SIZE);
  }

  public long get(int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("Index " + index +
                                            " is outside of 0.." +
                                            (length - 1));
    }

    return data.getLong(index * SizeOf.SIZE_OF_LONG);
  }

  public void set(int index, long value) {
    grow(index);
    if (index >= length) {
      length = index + 1;
    }

    data.setLong(index * SizeOf.SIZE_OF_LONG, value);
  }

  public void increment(int index, long value) {
    grow(index);
    if (index >= length) {
      length = index + 1;
    }

    data.setLong(index * SizeOf.SIZE_OF_LONG, data.getLong(index * SizeOf.SIZE_OF_LONG) + value);
  }

  public void add(long value) {
    grow(length);
    data.setLong(length * SizeOf.SIZE_OF_LONG, value);
    length += 1;
  }

  @Override
  public String toString() {
    int i;
    StringBuilder sb = new StringBuilder(length * 4);

    sb.append('{');
    int l = length - 1;
    for (i=0; i<l; i++) {
      sb.append(get(i));
      sb.append(',');
    }
    sb.append(get(i));
    sb.append('}');

    return sb.toString();
  }
}

