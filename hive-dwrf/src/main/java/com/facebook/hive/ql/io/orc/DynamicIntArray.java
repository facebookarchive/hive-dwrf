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
package com.facebook.hive.ql.io.orc;

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
final class DynamicIntArray {
  static final int DEFAULT_SIZE = SizeOf.SIZE_OF_INT * 8 * 1024;

  private Slice data;                    // the real data
  private int length = 0;                // max set element index +1

  public DynamicIntArray() {
    this(DEFAULT_SIZE);
  }

  public DynamicIntArray(int size) {

    data = Slices.allocate(size);
  }

  /**
   * Ensure that the given index is valid.
   */
  private void grow(int index) {
    if ((index * SizeOf.SIZE_OF_INT) + (SizeOf.SIZE_OF_INT - 1) >= data.length()) {
      int newSize = Math.max((index * SizeOf.SIZE_OF_INT) + DEFAULT_SIZE, 2 * data.length());
      Slice newSlice = Slices.allocate(newSize);
      newSlice.setBytes(0, data);
      data = newSlice;
    }
  }

  public int get(int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("Index " + index +
                                            " is outside of 0.." +
                                            (length - 1));
    }

    return data.getInt(index * SizeOf.SIZE_OF_INT);
  }

  public void set(int index, int value) {
    grow(index);
    if (index >= length) {
      length = index + 1;
    }

    data.setInt(index * SizeOf.SIZE_OF_INT, value);
  }

  public void increment(int index, int value) {
    grow(index);
    if (index >= length) {
      length = index + 1;
    }
    data.setInt(index * SizeOf.SIZE_OF_INT, data.getInt(index * SizeOf.SIZE_OF_INT) + value);
  }

  public void add(int value) {
    grow(length);
    data.setInt(length * SizeOf.SIZE_OF_INT, value);
    length += 1;
  }

  public int size() {
    return length;
  }

  public void clear() {
    length = 0;
    data = Slices.allocate(DEFAULT_SIZE);
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

  public int getSizeInBytes() {
    return data.length();
  }
}

