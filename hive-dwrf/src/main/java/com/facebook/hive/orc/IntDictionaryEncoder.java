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

import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.longs.Long2IntOpenHashMap;
import org.apache.hadoop.hive.ql.io.slice.SizeOf;

import java.io.IOException;
import java.io.OutputStream;

class IntDictionaryEncoder extends DictionaryEncoder {

  private long newKey;
  private int numElements = 0;
  private final int numBytes;
  private final boolean useVInts;

  protected final DynamicLongArray keys;
  protected final DynamicIntArray counts;
  protected Long2IntOpenHashMapWithByteSize dictionary = new Long2IntOpenHashMapWithByteSize();

  public IntDictionaryEncoder(int numBytes, boolean useVInts, MemoryEstimate memoryEstimate) {
    this(true, numBytes, useVInts, memoryEstimate);
  }

  public IntDictionaryEncoder(boolean sortKeys, int numBytes, boolean useVInts,
      MemoryEstimate memoryEstimate) {
    super(sortKeys, memoryEstimate);
    this.numBytes = numBytes;
    this.useVInts = useVInts;
    this.keys = new DynamicLongArray(memoryEstimate);
    this.counts = new DynamicIntArray(memoryEstimate);
  }

  public long getValue(int position) {
    return keys.get(position);
  }

  private class Long2IntOpenHashMapWithByteSize extends Long2IntOpenHashMap {
    private static final long serialVersionUID = 0L;

    public Long2IntOpenHashMapWithByteSize() {
      super();
      memoryEstimate.incrementTotalMemory(getByteSize());
    }

    @Override
    protected void rehash(final int newN) {
      // rehash resizes the arrays, so need to account for this in the memory estimate
      memoryEstimate.decrementTotalMemory(getByteSize());
      super.rehash(newN);
      memoryEstimate.incrementTotalMemory(getByteSize());
    }

    public int getByteSize() {
      int size = key.length * 8 + value.length * 4 + used.length;

      return size;
    }

    // A cleanup method that should be called before allowing the object to leave scope
    public void cleanup() {
      // Account for the destruction of the arrays used by this object
      memoryEstimate.decrementTotalMemory(getByteSize());
    }
  }

  /**
   *
   */
  public class LongPositionComparator implements IntComparator {
    @Override
    public int compare(Integer pos, Integer cmpPos) {
      return this.compare(pos.intValue(), cmpPos.intValue());
    }

    @Override
    public int compare(int pos, int cmpPos) {
      return compareValue(keys.get(pos), keys.get(cmpPos));
    }
  }

  public void visitDictionary(Visitor<Long> visitor, IntDictionaryEncoderVisitorContext context) throws IOException {
      int[] keysArray = null;
      if (sortKeys) {
        keysArray = new int[numElements];
        for (int idx = 0; idx < numElements; idx++) {
          keysArray[idx] = idx;
        }
        IntArrays.quickSort(keysArray, new LongPositionComparator());
      }
      for (int pos = 0; pos < numElements; pos++) {
        context.setOriginalPosition(keysArray == null? pos : keysArray[pos]);
        visitor.visit(context);
      }
  }

  public void visit(Visitor<Long> visitor) throws IOException {
    visitDictionary(visitor, new IntDictionaryEncoderVisitorContext());
  }

  @Override
  public void clear() {
    keys.clear();
    counts.clear();
    dictionary.cleanup();
    dictionary = new Long2IntOpenHashMapWithByteSize();
    // Decrement the dictionary memory by the total size of all the elements
    memoryEstimate.decrementDictionaryMemory(SizeOf.SIZE_OF_LONG * numElements);
    numElements = 0;
  }

  private int compareValue (long k, long cmpKey) {
    if (k > cmpKey) {
      return 1;
    } else if (k < cmpKey) {
      return -1;
    }
    return 0;
  }

  @Override
  protected int compareValue(int position) {
    long cmpKey = keys.get(position);
    return compareValue(newKey, cmpKey);
  }

  public int add (long value) {
    newKey = value;
    if (dictionary.containsKey(value)) {
      int index = dictionary.get(value);
      counts.increment(index, 1);
      return dictionary.get(value);
    } else {
      int valRow = numElements;
      numElements++;
      dictionary.put(value, valRow);
      keys.add(newKey);
      counts.add(1);

      // Add the size of one element to the dictionary size in memory
      memoryEstimate.incrementDictionaryMemory(SizeOf.SIZE_OF_LONG);
      return valRow;
    }
  }

  public class IntDictionaryEncoderVisitorContext implements VisitorContext<Long> {

    int originalPosition;
    public void setOriginalPosition(int pos) {
      originalPosition = pos;
    }
    public int getOriginalPosition() {
      return originalPosition;
    }

    public Long getKey() {
      return keys.get(originalPosition);
    }

    public void writeBytes(OutputStream outputStream) throws IOException {
      long cur = keys.get(originalPosition);
      SerializationUtils.writeIntegerType(outputStream, cur, numBytes, true, useVInts);
    }

    // TODO: this should be different
    public int getLength() {
      return 8;
    }

    public int getCount() {
      return counts.get(originalPosition);
    }
    @Override
    public int getIndexStride() {
      throw new UnsupportedOperationException("IntDictionaryEncoder does not currently track the" +
          " index stride");
    }
  }

  public int getUncompressedLength() {
    // The amount of memory used by entries in the dictionary
    return numElements * 8;
  }

  /**
   * Get the number of elements in the set.
   */
  @Override
  public int size() {
    return numElements;
  }

  // A cleanup method that should be called before allowing the object to leave scope
  public void cleanup() {
    keys.cleanup();
    counts.cleanup();
    dictionary.cleanup();
    // Decrement the dictionary memory by the total size of all the elements
    memoryEstimate.decrementDictionaryMemory(SizeOf.SIZE_OF_LONG * numElements);
  }
}

