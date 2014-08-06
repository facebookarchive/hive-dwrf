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

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.Text;

import com.google.common.primitives.Ints;

/**
 * A fast, memory efficient implementation of dictionary encoding stores strings. The strings are stored as UTF-8 bytes
 * and an offset/length for each entry.
 */
class StringDictionaryEncoder extends DictionaryEncoder {
  private final DynamicByteArray byteArray;

  // The following int arrays represent entries in the dictionary
  // int[]'s were used instead of DynamicIntArrays because they are
  // accessed a lot, and DynamicIntArrays perform poorly
  private int[] offsets = new int[DynamicIntArray.DEFAULT_SIZE];
  private int[] hashcodes = new int[DynamicIntArray.DEFAULT_SIZE];
  private int[] nexts = new int[DynamicIntArray.DEFAULT_SIZE];
  private int[] counts = new int[DynamicIntArray.DEFAULT_SIZE];
  private int[] indexStrides = new int[DynamicIntArray.DEFAULT_SIZE];

  private Text newKey = new Text();

  private final TextCompressedHashSet htDictionary = new TextCompressedHashSet();

  // The number of elements in the dictionary
  private int numElements = 0;

  private final boolean sortByStride;

  // A custom implementation of a hash set, based on fastutil's ObjectOpenCustomHashSet
  // Takes in indices into the int arrays of the surrounding class
  // NOTE: For the sake of speed and simplicity, assumes index 0 is never used (means we don't
  // have to initialize all the values in the set to -1 or some other impossible value)
  private class TextCompressedHashSet {
    // Essentially an alias for length - 1
    private int mask;
    // An array of indeces into the int[]'s in StringDictionaryEncoder
    private int[] key;
    // The number of values in the dictionary
    private int numValues;
    // The point beyond which the dictionary needs to grow
    private int maxFill;
    // The current size of the dictionary
    private int length;

    // The fraction of the dictionary size beyond which it should grow
    private static final float LOAD_FACTOR = 0.75f;
    // The number of entries the set should be initialized to expect
    private static final int MIN_EXPECTED = 128;

    public TextCompressedHashSet() {
      this.length = it.unimi.dsi.fastutil.HashCommon.arraySize(MIN_EXPECTED, LOAD_FACTOR);
      this.mask = length - 1;
      this.maxFill = it.unimi.dsi.fastutil.HashCommon.maxFill(length, LOAD_FACTOR);
      this.key = new int[length];
    }

    public int get(int k) {
      // Compute the bucket the value at k is in
      int pos = it.unimi.dsi.fastutil.HashCommon.murmurHash3(hashcodes[k]) & mask;
      // Iterate over the chain in that bucket
      int other = key[pos];
      while(other != 0) {
        // Compare the hashcodes as a quick way to rule out some results
        if (hashcodes[k] == hashcodes[other] && equalsValue(offsets[other], getEnd(other) - offsets[other])) {
          return other;
        }
        other = nexts[other];
      }
      return 0;
    }

    public int add(final int k) {
      // Compute the bucket the value at k is in
      int pos = it.unimi.dsi.fastutil.HashCommon.murmurHash3(hashcodes[k]) & mask;
      int other = key[pos];
      int prev = 0;
      // Iterate over the chain in that bucket
      while(other != 0) {
        // Compare the hashcodes as a quick way to rule out some results
        if (hashcodes[k] == hashcodes[other] && equalsValue(offsets[other], getEnd(other) - offsets[other])) {
          // If the value is found to already exist, and it's not already at the head of the chain
          // move it there to speed up adding this value in the future.
          if (other != key[pos]) {
            nexts[prev] = nexts[other];
            nexts[other] = key[pos];
            key[pos] = other;
          }
          counts[other]++;
          return other;
        }
        prev = other;
        other = nexts[other];
      }

      // If it's not already in the bucket add it at the front
      nexts[k] = key[pos];
      key[pos] = k;
      counts[k] = 1;
      // Check if it's necessary to rehash
      if (++numValues >= maxFill) {
        rehash(it.unimi.dsi.fastutil.HashCommon.arraySize(numValues + 1, LOAD_FACTOR));
      }
      // Add one to the total memory for the int key
      memoryEstimate.incrementTotalMemory(4);
      return 0;
    }

    private void rehash(final int newN) {
      int i = 0;
      int pos;
      int k;
      int next;
      final int[] key = this.key;
      final int newMask = newN - 1;
      final int[] newKey = new int[newN];
      // Iterate over all values in the set and rehash them
      for(int j = numValues; j != 0;) {
        while((k = key[i]) == 0) {
          i++;
        }
        do {
          // Compute the new hash
          pos = it.unimi.dsi.fastutil.HashCommon.murmurHash3(hashcodes[k])  & newMask;
          // Store the next value in the current bucket of the old set
          next = nexts[k];
          // Add the value to the beginning of the new bucket
          nexts[k] = newKey[pos];
          newKey[pos] = k;
          j--;
          // Repeat for each value in the old bucket
        } while ((k = next) != 0);
        i++;
      }
      length = newN;
      mask = newMask;
      maxFill = it.unimi.dsi.fastutil.HashCommon.maxFill(length, LOAD_FACTOR);
      this.key = newKey;
    }
    public void clear() {
      if (numValues == 0) {
        return;
      }

      // Decrement the total memory by the size of all the int keys we had added
      memoryEstimate.decrementTotalMemory(numValues * 4);
      numValues = 0;
      // Set all values to 0
      for (int i = 0; i < length; i++) {
        key[i] = 0;
      }
    }

    public int size() {
      return numValues;
      }
    }

  public class TextPositionComparator implements IntComparator {
    @Override
    public int compare (Integer k1, Integer k2) {
      return this.compare(k1.intValue(), k2.intValue());
    }

    @Override
    public int compare (int k1, int k2) {
      if (sortByStride) {
        if ((counts[k1] == 1 || counts[k2] == 1) && (counts[k1] != 1 || counts[k2] != 1)) {
          return Ints.compare(counts[k1], counts[k2]);
        }

        if ((counts[k1] == 1 && counts[k2] == 1) && indexStrides[k1] != indexStrides[k2]) {
          return Ints.compare(indexStrides[k1], indexStrides[k2]);
        }
      }

      int k1Length = getEnd(k1) - offsets[k1];

      int k2Length = getEnd(k2) - offsets[k2];

      return byteArray.compare(offsets[k1], k1Length, offsets[k2], k2Length);
    }
  }

  public StringDictionaryEncoder(MemoryEstimate memoryEstimate) {
    this(true, false, memoryEstimate);
  }

  /**
   * Returns the size of the int arrays used by this class, it's 4 times the length of the arrays
   */
  private int getSizeOfIntArrays() {
    return (offsets.length + hashcodes.length + nexts.length + counts.length +
        indexStrides.length) * 4;
  }

  public StringDictionaryEncoder(boolean sortKeys, boolean sortByStride,
      MemoryEstimate memoryEstimate) {
    super(sortKeys, memoryEstimate);
    this.sortByStride = sortByStride;
    this.byteArray = new DynamicByteArray(memoryEstimate);
    // Add to the memory the size of each int[] (length of array * size of int)
    // Current list of arrays offsets, hashcodes, nexts, counts, indexStrides
    memoryEstimate.incrementTotalMemory(getSizeOfIntArrays());
  }

  public int add(Text value, int indexStride) {
    newKey = value;
    int len = newKey.getLength();
    // See the comment on TextCompressedHashSet
    // This intentionally skips index 0
    int newKeyIndex = numElements + 1;
    hashcodes[newKeyIndex] = newKey.hashCode();
    int existing = htDictionary.add(newKeyIndex);
    if (existing != 0) {
      return existing - 1;
    } else {
      // update count of hashset keys
      int valRow = numElements;
      numElements += 1;
      // If we've outgrown the arrays, resize them
      if (newKeyIndex + 1 >= offsets.length) {
        // Make sure the memory estimate reflects the new array sizes
        memoryEstimate.decrementTotalMemory(getSizeOfIntArrays());
        offsets = getDoubleSizeArray(offsets);
        hashcodes = getDoubleSizeArray(hashcodes);
        nexts = getDoubleSizeArray(nexts);
        counts = getDoubleSizeArray(counts);
        indexStrides = getDoubleSizeArray(indexStrides);
        memoryEstimate.incrementTotalMemory(getSizeOfIntArrays());
      }
      // set current key offset and length
      offsets[newKeyIndex] = byteArray.add(newKey.getBytes(), 0, len);
      indexStrides[newKeyIndex] = indexStride;

      // Update the size of the dictionary in memory
      memoryEstimate.incrementDictionaryMemory(len);
      return valRow;
    }
  }

  private int[] getDoubleSizeArray(int[] array) {
    int[] newArray = new int[array.length * 2];
    System.arraycopy(array, 0, newArray, 0, array.length);
    return newArray;
  }

  private int getEnd(int pos) {
    if (pos + 1 > numElements) {
      return byteArray.size();
    }

    return offsets[pos + 1];
  }

  @Override
  protected int compareValue(int position) {
    return byteArray.compare(newKey.getBytes(), 0, newKey.getLength(),
        offsets[position], getEnd(position) - offsets[position]);
  }

  protected boolean equalsValue(int offset, int length) {
    return byteArray.equals(newKey.getBytes(), 0, newKey.getLength(), offset, length);
  }

  private class VisitorContextImpl implements VisitorContext<Text> {
    private int originalPosition;
    private int start;
    private int length;
    private int count;
    private int indexStride;
    private final Text text = new Text();

    public void setOriginalPosition(int pos) {
      originalPosition = pos - 1;
      start = offsets[pos];
      length = getEnd(pos) - offsets[pos];
      count = counts[pos];
      indexStride = indexStrides[pos];
    }

    public int getOriginalPosition() {
      return originalPosition;
    }

    public Text getKey() {
      byteArray.setText(text, start, length);
      return text;
    }

    public void writeBytes(OutputStream out) throws IOException {
        byteArray.write(out, start, length);
    }

    public int getLength() {
      return length;
    }

    public int getCount() {
      return count;
    }

    public int getIndexStride() {
      return indexStride;
    }
  }

  private void visitDictionary(Visitor<Text> visitor, VisitorContextImpl context
                      ) throws IOException {
      int[] keysArray = null;
      if (sortKeys) {
        keysArray = new int[numElements];
        for (int idx = 0; idx < numElements; idx++) {
          keysArray[idx] = idx + 1;
        }
        IntArrays.quickSort(keysArray, new TextPositionComparator());
      }

      for (int pos = 0; pos < numElements; pos++) {
        context.setOriginalPosition(keysArray == null? pos + 1: keysArray[pos]);
        visitor.visit(context);
      }
      keysArray = null;
  }

  /**
   * Visit all of the nodes in the tree in sorted order.
   * @param visitor the action to be applied to each ndoe
   * @throws IOException
   */
  public void visit(Visitor<Text> visitor) throws IOException {
    visitDictionary(visitor, new VisitorContextImpl());
  }

  public void getText(Text result, int originalPosition) {
    byteArray.setText(result, offsets[originalPosition + 1], getEnd(originalPosition + 1) - offsets[originalPosition + 1]);
  }

  /**
   * Reset the table to empty.
   */
  @Override
  public void clear() {
    // Subtract the amount of memory being used for the dictionary
    memoryEstimate.decrementDictionaryMemory(byteArray.size());
    byteArray.clear();
    // Add the new size
    memoryEstimate.incrementDictionaryMemory(byteArray.size());
    htDictionary.clear();
    // Make sure the memory estimate reflects the new array sizes
    memoryEstimate.decrementTotalMemory(getSizeOfIntArrays());
    offsets = new int[DynamicIntArray.DEFAULT_SIZE];
    hashcodes = new int[DynamicIntArray.DEFAULT_SIZE];
    nexts = new int[DynamicIntArray.DEFAULT_SIZE];
    counts = new int[DynamicIntArray.DEFAULT_SIZE];
    indexStrides = new int[DynamicIntArray.DEFAULT_SIZE];
    memoryEstimate.incrementTotalMemory(getSizeOfIntArrays());
    numElements = 0;
  }

  /**
   * Get the size of the character data in the table.
   * @return the bytes used by the table
   */
  public long getCharacterSize() {
    return byteArray.getSizeInBytes();
  }

  public int getUncompressedLength() {
    // The amount of memory used by entries in the dictionary
    return byteArray.size();
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
    htDictionary.clear();
    memoryEstimate.decrementTotalMemory(byteArray.getSizeInBytes());
    memoryEstimate.decrementTotalMemory(getSizeOfIntArrays());
    memoryEstimate.decrementDictionaryMemory(byteArray.size());
  }
}
