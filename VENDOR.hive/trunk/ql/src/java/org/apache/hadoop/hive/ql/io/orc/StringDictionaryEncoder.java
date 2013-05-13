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
package org.apache.hadoop.hive.ql.io.orc;

import it.unimi.dsi.fastutil.Hash;
import it.unimi.dsi.fastutil.ints.IntArrays;
import it.unimi.dsi.fastutil.ints.IntComparator;
import it.unimi.dsi.fastutil.objects.ObjectOpenCustomHashSet;

import java.io.IOException;
import java.io.OutputStream;

import org.apache.hadoop.io.Text;

/**
 * A fast, memory efficient implementation of dictionary encoding stores strings. The strings are stored as UTF-8 bytes
 * and an offset/length for each entry.
 */
class StringDictionaryEncoder extends DictionaryEncoder {
  private final DynamicByteArray byteArray = new DynamicByteArray();
  private final DynamicIntArray keySizes = new DynamicIntArray();
  private final Text newKey = new Text();
  private final Text cmpKey = new Text();

  private final TextCompressedOpenHashSet htDictionary = new TextCompressedOpenHashSet(new TextCompressedHashStrategy());

  private int numElements = 0;

  private class TextCompressed {
    private int offset = -1;
    private int hashcode = -1;
    private TextCompressed(int hash) {
      hashcode = hash;
    }
  }

  public class TextPositionComparator implements IntComparator {
   @Override
   public int compare (Integer k1, Integer k2) {
     return this.compare(k1.intValue(), k2.intValue());
   }

	 @Override
	 public int compare (int k1, int k2) {
		 int k1Offset = keySizes.get(2 * k1);
		 int k1Length = keySizes.get(2 * k1 + 1);

		 int k2Offset = keySizes.get(2 * k2);
		 int k2Length = keySizes.get(2 * k2 + 1);

		 byteArray.setText(cmpKey, k1Offset, k1Length);

		 return byteArray.compare(cmpKey.getBytes(), 0, k1Length, k2Offset, k2Length);
	 }
  }

  public class TextCompressedHashStrategy implements Hash.Strategy<TextCompressed> {
    @Override
    public boolean equals(TextCompressed obj1, TextCompressed other) {
      return compareValue(obj1.offset) == 0;
    }

    @Override
    public int hashCode(TextCompressed arg0) {
      return arg0.hashcode;
    }
  }

  public class TextCompressedOpenHashSet extends ObjectOpenCustomHashSet<TextCompressed> {
    private TextCompressedOpenHashSet(Hash.Strategy<TextCompressed> strategy) {
      super(strategy);
    }
  }

  public StringDictionaryEncoder() {
    super();
  }

  public StringDictionaryEncoder(boolean sortKeys) {
    super(sortKeys);
  }

  public int add(Text value) {
    newKey.set(value);
    int len = newKey.getLength();
    TextCompressed curKeyCompressed = new TextCompressed(newKey.hashCode());
    if (htDictionary.contains(curKeyCompressed)) {
      return htDictionary.get(curKeyCompressed).offset;
    } else {
      // update count of hashset keys
      int valRow = numElements;
      numElements += 1;
      // set current key offset and hashcode
      curKeyCompressed.offset = valRow;
      htDictionary.add(curKeyCompressed);
      keySizes.add(byteArray.add(newKey.getBytes(), 0, len));
      keySizes.add(len);
      return valRow;
    }
  }

  @Override
  protected int compareValue(int position) {
    return byteArray.compare(newKey.getBytes(), 0, newKey.getLength(),
      keySizes.get(2 * position), keySizes.get(2 * position + 1));
  }

  private class VisitorContextImpl implements VisitorContext<Text> {
    private int originalPosition;
    private final Text text = new Text();

    public void setOriginalPosition(int pos) {
      originalPosition = pos;
    }

    public int getOriginalPosition() {
      return originalPosition;
    }

    public Text getKey() {
      byteArray.setText(text, keySizes.get(originalPosition * 2), getLength());
      return text;
    }

    public void writeBytes(OutputStream out) throws IOException {
        byteArray.write(out, keySizes.get(originalPosition * 2), getLength());
    }

    public int getLength() {
      return keySizes.get(originalPosition * 2 + 1);
    }

  }

  private void visitDictionary(Visitor<Text> visitor, VisitorContextImpl context
                      ) throws IOException {
      int[] keysArray = null;
      if (sortKeys) {
        keysArray = new int[numElements];
        for (int idx = 0; idx < numElements; idx++) {
          keysArray[idx] = idx;
        }
        IntArrays.quickSort(keysArray, new TextPositionComparator());
      }

      for (int pos = 0; pos < numElements; pos++) {
        context.setOriginalPosition(keysArray == null? pos : keysArray[pos]);
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
      byteArray.setText(result, keySizes.get(originalPosition * 2),
          keySizes.get(originalPosition * 2 + 1));
  }

  /**
   * Reset the table to empty.
   */
  @Override
  public void clear() {
    byteArray.clear();
    keySizes.clear();
    htDictionary.clear();
    numElements = 0;
  }

  /**
   * Get the size of the character data in the table.
   * @return the bytes used by the table
   */
  public int getCharacterSize() {
    return byteArray.size();
  }

  /**
   * Calculate the approximate size in memory.
   * @return the number of bytes used in storing the tree.
   */
  public long getByteSize() {
    // one for dictionary keys
    long refSizes = (htDictionary.size() * 4);

    // 2 int fields per element
    long textCompressedSizes = numElements * 4 * 2;

    // bytes in the characters
    // keySizes stores 2 ints per element (2 * 4 * numElements)
    long totalSize =  getCharacterSize() + 2 * 4 * numElements;
    totalSize += refSizes + textCompressedSizes;
    return totalSize;
  }

  /**
   * Get the number of elements in the set.
   */
  @Override
  public int size() {
    return numElements;
  }

}
