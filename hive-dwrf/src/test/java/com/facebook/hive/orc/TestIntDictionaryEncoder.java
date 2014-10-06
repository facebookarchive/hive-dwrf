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

import static junit.framework.Assert.assertEquals;

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;

import org.apache.hadoop.io.DataOutputBuffer;
import org.junit.Test;

/**
 * Test the red-black tree with string keys.
 */
public class TestIntDictionaryEncoder {

  private IntDictionaryEncoder buildDictionaryEncoder(int[] values, boolean sortKeys,
      boolean useVInts) {

    IntDictionaryEncoder dictEncoder = new IntDictionaryEncoder(sortKeys, 4, useVInts);
    for(int value : values) {
      dictEncoder.add((long) value);
    }
    return dictEncoder;
  }

  private void checkContent(IntDictionaryEncoder dictEncoder, int[] values, int[] order)
      throws Exception {
    dictEncoder.visit(new TestVisitor(values, order));

  }

  @Test
  public void test1() throws Exception {
    IntDictionaryEncoder dictEncoder = new IntDictionaryEncoder(false, 4, true);

    assertEquals(98858, dictEncoder.getByteSize());
    assertEquals(0, dictEncoder.size());

    int [] addKeys = new int [] {1, 1, 0, 3, 2, 1, 3, 0, -6, 100};
    int [] addKPos = new int [] {0, 0, 1, 2, 3, 0, 2, 1, 4, 5};
    int [] sizes = new int [] {1, 1, 2, 3, 4, 4, 4, 4, 5, 6};
    int [] expectedUniqueValues = new int [] {1, 0, 3, 2, -6, 100};
    int [] expectedOrder = new int[expectedUniqueValues.length];
    int ctr = 0;
    for (int v : expectedUniqueValues) {
      expectedOrder[ctr] = ctr;
      ctr++;
    }
    ctr = 0;

    for (int i = 0; i < 10; i++) {
      int pos = dictEncoder.add(addKeys[i]);
      assertEquals(addKPos[i], pos);
      assertEquals(sizes[i], dictEncoder.size());
    }
    checkContent(dictEncoder, expectedUniqueValues, expectedOrder);
    dictEncoder.clear();
    assertEquals(98858, dictEncoder.getByteSize());
    assertEquals(0, dictEncoder.size());
    checkContent(dictEncoder, new int[0], new int[0]);
  }

  @Test
  public void test2() throws Exception {
    IntDictionaryEncoder[] encoders = new IntDictionaryEncoder[] { new IntDictionaryEncoder(4, true),
      new IntDictionaryEncoder(true, 4 , true) };

    for (IntDictionaryEncoder dictEncoder : encoders) {
      assertEquals(98858, dictEncoder.getByteSize());
      assertEquals(0, dictEncoder.size());

      int [] addKeys = new int [] {1, 1, 0, 3, 2, 1, 3, 0, -6, 100};
      int [] addKPos = new int [] {0, 0, 1, 2, 3, 0, 2, 1, 4, 5};
      int [] sizes = new int [] {1, 1, 2, 3, 4, 4, 4, 4, 5, 6};
      int [] expectedOrderedUniqueValues = new int [] { -6, 0, 1, 2, 3, 100 };
      int [] expectedOrder = new int[] { 4, 1, 0, 3, 2, 5 };

      for (int i = 0; i < 10; i++) {
        int pos = dictEncoder.add(addKeys[i]);
        assertEquals(addKPos[i], pos);
        assertEquals(sizes[i], dictEncoder.size());
      }

      checkContent(dictEncoder, expectedOrderedUniqueValues, expectedOrder);
      dictEncoder.clear();
      assertEquals(98858, dictEncoder.getByteSize());
      assertEquals(0, dictEncoder.size());
      checkContent(dictEncoder, new int[0], new int[0]);
    }
  }

  private static class TestVisitor implements IntDictionaryEncoder.Visitor<Long> {
    private final int[] values;
    private final int[] order;
    int currentIdx = 0;
    private final DataOutputBuffer buffer = new DataOutputBuffer();

    TestVisitor(int[] values, int [] order) {
      this.values = values;
      this.order = order;
    }

    public void visit(IntDictionaryEncoder.VisitorContext<Long> context) throws IOException {

      long curKey = context.getKey().longValue();
      assertEquals("in value " + currentIdx, values[currentIdx], curKey);
      assertEquals("in value " + currentIdx, order[currentIdx], context.getOriginalPosition());

      buffer.reset();
      context.writeBytes(buffer);
      final InputStream inbuffer = new ByteArrayInputStream(buffer.getData());
      assertEquals(curKey, SerializationUtils.readVslong(inbuffer));
      currentIdx++;
    }

  }
}
