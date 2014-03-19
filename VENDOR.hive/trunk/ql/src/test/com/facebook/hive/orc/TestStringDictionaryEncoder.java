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

import java.io.IOException;
import java.util.Arrays;

import org.apache.hadoop.io.DataOutputBuffer;
import org.apache.hadoop.io.Text;
import org.junit.Test;

/**
 * Test the red-black tree with string keys.
 */
public class TestStringDictionaryEncoder {

  private static class TestVisitor implements StringDictionaryEncoder.Visitor<Text> {
    private final String[] words;
    private final int[] order;
    private final DataOutputBuffer buffer = new DataOutputBuffer();
    int current = 0;

    TestVisitor(String[] args, int[] order) {
      words = args;
      this.order = order;
    }

    @Override
    public void visit(StringDictionaryEncoder.VisitorContext<Text> context
                     ) throws IOException {
      String word = context.getKey().toString();
      assertEquals("in word " + current, words[current], word);
      assertEquals("in word " + current, order[current],
        context.getOriginalPosition());
      buffer.reset();
      context.writeBytes(buffer);
      assertEquals(word, new String(buffer.getData(),0,buffer.getLength()));
      current += 1;
    }
  }

  private void checkContent(StringDictionaryEncoder dictEncoder, String[] values, int[] order)
      throws Exception {
    dictEncoder.visit(new TestVisitor(values, order));

  }

  private StringDictionaryEncoder buildDictionary(String[] values, boolean sortKeys) {
    StringDictionaryEncoder dictEncoder = new StringDictionaryEncoder(sortKeys, false);
    for(String value : values) {
      dictEncoder.add(new Text(value), 0);
    }
    return dictEncoder;
  }

  @Test
  public void test1() throws Exception {
    StringDictionaryEncoder dict = new StringDictionaryEncoder();

    String [] addKeys = new String[] {
      "owen", "ashutosh", "owen", "alan", "alan", "ashutosh", "greg", "eric", "arun", "eric14", "o", "ziggy", "z",
      "greg",
    };

    int [] addKPos = new int[] {0, 1, 0, 2, 2, 1, 3, 4, 5, 6, 7, 8, 9, 3};
    int [] sizes = new int []{1, 2, 2, 3, 3, 3, 4, 5, 6, 7, 8, 9, 10, 10};

    String [] expectedOrderedUniqueValues = {"alan", "arun", "ashutosh", "eric", "eric14", "greg",
      "o", "owen", "z", "ziggy"};


    int [] expectedOrder = new int[]{2,5,1,4,6,3,7,0,9,8};
    for (int i=0; i < addKeys.length; i++) {
      int addPos = dict.add(new Text(addKeys[i]), 0);
      assertEquals(addPos, addKPos[i]);
      assertEquals(sizes[i], dict.size());
    }
    checkContent(dict, expectedOrderedUniqueValues, expectedOrder);
  }

  private void testUnsorted(boolean strideDictionaries) throws Exception {

    StringDictionaryEncoder dict = new StringDictionaryEncoder(false, strideDictionaries);
    String [] addKeys = new String[] {
      "owen", "ashutosh", "owen", "alan", "alan", "ashutosh", "greg", "eric", "arun", "eric14", "o", "ziggy", "z",
    };

    int [] addKPos = new int[] {0, 1, 0, 2, 2, 1, 3, 4, 5, 6, 7, 8, 9, 10};
    int [] sizes = new int []{1, 2, 2, 3, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    String [] expectedUniqueValues = new String[] {
      "owen", "ashutosh", "alan", "greg", "eric", "arun", "eric14", "o", "ziggy", "z",
    };
    int [] expectedOrder = new int[addKeys.length];
    for (int i = 0; i < addKeys.length; i++) {
      expectedOrder[i] = i;
    }

    for (int i=0; i < addKeys.length; i++) {
      int addPos = dict.add(new Text(addKeys[i]), 0);
      assertEquals(addPos, addKPos[i]);
      assertEquals(sizes[i], dict.size());
    }
    checkContent(dict, expectedUniqueValues, expectedOrder);
    dict.clear();
    assertEquals(688128, dict.getSizeInBytes());
    assertEquals(0, dict.size());
  }

  @Test
  public void testUnsorted() throws Exception {
    testUnsorted(false);
  }

  @Test
  public void testUnsortedStrideDictionaries() throws Exception {
    testUnsorted(true);
  }

  @Test
  public void testSortedStrideDictionaries() throws Exception {
    StringDictionaryEncoder dict = new StringDictionaryEncoder(true, true);
    String [] addKeys = new String[] {
      "owen", "ashutosh", "owen", "alan", "alan", "ashutosh", "greg", "eric", "arun", "eric14", "o", "ziggy", "z",
    };

    int [] addKPos = new int[] {0, 1, 0, 2, 2, 1, 3, 4, 5, 6, 7, 8, 9, 10};
    int [] sizes = new int []{1, 2, 2, 3, 3, 3, 4, 5, 6, 7, 8, 9, 10, 11};
    String [] expectedUniqueValues = new String[] {
      "arun", "eric", "eric14", "greg", "o", "z", "ziggy", "alan", "ashutosh", "owen",
      };
    int [] expectedOrder = new int[] {5, 4, 6, 3, 7, 9, 8, 2, 1, 0};

    for (int i=0; i < addKeys.length; i++) {
      int addPos = dict.add(new Text(addKeys[i]), 0);
      assertEquals(addPos, addKPos[i]);
      assertEquals(sizes[i], dict.size());
    }
    checkContent(dict, expectedUniqueValues, expectedOrder);
    dict.clear();
    assertEquals(688128, dict.getSizeInBytes());
    assertEquals(0, dict.size());
  }

  @Test
  public void test2() throws Exception {
    String [] v = new String[] {"a", "b", "c", "d", "e", "f", "g", "h", "i", "j", "k", "l",
        "m", "n", "o", "p", "q", "r", "s", "t", "u", "v", "w", "x", "y", "z"};
    StringDictionaryEncoder dict  =
      buildDictionary(v, true);
    assertEquals(26, dict.size());
    checkContent(dict, v,
        new int[]{0,1,2, 3,4,5, 6,7,8, 9,10,11, 12,13,14,
      15,16,17, 18,19,20, 21,22,23, 24,25});
  }

  @Test
  public void test3() throws Exception {
    String [] v = new String[] {"z", "y", "x", "w", "v", "u", "t", "s", "r", "q", "p", "o", "n",
        "m", "l", "k", "j", "i", "h", "g", "f", "e", "d", "c", "b", "a"};
    StringDictionaryEncoder dict =
      buildDictionary(v, true);
    assertEquals(26, dict.size());
    Arrays.sort(v);
    checkContent(dict, v,
      new int[]{25,24,23, 22,21,20, 19,18,17, 16,15,14,
      13,12,11, 10,9,8, 7,6,5, 4,3,2, 1,0});
  }

  @Test
  public void test4() throws Exception {
    String [] v = new String[] {"z", "y", "x", "w", "v", "u", "t", "s", "r", "q", "p", "o", "n",
        "m", "l", "k", "j", "i", "h", "g", "f", "e", "d", "c", "b", "a"};
    StringDictionaryEncoder dict =
      buildDictionary(v, false);
    assertEquals(26, dict.size());
    checkContent(dict, v,
        new int[]{0,1,2, 3,4,5, 6,7,8, 9,10,11, 12,13,14,
      15,16,17, 18,19,20, 21,22,23, 24,25});
  }


  public static void main(String[] args) throws Exception {
    TestStringDictionaryEncoder test = new TestStringDictionaryEncoder();
    test.test1();
    test.test2();
    test.test3();
    test.test4();
    test.testUnsorted();
    TestSerializationUtils serUtils = new TestSerializationUtils();
    serUtils.TestDoubles();
    TestDynamicArray test6 = new TestDynamicArray();
    test6.testByteArray();
    test6.testIntArray();
    TestZlib zlib = new TestZlib();
    zlib.testCorrupt();
    zlib.testNoOverflow();
    TestInStream inStreamTest = new TestInStream();
    inStreamTest.testUncompressed();
    inStreamTest.testCompressed();
    inStreamTest.testCorruptStream();
    TestRunLengthByteReader rleByte = new TestRunLengthByteReader();
    rleByte.testUncompressedSeek();
    rleByte.testCompressedSeek();
    rleByte.testSkips();
    TestRunLengthIntegerReader rleInt = new TestRunLengthIntegerReader();
    rleInt.testUncompressedSeek();
    rleInt.testCompressedSeek();
    rleInt.testSkips();
    TestBitFieldReader bit = new TestBitFieldReader();
    bit.testUncompressedSeek();
    bit.testCompressedSeek();
    bit.testSkips();
    TestOrcFile test1 = new TestOrcFile();
    test1.test1();
    test1.testEmptyFile();
    test1.testMetaData();
    test1.testUnionAndTimestamp();
    test1.testColumnProjection();
    test1.testSnappy();
    test1.testWithoutIndex();
    test1.testSeek();
    TestFileDump test2 = new TestFileDump();
    test2.testDump();
    test2.testDictionaryThreshold();
    test2.testUnsortedDictionary();
    TestStreamName test3 = new TestStreamName();
    test3.test1();
    TestInputOutputFormat test4 = new TestInputOutputFormat();
    test4.testInOutFormat();
    test4.testMROutput();
    test4.testEmptyFile();
    test4.testDefaultTypes();
    TestOrcStruct test5 = new TestOrcStruct();
    test5.testStruct();
    test5.testInspectorFromTypeInfo();
    test5.testUnion();
  }
}
