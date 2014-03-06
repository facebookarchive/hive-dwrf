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

import static com.facebook.hive.orc.OrcTestUtils.byteBuf;
import static com.facebook.hive.orc.OrcTestUtils.bytes;
import static com.facebook.hive.orc.OrcTestUtils.inner;
import static com.facebook.hive.orc.OrcTestUtils.list;
import static com.facebook.hive.orc.OrcTestUtils.map;
import static junit.framework.Assert.assertEquals;
import static junit.framework.Assert.assertNotNull;
import static junit.framework.Assert.assertNull;
import static junit.framework.Assert.assertTrue;

import java.io.File;
import java.io.IOException;
import java.nio.ByteBuffer;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Random;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.facebook.hive.orc.OrcTestUtils.BigRow;
import com.facebook.hive.orc.OrcTestUtils.DoubleRow;
import com.facebook.hive.orc.OrcTestUtils.InnerStruct;
import com.facebook.hive.orc.OrcTestUtils.MiddleStruct;
import com.facebook.hive.orc.OrcTestUtils.ReallyBigRow;
import com.facebook.hive.orc.lazy.OrcLazyBinary;
import com.facebook.hive.orc.lazy.OrcLazyBoolean;
import com.facebook.hive.orc.lazy.OrcLazyByte;
import com.facebook.hive.orc.lazy.OrcLazyDouble;
import com.facebook.hive.orc.lazy.OrcLazyFloat;
import com.facebook.hive.orc.lazy.OrcLazyInt;
import com.facebook.hive.orc.lazy.OrcLazyList;
import com.facebook.hive.orc.lazy.OrcLazyLong;
import com.facebook.hive.orc.lazy.OrcLazyMap;
import com.facebook.hive.orc.lazy.OrcLazyObject;
import com.facebook.hive.orc.lazy.OrcLazyObjectInspectorUtils;
import com.facebook.hive.orc.lazy.OrcLazyRow;
import com.facebook.hive.orc.lazy.OrcLazyShort;
import com.facebook.hive.orc.lazy.OrcLazyString;
import com.facebook.hive.orc.lazy.OrcLazyStruct;
import com.facebook.hive.orc.lazy.OrcLazyTimestamp;
import com.facebook.hive.orc.lazy.OrcLazyUnion;

/**
 * Tests for the top level reader/streamFactory of ORC files.
 */
public class TestOrcFile {

  Path workDir = new Path(System.getProperty("test.tmp.dir",
      "target" + File.separator + "test" + File.separator + "tmp"));

  Configuration conf;
  FileSystem fs;
  Path testFilePath, testFilePath2;

  @Rule
  public TestName testCaseName = new TestName();

  @Before
  public void openFileSystem () throws Exception {
    conf = new Configuration();
    fs = FileSystem.getLocal(conf);
    testFilePath = new Path(workDir, "TestOrcFile." +
        testCaseName.getMethodName() + ".orc");
    testFilePath2 = new Path(workDir, "TestOrcFile2." +
        testCaseName.getMethodName() + ".orc");
    fs.delete(testFilePath, false);
    fs.delete(testFilePath2, false);
  }

  @Test
  public void testHash() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.ZLIB, 10000, 10000);
    writer.addRow(new BigRow(false, (byte) 1, (short) 1, 1,
        1L, (float) 1.0, 1.0, bytes(1), "1",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map(inner(3, "good"), inner(4, "bad"))));
    writer.addRow(new BigRow(null, null, null, null,
        null, null, null, null, null, null, null, null));
    writer.close();
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    RecordReader rows = reader.rows(null);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(1, row.getFieldValue(0).hashCode());
    assertEquals(1, row.getFieldValue(1).hashCode());
    assertEquals(1, row.getFieldValue(2).hashCode());
    assertEquals(1, row.getFieldValue(3).hashCode());
    assertEquals(1, row.getFieldValue(4).hashCode());
    assertEquals(1065353216, row.getFieldValue(5).hashCode());
    assertEquals(1072693248, row.getFieldValue(6).hashCode());
    assertEquals(32, row.getFieldValue(7).hashCode());
    assertEquals(80, row.getFieldValue(8).hashCode());
    assertEquals(8417130, row.getFieldValue(9).hashCode());
    assertEquals(127296452, row.getFieldValue(10).hashCode());
    assertEquals(7, row.getFieldValue(11).hashCode());

    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(0, row.getFieldValue(0).hashCode());
    assertEquals(0, row.getFieldValue(1).hashCode());
    assertEquals(0, row.getFieldValue(2).hashCode());
    assertEquals(0, row.getFieldValue(3).hashCode());
    assertEquals(0, row.getFieldValue(4).hashCode());
    assertEquals(0, row.getFieldValue(5).hashCode());
    assertEquals(0, row.getFieldValue(6).hashCode());
    assertEquals(0, row.getFieldValue(7).hashCode());
    assertEquals(0, row.getFieldValue(8).hashCode());
    assertEquals(0, row.getFieldValue(9).hashCode());
    assertEquals(0, row.getFieldValue(10).hashCode());
    assertEquals(0, row.getFieldValue(11).hashCode());
  }



  @Test
  public void testSeekAcrossChunks() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (DoubleRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    // Create a table consisting of a single column of doubles
    // Add enough values to it to get 3 index strides (doubles are 8 bytes) more is ok
    // Note that the compression buffer size and index stride length are very important
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector, 2097152,
        CompressionKind.ZLIB, 262144, 10000);
    Random rand = new Random(42);
    double[] values = new double[131702];

    // The first compression block is all 0's
    for (int i = 0; i < 32768; i++) {
      values[i] = 0;
      writer.addRow(new DoubleRow(values[i]));
    }

    // The second compression block is random doubles
    for (int i = 0; i < 32768; i++) {
      values[i + 32768] = rand.nextDouble();
      writer.addRow(new DoubleRow(values[i + 32768]));
    }

    // The third compression block is all 0's
    // (important so it compresses to the same size as the first)
    for (int i = 0; i < 32768; i++) {
      values[i + 32768 + 32768] = 0;
      writer.addRow(new DoubleRow(values[i + 32768 + 32768]));
    }

    // The fourth compression block is random
    for (int i = 0; i < 32768; i++) {
      values[i + 32768 + 32768 + 32768] = rand.nextDouble();
      writer.addRow(new DoubleRow(values[i + 32768 + 32768 + 32768]));
    }

    writer.close();
    OrcConf.setIntVar(conf, OrcConf.ConfVars.HIVE_ORC_READ_COMPRESSION_STRIDES, 2);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);

    StructObjectInspector readerInspector = (StructObjectInspector) reader.getObjectInspector();
    List<? extends StructField> fields = readerInspector.getAllStructFieldRefs();
    DoubleObjectInspector columnInspector =
      (DoubleObjectInspector) fields.get(0).getFieldObjectInspector();

    RecordReader rows = reader.rows(null);
    Object row = null;

    // Skip enough values to get to the 2nd index stride in the first chunk
    for (int i = 0; i < 40001; i++) {
      row = rows.next(row);
    }

    // This will set previousOffset to be the size of the first compression block and the
    // compressionOffset to some other value (doesn't matter what point is it's different from the
    // start of the compression block)
    assertEquals(values[40000], columnInspector.get(readerInspector.getStructFieldData(row,
        fields.get(0))));

    // Skip enough values to get to the 2nd index stride of the second chunk
    for (int i = 0; i < 80000; i++) {
      rows.next(row);
    }

    // When seek is called, previousOffset will equal newCompressedOffset since the former is the
    // the length of the first compression block and the latter is the length of the third
    // compression block (remember the chunks contain 2 index strides), so if we only check this
    // (or for some other reason) we will not adjust compressedIndex, we will read the wrong data
    assertEquals(values[120000], columnInspector.get(readerInspector.getStructFieldData(row, fields.get(0))));

    rows.close();
  }

  @Test
  public void test1() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        100000, CompressionKind.ZLIB, 10000, 10000);
    writer.addRow(new BigRow(false, (byte) 1, (short) 1024, 65536,
        Long.MAX_VALUE, (float) 1.0, -15.0, bytes(0,1,2,3,4), "hi",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(3, "good"), inner(4, "bad")),
        map()));
    writer.addRow(new BigRow(true, (byte) 100, (short) 2048, 65536,
        Long.MAX_VALUE, (float) 2.0, -5.0, bytes(), "bye",
        new MiddleStruct(inner(1, "bye"), inner(2, "sigh")),
        list(inner(100000000, "cat"), inner(-100000, "in"), inner(1234, "hat")),
        map(inner(5,"chani"), inner(1,"mauddib"))));
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);

    // check the stats
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(2, stats[1].getNumberOfValues());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getFalseCount());
    assertEquals(1, ((BooleanColumnStatistics) stats[1]).getTrueCount());
    assertEquals("count: 2 true: 1", stats[1].toString());

    assertEquals(2048, ((IntegerColumnStatistics) stats[3]).getMaximum());
    assertEquals(1024, ((IntegerColumnStatistics) stats[3]).getMinimum());
    assertEquals(true, ((IntegerColumnStatistics) stats[3]).isSumDefined());
    assertEquals(3072, ((IntegerColumnStatistics) stats[3]).getSum());
    assertEquals("count: 2 min: 1024 max: 2048 sum: 3072",
        stats[3].toString());

    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMaximum());
    assertEquals(Long.MAX_VALUE,
        ((IntegerColumnStatistics) stats[5]).getMinimum());
    assertEquals(false, ((IntegerColumnStatistics) stats[5]).isSumDefined());
    assertEquals("count: 2 min: 9223372036854775807 max: 9223372036854775807",
        stats[5].toString());

    assertEquals(-15.0, ((DoubleColumnStatistics) stats[7]).getMinimum());
    assertEquals(-5.0, ((DoubleColumnStatistics) stats[7]).getMaximum());
    assertEquals(-20.0, ((DoubleColumnStatistics) stats[7]).getSum(), 0.00001);
    assertEquals("count: 2 min: -15.0 max: -5.0 sum: -20.0",
        stats[7].toString());

    assertEquals("count: 2 min: bye max: hi", stats[9].toString());

    // check the inspectors
    StructObjectInspector readerInspector =
        (StructObjectInspector) reader.getObjectInspector();
    assertEquals(ObjectInspector.Category.STRUCT,
        readerInspector.getCategory());
    assertEquals("struct<boolean1:boolean,byte1:tinyint,short1:smallint,"
        + "int1:int,long1:bigint,float1:float,double1:double,bytes1:"
        + "binary,string1:string,middle:struct<list:array<struct<int1:int,"
        + "string1:string>>>,list:array<struct<int1:int,string1:string>>,"
        + "map:map<string,struct<int1:int,string1:string>>>",
        readerInspector.getTypeName());
    List<? extends StructField> fields =
        readerInspector.getAllStructFieldRefs();
    BooleanObjectInspector bo = (BooleanObjectInspector) readerInspector.
        getStructFieldRef("boolean1").getFieldObjectInspector();
    ByteObjectInspector by = (ByteObjectInspector) readerInspector.
        getStructFieldRef("byte1").getFieldObjectInspector();
    ShortObjectInspector sh = (ShortObjectInspector) readerInspector.
        getStructFieldRef("short1").getFieldObjectInspector();
    IntObjectInspector in = (IntObjectInspector) readerInspector.
        getStructFieldRef("int1").getFieldObjectInspector();
    LongObjectInspector lo = (LongObjectInspector) readerInspector.
        getStructFieldRef("long1").getFieldObjectInspector();
    FloatObjectInspector fl = (FloatObjectInspector) readerInspector.
        getStructFieldRef("float1").getFieldObjectInspector();
    DoubleObjectInspector dbl = (DoubleObjectInspector) readerInspector.
        getStructFieldRef("double1").getFieldObjectInspector();
    BinaryObjectInspector bi = (BinaryObjectInspector) readerInspector.
        getStructFieldRef("bytes1").getFieldObjectInspector();
    StringObjectInspector st = (StringObjectInspector) readerInspector.
        getStructFieldRef("string1").getFieldObjectInspector();
    StructObjectInspector mid = (StructObjectInspector) readerInspector.
        getStructFieldRef("middle").getFieldObjectInspector();
    List<? extends StructField> midFields =
        mid.getAllStructFieldRefs();
    ListObjectInspector midli =
        (ListObjectInspector) midFields.get(0).getFieldObjectInspector();
    StructObjectInspector inner = (StructObjectInspector)
        midli.getListElementObjectInspector();
    List<? extends StructField> inFields = inner.getAllStructFieldRefs();
    IntObjectInspector inner_in = (IntObjectInspector) inFields.get(0).getFieldObjectInspector();
    StringObjectInspector inner_st = (StringObjectInspector) inFields.get(1).getFieldObjectInspector();
    ListObjectInspector li = (ListObjectInspector) readerInspector.
        getStructFieldRef("list").getFieldObjectInspector();
    MapObjectInspector ma = (MapObjectInspector) readerInspector.
        getStructFieldRef("map").getFieldObjectInspector();
    StructObjectInspector lc = (StructObjectInspector)
        li.getListElementObjectInspector();
    StringObjectInspector mk = (StringObjectInspector)
        ma.getMapKeyObjectInspector();
    StructObjectInspector mv = (StructObjectInspector)
        ma.getMapValueObjectInspector();
    RecordReader rows = reader.rows(null);
    Object row = rows.next(null);
    assertNotNull(row);
    // check the contents of the first row
    assertEquals(false,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(1, by.get(readerInspector.getStructFieldData(row,
        fields.get(1))));
    assertEquals(1024, sh.get(readerInspector.getStructFieldData(row,
        fields.get(2))));
    assertEquals(65536, in.get(readerInspector.getStructFieldData(row,
        fields.get(3))));
    assertEquals(Long.MAX_VALUE, lo.get(readerInspector.
        getStructFieldData(row, fields.get(4))));
    assertEquals(1.0, fl.get(readerInspector.getStructFieldData(row,
        fields.get(5))), 0.00001);
    assertEquals(-15.0, dbl.get(readerInspector.getStructFieldData(row,
        fields.get(6))), 0.00001);
    assertEquals(bytes(0,1,2,3,4), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(7))));
    assertEquals("hi", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(8))));
    List<?> midRow = midli.getList(mid.getStructFieldData(readerInspector.
        getStructFieldData(row, fields.get(9)), midFields.get(0)));
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, inner_in.get(inner.getStructFieldData(midRow.get(0),
        inFields.get(0))));
    assertEquals("bye", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(0), inFields.get(1))));
    assertEquals(2, inner_in.get(inner.getStructFieldData(midRow.get(1),
        inFields.get(0))));
    assertEquals("sigh", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(1), inFields.get(1))));
    List<?> list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    assertEquals(2, list.size());
    assertEquals(3, inner_in.get(inner.getStructFieldData(list.get(0),
        inFields.get(0))));
    assertEquals("good", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(0), inFields.get(1))));
    assertEquals(4, inner_in.get(inner.getStructFieldData(list.get(1),
        inFields.get(0))));
    assertEquals("bad", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(1), inFields.get(1))));
    Map<?,?> map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    assertEquals(0, map.size());

    // check the contents of second row
    assertEquals(true, rows.hasNext());
    row = rows.next(row);
    assertEquals(true,
        bo.get(readerInspector.getStructFieldData(row, fields.get(0))));
    assertEquals(100, by.get(readerInspector.getStructFieldData(row,
        fields.get(1))));
    assertEquals(2048, sh.get(readerInspector.getStructFieldData(row,
        fields.get(2))));
    assertEquals(65536, in.get(readerInspector.getStructFieldData(row,
        fields.get(3))));
    assertEquals(Long.MAX_VALUE, lo.get(readerInspector.
        getStructFieldData(row, fields.get(4))));
    assertEquals(2.0, fl.get(readerInspector.getStructFieldData(row,
        fields.get(5))), 0.00001);
    assertEquals(-5.0, dbl.get(readerInspector.getStructFieldData(row,
        fields.get(6))), 0.00001);
    assertEquals(bytes(), bi.getPrimitiveWritableObject(
        readerInspector.getStructFieldData(row, fields.get(7))));
    assertEquals("bye", st.getPrimitiveJavaObject(readerInspector.
        getStructFieldData(row, fields.get(8))));
    midRow = midli.getList(mid.getStructFieldData(readerInspector.
        getStructFieldData(row, fields.get(9)), midFields.get(0)));
    assertNotNull(midRow);
    assertEquals(2, midRow.size());
    assertEquals(1, inner_in.get(inner.getStructFieldData(midRow.get(0),
        inFields.get(0))));
    assertEquals("bye", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(0), inFields.get(1))));
    assertEquals(2, inner_in.get(inner.getStructFieldData(midRow.get(1),
        inFields.get(0))));
    assertEquals("sigh", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (midRow.get(1), inFields.get(1))));
    list = li.getList(readerInspector.getStructFieldData(row,
        fields.get(10)));
    assertEquals(3, list.size());
    assertEquals(100000000, inner_in.get(inner.getStructFieldData(list.get(0),
        inFields.get(0))));
    assertEquals("cat", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(0), inFields.get(1))));
    assertEquals(-100000, inner_in.get(inner.getStructFieldData(list.get(1),
        inFields.get(0))));
    assertEquals("in", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(1), inFields.get(1))));
    assertEquals(1234, inner_in.get(inner.getStructFieldData(list.get(2),
        inFields.get(0))));
    assertEquals("hat", inner_st.getPrimitiveJavaObject(inner.getStructFieldData
        (list.get(2), inFields.get(1))));
    map = ma.getMap(readerInspector.getStructFieldData(row,
        fields.get(11)));
    assertEquals(2, map.size());
    boolean[] found = new boolean[2];
    for(Object key: map.keySet()) {
      String str = mk.getPrimitiveJavaObject(key);
      if (str.equals("chani")) {
        assertEquals(false, found[0]);
        assertEquals(5, inner_in.get(inner.getStructFieldData(map.get(key),
            inFields.get(0))));
        assertEquals(str, inner_st.getPrimitiveJavaObject(
            inner.getStructFieldData(map.get(key), inFields.get(1))));
        found[0] = true;
      } else if (str.equals("mauddib")) {
        assertEquals(false, found[1]);
        assertEquals(1, inner_in.get(inner.getStructFieldData(map.get(key),
            inFields.get(0))));
        assertEquals(str, inner_st.getPrimitiveJavaObject(
            inner.getStructFieldData(map.get(key), inFields.get(1))));
        found[1] = true;
      } else {
        throw new IllegalArgumentException("Unknown key " + str);
      }
    }
    assertEquals(true, found[0]);
    assertEquals(true, found[1]);

    // handle the close up
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  @Test
  public void testColumnProjection() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        1000, CompressionKind.NONE, 100, 1000);
    Random r1 = new Random(1);
    Random r2 = new Random(2);
    int x;
    int minInt=0, maxInt=0;
    String y;
    String minStr = null, maxStr = null;
    for(int i=0; i < 21000; ++i) {
      x = r1.nextInt();
      y = Long.toHexString(r2.nextLong());
      if (i == 0 || x < minInt) {
        minInt = x;
      }
      if (i == 0 || x > maxInt) {
        maxInt = x;
      }
      if (i == 0 || y.compareTo(minStr) < 0) {
        minStr = y;
      }
      if (i == 0 || y.compareTo(maxStr) > 0) {
        maxStr = y;
      }
      writer.addRow(inner(x, y));
    }
    writer.close();
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);

    // check out the statistics
    ColumnStatistics[] stats = reader.getStatistics();
    assertEquals(3, stats.length);
    for(ColumnStatistics s: stats) {
      assertEquals(21000, s.getNumberOfValues());
      if (s instanceof IntegerColumnStatistics) {
        assertEquals(minInt, ((IntegerColumnStatistics) s).getMinimum());
        assertEquals(maxInt, ((IntegerColumnStatistics) s).getMaximum());
      } else if (s instanceof  StringColumnStatistics) {
        assertEquals(maxStr, ((StringColumnStatistics) s).getMaximum());
        assertEquals(minStr, ((StringColumnStatistics) s).getMinimum());
      }
    }

    // check out the types
    List<OrcProto.Type> types = reader.getTypes();
    assertEquals(3, types.size());
    assertEquals(OrcProto.Type.Kind.STRUCT, types.get(0).getKind());
    assertEquals(2, types.get(0).getSubtypesCount());
    assertEquals(1, types.get(0).getSubtypes(0));
    assertEquals(2, types.get(0).getSubtypes(1));
    assertEquals(OrcProto.Type.Kind.INT, types.get(1).getKind());
    assertEquals(0, types.get(1).getSubtypesCount());
    assertEquals(OrcProto.Type.Kind.STRING, types.get(2).getKind());
    assertEquals(0, types.get(2).getSubtypesCount());

    // read the contents and make sure they match
    RecordReader rows1 = reader.rows(new boolean[]{true, true, false});
    RecordReader rows2 = reader.rows(new boolean[]{true, false, true});
    r1 = new Random(1);
    r2 = new Random(2);
    OrcLazyStruct row1 = null;
    OrcLazyStruct row2 = null;
    for(int i = 0; i < 21000; ++i) {
      assertEquals(true, rows1.hasNext());
      assertEquals(true, rows2.hasNext());
      row1 = (OrcLazyStruct) rows1.next(row1);
      row2 = (OrcLazyStruct) rows2.next(row2);
      assertEquals(r1.nextInt(), ((IntWritable) ((OrcLazyInt) ((OrcStruct) row1.materialize()).getFieldValue(0)).materialize()).get());
      assertEquals(Long.toHexString(r2.nextLong()),
          ((OrcLazyString) ((OrcStruct) row2.materialize()).getFieldValue(1)).materialize().toString());
    }
    assertEquals(false, rows1.hasNext());
    assertEquals(false, rows2.hasNext());
    rows1.close();
    rows2.close();
  }

  @Test
  public void testEmptyFile() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        1000, CompressionKind.NONE, 100, 10000);
    writer.close();
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(false, reader.rows(null).hasNext());
    assertEquals(CompressionKind.NONE, reader.getCompression());
    assertEquals(0, reader.getNumberOfRows());
    assertEquals(0, reader.getCompressionSize());
    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(3, reader.getContentLength());
    assertEquals(false, reader.getStripes().iterator().hasNext());
  }

  @Test
  public void testMetaData() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        1000, CompressionKind.NONE, 100, 10000);
    writer.addUserMetadata("my.meta", byteBuf(1, 2, 3, 4, 5, 6, 7, -1, -2, 127, -128));
    writer.addUserMetadata("clobber", byteBuf(1,2,3));
    writer.addUserMetadata("clobber", byteBuf(4,3,2,1));
    ByteBuffer bigBuf = ByteBuffer.allocate(40000);
    Random random = new Random(0);
    random.nextBytes(bigBuf.array());
    writer.addUserMetadata("big", bigBuf);
    bigBuf.position(0);
    writer.addRow(new BigRow(true, (byte) 127, (short) 1024, 42,
        42L * 1024 * 1024 * 1024, (float) 3.1415, -2.713, null,
        null, null, null, null));
    writer.addUserMetadata("clobber", byteBuf(5,7,11,13,17,19));
    writer.close();
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(byteBuf(5,7,11,13,17,19), reader.getMetadataValue("clobber"));
    assertEquals(byteBuf(1,2,3,4,5,6,7,-1,-2,127,-128),
        reader.getMetadataValue("my.meta"));
    assertEquals(bigBuf, reader.getMetadataValue("big"));
    try {
      reader.getMetadataValue("unknown");
      assertTrue(false);
    } catch (IllegalArgumentException iae) {
      // PASS
    }
    int i = 0;
    for(String key: reader.getMetadataKeys()) {
      if ("my.meta".equals(key) ||
          "clobber".equals(key) ||
          "big".equals(key)) {
        i += 1;
      } else {
        throw new IllegalArgumentException("unknown key " + key);
      }
    }
    assertEquals(3, i);
  }

  /**
   * We test union and timestamp separately since we need to make the
   * object inspector manually. (The Hive reflection-based doesn't handle
   * them properly.)
   */
  @Test
  public void testUnionAndTimestamp() throws Exception {
    List<OrcProto.Type> types = new ArrayList<OrcProto.Type>();
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRUCT).
        addFieldNames("time").addFieldNames("union").
        addSubtypes(1).addSubtypes(2).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.TIMESTAMP).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.UNION).
        addSubtypes(3).addSubtypes(4).build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.INT).
        build());
    types.add(OrcProto.Type.newBuilder().setKind(OrcProto.Type.Kind.STRING).
        build());

    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = OrcLazyObjectInspectorUtils.createWritableObjectInspector(0, types);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        1000, CompressionKind.NONE, 100, 10000);
    OrcStruct row = new OrcStruct(types.get(0).getFieldNamesList());
    OrcUnion union = new OrcUnion();
    row.setFieldValue(1, union);
    row.setFieldValue(0, Timestamp.valueOf("2000-03-12 15:00:00"));
    union.set((byte) 0, new IntWritable(42));
    writer.addRow(row);
    row.setFieldValue(0, Timestamp.valueOf("2000-03-20 12:00:00.123456789"));
    union.set((byte)1, new Text("hello"));
    writer.addRow(row);
    row.setFieldValue(0, null);
    row.setFieldValue(1, null);
    writer.addRow(row);
    row.setFieldValue(1, union);
    union.set((byte) 0, null);
    writer.addRow(row);
    union.set((byte) 1, null);
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(200000));
    row.setFieldValue(0, Timestamp.valueOf("1900-01-01 00:00:00"));
    writer.addRow(row);
    for(int i=1900; i < 2200; ++i) {
      row.setFieldValue(0, Timestamp.valueOf(i + "-05-05 12:34:56." + i));
      if ((i & 1) == 0) {
        union.set((byte) 0, new IntWritable(i*i));
      } else {
        union.set((byte) 1, new Text(new Integer(i*i).toString()));
      }
      writer.addRow(row);
    }
    // let's add a lot of constant rows to test the rle
    row.setFieldValue(0, null);
    union.set((byte) 0, new IntWritable(1732050807));
    for(int i=0; i < 5000; ++i) {
      writer.addRow(row);
    }
    union.set((byte) 0, new IntWritable(0));
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(10));
    writer.addRow(row);
    union.set((byte) 0, new IntWritable(138));
    writer.addRow(row);
    writer.close();
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(false, reader.getMetadataKeys().iterator().hasNext());
    assertEquals(5309, reader.getNumberOfRows());
    int stripeCount = 0;
    int rowCount = 0;
    long currentOffset = -1;
    for(StripeInformation stripe: reader.getStripes()) {
      stripeCount += 1;
      rowCount += stripe.getNumberOfRows();
      if (currentOffset < 0) {
        currentOffset = stripe.getOffset() + stripe.getIndexLength() +
            stripe.getDataLength() + stripe.getFooterLength();
      } else {
        assertEquals(currentOffset, stripe.getOffset());
        currentOffset += stripe.getIndexLength() +
            stripe.getDataLength() + stripe.getFooterLength();
      }
    }
    assertEquals(reader.getNumberOfRows(), rowCount);
    assertEquals(2, stripeCount);
    assertEquals(reader.getContentLength(), currentOffset);
    RecordReader rows = reader.rows(null);
    assertEquals(0, rows.getRowNumber());
    assertEquals(0.0, rows.getProgress(), 0.000001);
    assertEquals(true, rows.hasNext());
    OrcLazyStruct lazyRow = (OrcLazyStruct) rows.next(null);
    row = (OrcStruct) lazyRow.materialize();
    inspector = reader.getObjectInspector();
    assertEquals("struct<time:timestamp,union:uniontype<int,string>>",
        inspector.getTypeName());
    assertEquals(Timestamp.valueOf("2000-03-12 15:00:00"),
        ((TimestampWritable) ((OrcLazyTimestamp) row.getFieldValue(0)).materialize()).getTimestamp());
    union = (OrcUnion) ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(0, union.getTag());
    assertEquals(new IntWritable(42), union.getObject());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(Timestamp.valueOf("2000-03-20 12:00:00.123456789"),
        ((TimestampWritable) ((OrcLazyTimestamp) row.getFieldValue(0)).materialize()).getTimestamp());
    ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(1, union.getTag());
    assertEquals(new Text("hello"), union.getObject());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(null, ((OrcLazyObject) row.getFieldValue(0)).materialize());
    assertEquals(null, ((OrcLazyObject) row.getFieldValue(1)).materialize());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(null, ((OrcLazyObject) row.getFieldValue(0)).materialize());
    union = (OrcUnion) ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(0, union.getTag());
    assertEquals(null, union.getObject());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(null, ((OrcLazyObject) row.getFieldValue(0)).materialize());
    ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(1, union.getTag());
    assertEquals(null, union.getObject());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    assertEquals(Timestamp.valueOf("1900-01-01 00:00:00"),
        ((TimestampWritable) ((OrcLazyTimestamp) row.getFieldValue(0)).materialize()).getTimestamp());
    ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(new IntWritable(200000), union.getObject());
    for(int i=1900; i < 2200; ++i) {
      lazyRow = (OrcLazyStruct) rows.next(lazyRow);
      row = (OrcStruct) lazyRow.materialize();
      assertEquals(Timestamp.valueOf(i + "-05-05 12:34:56." + i),
          ((TimestampWritable) ((OrcLazyTimestamp) row.getFieldValue(0)).materialize()).getTimestamp());
      ((OrcLazyUnion) row.getFieldValue(1)).materialize();
      if ((i & 1) == 0) {
        assertEquals(0, union.getTag());
        assertEquals(new IntWritable(i*i), union.getObject());
      } else {
        assertEquals(1, union.getTag());
        assertEquals(new Text(new Integer(i*i).toString()), union.getObject());
      }
    }
    for(int i=0; i < 5000; ++i) {
      lazyRow = (OrcLazyStruct) rows.next(lazyRow);
      row = (OrcStruct) lazyRow.materialize();
      ((OrcLazyUnion) row.getFieldValue(1)).materialize();
      assertEquals(new IntWritable(1732050807), union.getObject());
    }
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(new IntWritable(0), union.getObject());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(new IntWritable(10), union.getObject());
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    ((OrcLazyUnion) row.getFieldValue(1)).materialize();
    assertEquals(new IntWritable(138), union.getObject());
    assertEquals(false, rows.hasNext());
    assertEquals(1.0, rows.getProgress(), 0.00001);
    assertEquals(reader.getNumberOfRows(), rows.getRowNumber());
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @Test
  public void testSnappy() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        1000, CompressionKind.SNAPPY, 100, 10000);
    Random rand = new Random(12);
    for(int i=0; i < 10000; ++i) {
      writer.addRow(new InnerStruct(rand.nextInt(),
          Integer.toHexString(rand.nextInt())));
    }
    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    RecordReader rows = reader.rows(null);
    rand = new Random(12);
    OrcLazyStruct row = null;
    for(int i=0; i < 10000; ++i) {
      assertEquals(true, rows.hasNext());
      row = (OrcLazyStruct) rows.next(row);
      assertEquals(rand.nextInt(), ((IntWritable) ((OrcLazyInt) ((OrcStruct) row.materialize()).getFieldValue(0)).materialize()).get());
      assertEquals(Integer.toHexString(rand.nextInt()),
          ((OrcLazyString) ((OrcStruct) row.materialize()).getFieldValue(1)).materialize().toString());
    }
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  /**
   * Read and write a randomly generated snappy file.
   * @throws Exception
   */
  @Test
  public void testWithoutIndex() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }

    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        5000, CompressionKind.SNAPPY, 1000, 0);
    Random rand = new Random(24);
    for(int i=0; i < 10000; ++i) {
      InnerStruct row = new InnerStruct(rand.nextInt(),
          Integer.toBinaryString(rand.nextInt()));
      for(int j=0; j< 5; ++j) {
        writer.addRow(row);
      }
    }
    writer.close();
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(50000, reader.getNumberOfRows());
    assertEquals(0, reader.getRowIndexStride());
    StripeInformation stripe = reader.getStripes().iterator().next();
    assertEquals(true, stripe.getDataLength() != 0);
    assertEquals(0, stripe.getIndexLength());
    RecordReader rows = reader.rows(null);
    rand = new Random(24);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    for(int i=0; i < 10000; ++i) {
      int intVal = rand.nextInt();
      String strVal = Integer.toBinaryString(rand.nextInt());
      for(int j=0; j < 5; ++j) {
        assertEquals(true, rows.hasNext());
        lazyRow = (OrcLazyStruct) rows.next(lazyRow);
        row = (OrcStruct) lazyRow.materialize();
        assertEquals(intVal, ((IntWritable) ((OrcLazyInt) row.getFieldValue(0)).materialize()).get());
        assertEquals(strVal, ((OrcLazyString) row.getFieldValue(1)).materialize().toString());
      }
    }
    assertEquals(false, rows.hasNext());
    rows.close();
  }

  private static class RandomRowInputs {
    long[] intValues;
    double[] doubleValues;
    String[] stringValues;
    BytesWritable[] byteValues;
    String[] words = new String[128];

    public RandomRowInputs(int count) {
      intValues= new long[count];
      doubleValues = new double[count];
      stringValues = new String[count];
      byteValues = new BytesWritable[count];
    }
  }

  private RandomRowInputs writeRandomRows(int count, boolean lowMemoryMode) throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (ReallyBigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    OrcConf.setBoolVar(conf, OrcConf.ConfVars.HIVE_ORC_FILE_ENABLE_LOW_MEMORY_MODE, lowMemoryMode);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        4000000, CompressionKind.ZLIB, 65536, 1000,
        new MemoryManager(conf));
    Random rand = new Random(42);
    RandomRowInputs inputs = new RandomRowInputs(count);
    long[] intValues = inputs.intValues;
    double[] doubleValues = inputs.doubleValues;
    String[] stringValues = inputs.stringValues;
    BytesWritable[] byteValues = inputs.byteValues;
    String[] words = inputs.words;
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < count/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = rand.nextLong();
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = words[rand.nextInt(words.length)];
    }
    for(int i=0; i < count; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < count; ++i) {
      ReallyBigRow bigrow = createRandomRow(intValues, doubleValues, stringValues,
          byteValues, words, i);
      writer.addRow(bigrow);
    }
    writer.close();
    writer = null;
    return inputs;
  }

  private static enum NumberOfNulls {
    // No nulls
    NONE,
    // Every nth value is null
    SOME,
    // Every nth value is NOT null
    MANY
  }

  private void compareRows(OrcStruct row, RandomRowInputs inputs, int rowNumber, NumberOfNulls numNulls) throws Exception {
    ReallyBigRow expected = null ;
    switch (numNulls) {
      case MANY:
      case SOME:
        expected = createRandomRowWithNulls(inputs.intValues, inputs.doubleValues,
        inputs.stringValues, inputs.byteValues, inputs.words, rowNumber, numNulls);
        break;
      case NONE:
        expected = createRandomRow(inputs.intValues, inputs.doubleValues,
            inputs.stringValues, inputs.byteValues, inputs.words, rowNumber);
        break;
    }
    OrcLazyBoolean lazyboolean1 = (OrcLazyBoolean) row.getFieldValue(0);
    if (lazyboolean1.nextIsNull()) {
      assertNull(expected.boolean1);
    } else {
      assertEquals(expected.boolean1.booleanValue(),
                   ((BooleanWritable) lazyboolean1.materialize()).get());
    }
    try {
      boolean b1 = lazyboolean1.materializeBoolean();
      assertEquals(expected.boolean1.booleanValue(), b1);
    }
    catch(IOException e) {
      assert(lazyboolean1.nextIsNull());
      assertNull(expected.boolean1);
    }

    if (((OrcLazyObject) row.getFieldValue(1)).nextIsNull()) {
      assertNull(expected.byte1);
    } else {
      assertEquals(expected.byte1.byteValue(),
          ((ByteWritable) ((OrcLazyByte) row.getFieldValue(1)).materialize()).get());
    }

    OrcLazyShort short1 = (OrcLazyShort) row.getFieldValue(2);
    try
    {
      short s1 = short1.materializeShort();
      assertEquals(expected.short1.shortValue(), s1);
    }
    catch(IOException e) {
      assert(short1.nextIsNull());
      assertNull(expected.short1);
    }
    if (short1.nextIsNull()) {
      assertNull(expected.short1);
    } else {
      assertEquals(expected.short1.shortValue(),
                   ((ShortWritable) short1.materialize()).get());
    }


    OrcLazyInt int1 = (OrcLazyInt)row.getFieldValue(3);
    if (int1.nextIsNull()) {
      assertNull(expected.int1);
    } else {
      assertEquals(expected.int1.intValue(),
                   ((IntWritable) int1.materialize()).get());
    }
    try {
      int i1 = int1.materializeInt();
      assertEquals(expected.int1.intValue(), i1);
    }
    catch(IOException e) {
      assert(int1.nextIsNull());
      assertNull(expected.int1);
    }

    OrcLazyLong long1 = (OrcLazyLong)row.getFieldValue(4);
    if (long1.nextIsNull()) {
      assertNull(expected.long1);
    } else {
      assertEquals(expected.long1.longValue(),
                   ((LongWritable) long1.materialize()).get());
    }
    try {
      long l1 =  long1.materializeLong();
      assertEquals(expected.long1.longValue(), l1);
    }
    catch(IOException e) {
      assert(long1.nextIsNull());
      assertNull(expected.long1);
    }

    OrcLazyShort short2 = (OrcLazyShort)row.getFieldValue(5);
    try {
      short s2 = short2.materializeShort();
      assertEquals(expected.short2.shortValue(), s2);
    }
    catch(IOException e) {
      assert(short2.nextIsNull());
      assertNull(expected.short2);
    }
    if (short2.nextIsNull()) {
      assertNull(expected.short2);
    } else {
      assertEquals(expected.short2.shortValue(),
                   ((ShortWritable) short2.materialize()).get());
    }

    OrcLazyInt int2 = (OrcLazyInt) row.getFieldValue(6);
    try {
      int i2 = int2.materializeInt();
      assertEquals(expected.int2.intValue(), i2);
    }
    catch(IOException e) {
      assert(int2.nextIsNull());
      assertNull(expected.int2);
    }
    if (int2.nextIsNull()) {
      assertNull(expected.int2);
    } else {
      assertEquals(expected.int2.intValue(),
                   ((IntWritable) int2.materialize()).get());
    }

    OrcLazyLong long2 = (OrcLazyLong) row.getFieldValue(7);
    try {
      long l2 = long2.materializeLong();
      assertEquals(expected.long2.longValue(), l2);
    }
    catch(IOException e) {
      assert(long2.nextIsNull());
      assertNull(expected.long2);
    }
    if (long2.nextIsNull()) {
      assertNull(expected.long2);
    } else {
      assertEquals(expected.long2.longValue(),
                   ((LongWritable) long2.materialize()).get());
    }

    OrcLazyFloat float1 = (OrcLazyFloat) row.getFieldValue(8);
    if (float1.nextIsNull()) {
      assertNull(expected.float1);
    } else {
      assertEquals(expected.float1.floatValue(),
                   ((FloatWritable) float1.materialize()).get(), 0.0001);
    }
    try {
      float f1 = float1.materializeFloat();
      assertEquals(expected.float1.floatValue(), f1, 0.0001);
    }
    catch(IOException e) {
      assert(float1.nextIsNull());
      assertNull(expected.float1);
    }

    OrcLazyDouble double1 = (OrcLazyDouble) row.getFieldValue(9);
    try {
      double d1 = double1.materializeDouble();
      assertEquals(expected.double1.doubleValue(), d1, 0.0001);
    }
    catch(IOException e) {
      assert(double1.nextIsNull());
      assertNull(expected.double1);
    }
    if (double1.nextIsNull()) {
      assertNull(expected.double1);
    } else {
      assertEquals(expected.double1.doubleValue(),
                   ((DoubleWritable) double1.materialize()).get(), 0.0001);
    }

    if (((OrcLazyObject) row.getFieldValue(10)).nextIsNull()) {
      assertNull(expected.bytes1);
    } else {
      assertEquals(expected.bytes1, ((OrcLazyBinary) row.getFieldValue(10)).materialize());
    }
    if (((OrcLazyObject) row.getFieldValue(11)).nextIsNull()) {
      assertNull(expected.string1);
    } else {
      assertEquals(expected.string1, ((OrcLazyString) row.getFieldValue(11)).materialize());
    }
    if (((OrcLazyString) row.getFieldValue(12)).nextIsNull()) {
      assertNull(expected.string2);
    } else {
      assertEquals(expected.string2, ((OrcLazyString) row.getFieldValue(12)).materialize());
    }
    if (((OrcLazyObject) row.getFieldValue(13)).nextIsNull()) {
      assertNull(expected.middle);
    } else {
      List<InnerStruct> expectedList = expected.middle.list;
      OrcStruct actualMiddle = (OrcStruct) ((OrcLazyStruct) row.getFieldValue(13)).materialize();
      List<OrcStruct> actualList =
          (List) actualMiddle.getFieldValue(0);
      compareListOfStructs(expectedList, actualList);
      List<String> actualFieldNames = actualMiddle.getFieldNames();
      List<String> expectedFieldNames = new ArrayList<String>();
      expectedFieldNames.add("list");
      compareLists(expectedFieldNames, actualFieldNames);
    }
    if (((OrcLazyObject) row.getFieldValue(14)).nextIsNull()) {
      assertNull(expected.list);
    } else {
      compareListOfStructs(expected.list, (List) ((OrcLazyList) row.getFieldValue(14)).materialize());
    }
    if (((OrcLazyObject) row.getFieldValue(15)).nextIsNull()) {
      assertNull(expected.map);
    } else {
      compareMap(expected.map, (Map) ((OrcLazyMap) row.getFieldValue(15)).materialize());
    }
  }

  private void compareRowsWithoutNextIsNull(OrcStruct row, RandomRowInputs inputs, int rowNumber,
      NumberOfNulls numNulls) throws Exception {

    ReallyBigRow expected = null;
    switch (numNulls) {
      case MANY:
      case SOME:
        expected = createRandomRowWithNulls(inputs.intValues, inputs.doubleValues,
        inputs.stringValues, inputs.byteValues, inputs.words, rowNumber, numNulls);
        break;
      case NONE:
        expected = createRandomRow(inputs.intValues, inputs.doubleValues,
            inputs.stringValues, inputs.byteValues, inputs.words, rowNumber);
        break;
    }

    OrcLazyBoolean lazyboolean1 = (OrcLazyBoolean) row.getFieldValue(0);
    BooleanWritable boolean1 = (BooleanWritable) lazyboolean1.materialize();
    if (boolean1 == null) {
      assertNull(expected.boolean1);
    } else {
      assertEquals(expected.boolean1.booleanValue(), boolean1.get());
    }
    try {
      boolean b1 = lazyboolean1.materializeBoolean();
      assertEquals(expected.boolean1.booleanValue(), b1);
    }
    catch(IOException e) {
      assertNull(expected.boolean1);
    }

    ByteWritable byte1 = (ByteWritable) ((OrcLazyByte) row.getFieldValue(1)).materialize();
    if (byte1 == null) {
      assertNull(expected.byte1);
    } else {
      assertEquals(expected.byte1.byteValue(), byte1.get());
    }

    OrcLazyShort lazyshort1 = (OrcLazyShort) row.getFieldValue(2);
    ShortWritable short1 = (ShortWritable) lazyshort1.materialize();
    if (short1 == null) {
      assertNull(expected.short1);
    } else {
      assertEquals(expected.short1.shortValue(), short1.get());
    }

    try {
      short s1 = lazyshort1.materializeShort();
      assertEquals(expected.short1.shortValue(), s1);
    }
    catch(IOException e) {
      assertNull(expected.short1);
    }

    OrcLazyInt lazyint1 = (OrcLazyInt) row.getFieldValue(3);
    IntWritable int1 = (IntWritable) lazyint1.materialize();
    if (int1 == null) {
      assertNull(expected.int1);
    } else {
      assertEquals(expected.int1.intValue(), int1.get());
    }
    try {
      int i1 = lazyint1.materializeInt();
      assertEquals(expected.int1.intValue(), i1);
    }
    catch(IOException e) {
      assertNull(expected.int1);
    }

    OrcLazyLong lazylong1 = (OrcLazyLong) row.getFieldValue(4);
    LongWritable long1 = (LongWritable) lazylong1.materialize();
    if (long1 == null) {
      assertNull(expected.long1);
    } else {
      assertEquals(expected.long1.longValue(), long1.get());
    }
    try {
      long l1 = lazylong1.materializeLong();
      assertEquals(expected.long1.longValue(), l1);
    }
    catch(IOException e) {
      assertNull(expected.long1);
    }

    OrcLazyShort lazyshort2 = (OrcLazyShort) row.getFieldValue(5);
    ShortWritable short2 = (ShortWritable) lazyshort2.materialize();
    try {
      short s2 = lazyshort2.materializeShort();
      assertEquals(expected.short2.shortValue(), s2);
    }
    catch(IOException e) {
      assertNull(expected.short2);
    }
    if (short2 == null) {
      assertNull(expected.short2);
    } else {
      assertEquals(expected.short2.shortValue(), short2.get());
    }

    OrcLazyInt lazyint2 = (OrcLazyInt) row.getFieldValue(6);
    IntWritable int2 = (IntWritable) lazyint2.materialize();
    try {
      int i2 = lazyint2.materializeInt();
      assertEquals(expected.int2.intValue(), i2);
    }
    catch(IOException e) {
      assertNull(expected.int2);
    }
    if (int2 == null) {
      assertNull(expected.int2);
    } else {
      assertEquals(expected.int2.intValue(), int2.get());
    }

    OrcLazyLong lazylong2 = (OrcLazyLong) row.getFieldValue(7);
    LongWritable long2 = (LongWritable) lazylong2.materialize();
    try {
      long l2 = lazylong2.materializeLong();
      assertEquals(expected.long2.longValue(), l2);
    }
    catch(IOException e) {
      assertNull(expected.long2);
    }
    if (long2 == null) {
      assertNull(expected.long2);
    } else {
      assertEquals(expected.long2.longValue(), long2.get());
    }

    OrcLazyFloat lazyfloat1 = (OrcLazyFloat) row.getFieldValue(8);
    FloatWritable float1 = (FloatWritable) lazyfloat1.materialize();
    if (float1 == null) {
      assertNull(expected.float1);
    } else {
      assertEquals(expected.float1.floatValue(), float1.get(), 0.0001);
    }
    try {
      float f1 = lazyfloat1.materializeFloat();
      assertEquals(expected.float1.floatValue(), f1, 0.0001);
    }
    catch(IOException e) {
      assertNull(expected.float1);
    }

    OrcLazyDouble lazydouble1 = (OrcLazyDouble) row.getFieldValue(9);
    DoubleWritable double1 = (DoubleWritable) lazydouble1.materialize();
    try {
      double d1 = lazydouble1.materializeDouble();
      assertEquals(expected.double1.doubleValue(), d1, 0.0001);
    }
    catch(IOException e) {
      assertNull(expected.double1);
    }
    if (double1 == null) {
      assertNull(expected.double1);
    } else {
      assertEquals(expected.double1.doubleValue(), double1.get(), 0.0001);
    }

    BytesWritable bytes1 = (BytesWritable) ((OrcLazyBinary) row.getFieldValue(10)).materialize();
    if (bytes1 == null) {
      assertNull(expected.bytes1);
    } else {
      assertEquals(expected.bytes1, bytes1);
    }

    Text string1 = (Text) ((OrcLazyString) row.getFieldValue(11)).materialize();
    if (string1 == null) {
      assertNull(expected.string1);
    } else {
      assertEquals(expected.string1, string1);
    }

    Text string2 = (Text) ((OrcLazyString) row.getFieldValue(12)).materialize();
    if (string2 == null) {
      assertNull(expected.string2);
    } else {
      assertEquals(expected.string2, string2);
    }

    OrcStruct middle = (OrcStruct) ((OrcLazyStruct) row.getFieldValue(13)).materialize();
    if (middle == null) {
      assertNull(expected.middle);
    } else {
      List<InnerStruct> expectedList = expected.middle.list;
      List<OrcStruct> actualList = (List) middle.getFieldValue(0);
      compareListOfStructs(expectedList, actualList);
      List<String> actualFieldNames = middle.getFieldNames();
      List<String> expectedFieldNames = new ArrayList<String>();
      expectedFieldNames.add("list");
      compareLists(expectedFieldNames, actualFieldNames);
    }

    List list = (List) ((OrcLazyList) row.getFieldValue(14)).materialize();
    if (list == null) {
      assertNull(expected.list);
    } else {
      compareListOfStructs(expected.list, list);
    }

    Map map = (Map) ((OrcLazyMap) row.getFieldValue(15)).materialize();
    if (map == null) {
      assertNull(expected.map);
    } else {
      compareMap(expected.map, map);
    }
  }

  @Test
  public void testSeek() throws Exception {
    final int COUNT=32768;
    RandomRowInputs inputs = writeRandomRows(COUNT, false);
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    for(int i=COUNT-1; i >= 0; --i) {
      rows.seekToRow(i);
      lazyRow = (OrcLazyStruct) rows.next(lazyRow);
      row = (OrcStruct) lazyRow.materialize();
      compareRows(row, inputs, i, NumberOfNulls.NONE);
    }
    rows.close();
  }

  @Test
  public void testSeekLowMemory() throws Exception {
    final int COUNT=32768;
    RandomRowInputs inputs = writeRandomRows(COUNT, true);
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    for(int i=COUNT-1; i >= 0; --i) {
      rows.seekToRow(i);
      lazyRow = (OrcLazyStruct) rows.next(lazyRow);
      row = (OrcStruct) lazyRow.materialize();
      compareRows(row, inputs, i, NumberOfNulls.NONE);
    }
    rows.close();
  }

  private void readEveryNthRow(int n, boolean withoutNextIsNull, NumberOfNulls numNulls) throws Exception {
    final int COUNT=32768;
    RandomRowInputs inputs = null;
    switch (numNulls) {
      case NONE:
        inputs = writeRandomRows(COUNT, false);
        break;
      case SOME:
      case MANY:
        inputs = writeRandomRowsWithNulls(COUNT, numNulls, false);
        break;
    }

    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    for(int i = 0; i < COUNT; i++) {
      rows.seekToRow(i);
      lazyRow = (OrcLazyStruct) rows.next(lazyRow);
      if (i % n == 0) {
        row = (OrcStruct) lazyRow.materialize();
        if (withoutNextIsNull) {
          compareRowsWithoutNextIsNull(row, inputs, i, numNulls);
        } else {
          compareRows(row, inputs, i, numNulls);
        }
      }
    }
    rows.close();
  }

  @Test
  public void testEveryRow() throws Exception {
    readEveryNthRow(1, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryOtherRow() throws Exception {
    readEveryNthRow(2, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryThirdRow() throws Exception {
    readEveryNthRow(3, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryFourthRow() throws Exception {
    readEveryNthRow(4, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryFifthRow() throws Exception {
    readEveryNthRow(5, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEverySixthRow() throws Exception {
    readEveryNthRow(6, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEverySeventhRow() throws Exception {
    readEveryNthRow(7, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryEighthRow() throws Exception {
    readEveryNthRow(8, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryNinthRow() throws Exception {
    readEveryNthRow(9, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryTenthRow() throws Exception {
    readEveryNthRow(10, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryHundredthRow() throws Exception {
    readEveryNthRow(100, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryThousandthRow() throws Exception {
    readEveryNthRow(1000, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryTenThousandthRow() throws Exception {
    readEveryNthRow(10000, false, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(1, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryOtherRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(2, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryThirdRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(3, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryFourthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(4, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryFifthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(5, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEverySixthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(6, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEverySeventhRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(7, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryEighthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(8, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryNinthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(9, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryTenthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(10, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryHundredthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(100, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryThousandthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(1000, true, NumberOfNulls.NONE);
  }

  @Test
  public void testEveryTenThousandthRowWithoutNextIsNull() throws Exception {
    readEveryNthRow(10000, true, NumberOfNulls.NONE);
  }

  private RandomRowInputs writeRandomRowsWithNulls(int count, NumberOfNulls numNulls,
      boolean lowMemoryMode) throws IOException {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (ReallyBigRow.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        lowMemoryMode ? 200000 : 4000000, CompressionKind.ZLIB, 65536, 1000,
         new MemoryManager(conf));
    Random rand = new Random(42);
    RandomRowInputs inputs = new RandomRowInputs(count);
    long[] intValues = inputs.intValues;
    double[] doubleValues = inputs.doubleValues;
    String[] stringValues = inputs.stringValues;
    BytesWritable[] byteValues = inputs.byteValues;
    String[] words = inputs.words;
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < count/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = rand.nextLong();
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = words[rand.nextInt(words.length)];
    }
    for(int i=0; i < count; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < count; ++i) {
      ReallyBigRow bigrow = createRandomRowWithNulls(intValues, doubleValues, stringValues,
          byteValues, words, i, numNulls);
      writer.addRow(bigrow);
    }
    writer.close();
    writer = null;
    return inputs;
  }

  @Test
  public void testEveryRowWithNulls() throws Exception {
    readEveryNthRow(1, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryOtherRowWithNulls() throws Exception {
    readEveryNthRow(2, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryThirdRowWithNulls() throws Exception {
    readEveryNthRow(3, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryFourthRowWithNulls() throws Exception {
    readEveryNthRow(4, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryFifthRowWithNulls() throws Exception {
    readEveryNthRow(5, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEverySixthRowWithNulls() throws Exception {
    readEveryNthRow(6, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEverySeventhRowWithNulls() throws Exception {
    readEveryNthRow(7, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryEighthRowWithNulls() throws Exception {
    readEveryNthRow(8, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryNinthRowWithNulls() throws Exception {
    readEveryNthRow(9, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryTenthRowWithNulls() throws Exception {
    readEveryNthRow(10, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryHundredthRowWithNulls() throws Exception {
    readEveryNthRow(100, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryThousandthRowWithNulls() throws Exception {
    readEveryNthRow(1000, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryTenThousandthRowWithNulls() throws Exception {
    readEveryNthRow(10000, false, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(1, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryOtherRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(2, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryThirdRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(3, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryFourthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(4, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryFifthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(5, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEverySixthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(6, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEverySeventhRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(7, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryEighthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(8, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryNinthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(9, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryTenthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(10, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryHundredthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(100, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryThousandthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(1000, true, NumberOfNulls.SOME);
  }

  @Test
  public void testEveryTenThousandthRowWithNullsWithoutNextIsNull() throws Exception {
    readEveryNthRow(10000, true, NumberOfNulls.SOME);
  }

  private void skipEveryNthRow(int n, boolean withoutNextIsNull, NumberOfNulls numNulls) throws Exception {
    final int COUNT=32768;
    RandomRowInputs inputs = null;
    switch (numNulls) {
      case NONE:
        inputs = writeRandomRows(COUNT, false);
        break;
      case SOME:
      case MANY:
        inputs = writeRandomRowsWithNulls(COUNT, numNulls, false);
        break;
    }

    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
    OrcLazyRow lazyRow = null;
    OrcStruct row = null;
    for(int i=0; i < COUNT; i++) {
      lazyRow = (OrcLazyRow) rows.next(lazyRow);
      if (i % n != 0) {
        row = (OrcStruct) lazyRow.materialize();
        if (withoutNextIsNull) {
          compareRowsWithoutNextIsNull(row, inputs, i, numNulls);
        } else {
          compareRows(row, inputs, i, numNulls);
        }
      }
    }
    rows.close();
  }

  @Test
  public void testEveryRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(1, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryOtherRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(2, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryThirdRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(3, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryFourthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(4, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryFifthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(5, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEverySixthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(6, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEverySeventhRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(7, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryEighthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(8, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryNinthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(9, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryTenthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(10, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryHundredthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(100, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryThousandthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(1000, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryTenThousandthRowWithLotsOfNulls() throws Exception {
    skipEveryNthRow(10000, false, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(1, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryOtherRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(2, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryThirdRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(3, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryFourthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(4, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryFifthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(5, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEverySixthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(6, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEverySeventhRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(7, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryEighthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(8, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryNinthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(9, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryTenthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(10, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryHundredthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(100, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryThousandthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(1000, true, NumberOfNulls.MANY);
  }

  @Test
  public void testEveryTenThousandthRowWithLotsOfNullsWithoutNextIsNull() throws Exception {
    skipEveryNthRow(10000, true, NumberOfNulls.MANY);
  }

  private void compareInner(InnerStruct expect,
                            OrcStruct actual) throws Exception {
    if (expect == null || actual == null) {
      assertEquals(expect, actual);
    } else {
      if (actual.getFieldValue(0) == null) {
        assertNull(expect.int1);
      } else {
        assertEquals(expect.int1.intValue(), ((IntWritable) actual.getFieldValue(0)).get());
      }
      assertEquals(expect.string1, actual.getFieldValue(1));
    }
  }

  private void compareListOfStructs(List<InnerStruct> expect,
                           List<OrcStruct> actual) throws Exception {
    assertEquals(expect.size(), actual.size());
    for(int j=0; j < expect.size(); ++j) {
      compareInner(expect.get(j), actual.get(j));
    }
  }

  private void compareLists(List expect, List actual) throws Exception {
    assertEquals(expect.size(), actual.size());
    assertTrue(expect.containsAll(actual));
  }

  private void compareMap(Map<Text, InnerStruct> expect, Map<Text, OrcStruct> actual)
      throws Exception {
    assertEquals(expect.size(), actual.size());
    for (Text key : expect.keySet()) {
      compareInner(expect.get(key), actual.get(key));
    }
  }

  private ReallyBigRow createRandomRow(long[] intValues, double[] doubleValues,
                                 String[] stringValues,
                                 BytesWritable[] byteValues,
                                 String[] words, int i) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    return new ReallyBigRow((intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (short) (intValues[i] % 10), (int) (intValues[i] % 10), (long) (intValues[i] % 10),
        (float) doubleValues[i], doubleValues[i], byteValues[i],stringValues[i], Integer.toHexString(i),
        new MiddleStruct(inner, inner2), list(), map(inner,inner2));
  }

  private ReallyBigRow createRandomRowWithNulls(long[] intValues, double[] doubleValues,
      String[] stringValues, BytesWritable[] byteValues, String[] words, int i, NumberOfNulls numNulls) {
    boolean lotsOfNulls = numNulls == NumberOfNulls.MANY;
    Boolean booleanVal = intValues[i] % 10 == 0 ^ lotsOfNulls ? null : (intValues[i] & 1) == 0;
    Byte byteVal = intValues[i] % 11 == 0 ^ lotsOfNulls ? null : (byte) intValues[i];
    Short shortVal = intValues[i] % 12 == 0 ^ lotsOfNulls  ? null : (short) intValues[i];
    Integer intVal = intValues[i] % 13 == 0 ^ lotsOfNulls  ? null : (int) intValues[i];
    Long longVal = intValues[i] % 14 == 0 ^ lotsOfNulls  ? null : intValues[i];
    Float floatVal = intValues[i] % 15 == 0 ^ lotsOfNulls  ? null : (float) doubleValues[i];
    Double doubleVal = intValues[i] % 16 == 0 ^ lotsOfNulls  ? null : doubleValues[i];
    BytesWritable bytesVal = intValues[i] % 17 == 0 ^ lotsOfNulls  ? null : byteValues[i];
    String strVal = intValues[i] % 18 == 0 ^ lotsOfNulls  ? null : stringValues[i];
    InnerStruct inner = intValues[i] % 19 == 0 ^ lotsOfNulls  ? null : new InnerStruct(
        intValues[i] % 10 == 0 ^ lotsOfNulls  ? null : (int) intValues[i],
        intValues[i] % 11 == 0 ^ lotsOfNulls  ? null : stringValues[i]);
    InnerStruct inner2 = intValues[i] % 12 == 0 ^ lotsOfNulls  ? null : new InnerStruct(
        intValues[i] % 13 == 0 ^ lotsOfNulls  ? null : (int) (intValues[i] >> 32),
        intValues[i] % 14 == 0 ^ lotsOfNulls  ? null : words[i % words.length] + "-x");
    MiddleStruct middle = intValues[i] % 15 == 0 ^ lotsOfNulls  ? null : new MiddleStruct(inner, inner2);
    List<InnerStruct> list = intValues[i] % 16 == 0 ^ lotsOfNulls  ? null : list(inner, inner2);
    Map<Text, InnerStruct> map = intValues[i] % 17 == 0 ^ lotsOfNulls  ? null : map(inner, inner2);
    String strVal2 = intValues[i] % 18 == 0 ^ lotsOfNulls  ? null : Integer.toHexString(i);
    Short shortVal2 = intValues[i] % 19 == 0 ^ lotsOfNulls  ? null : (short) (intValues[i] % 10);
    Integer intVal2 = intValues[i] % 10 == 0 ^ lotsOfNulls  ? null : (int) (intValues[i] % 10);
    Long longVal2 = intValues[i] % 11 == 0 ^ lotsOfNulls  ? null : (long) (intValues[i] % 10);
    return new ReallyBigRow(booleanVal, byteVal, shortVal, intVal, longVal, shortVal2, intVal2,
        longVal2, floatVal, doubleVal, bytesVal, strVal, strVal2, middle, list, map);
  }

  private static class MyMemoryManager extends MemoryManager {
    final long totalSpace;
    double rate;
    Path path = null;
    long lastAllocation = 0;
    int rows = 0;
    MemoryManager.Callback callback;

    MyMemoryManager(Configuration conf, long totalSpace, double rate) {
      super(conf);
      this.totalSpace = totalSpace;
      this.rate = rate;
    }

    @Override
    void addWriter(Path path, long requestedAllocation,
                   MemoryManager.Callback callback, long initialAllocation) {
      this.path = path;
      this.lastAllocation = requestedAllocation;
      this.callback = callback;
    }

    @Override
    synchronized void removeWriter(Path path) {
      this.path = null;
      this.lastAllocation = 0;
    }

    @Override
    long getTotalMemoryPool() {
      return totalSpace;
    }

    @Override
    double getAllocationScale() {
      return rate;
    }

    @Override
    void addedRow() throws IOException {
      if (++rows % 100 == 0) {
        callback.checkMemory(rate);
      }
    }
  }

  @Test
  public void testMemoryManagement() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (InnerStruct.class,
              ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    MyMemoryManager memory = new MyMemoryManager(conf, 10000, 0.1);
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        50000, CompressionKind.NONE, 100, 0, memory);
    assertEquals(testFilePath, memory.path);
    for(int i=0; i < 2500; ++i) {
      writer.addRow(new InnerStruct(i*300, Integer.toHexString(10*i)));
    }
    writer.close();
    assertEquals(null, memory.path);
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue("stripe " + i + " is too long at " + stripe.getDataLength(),
          stripe.getDataLength() < 6000);
    }
    assertEquals(5, i);
    assertEquals(2500, reader.getNumberOfRows());
  }
}
