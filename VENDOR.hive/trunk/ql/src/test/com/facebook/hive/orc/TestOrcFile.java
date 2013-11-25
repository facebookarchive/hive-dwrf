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
import com.facebook.hive.orc.OrcTestUtils.BigRowTwoStrings;
import com.facebook.hive.orc.OrcTestUtils.InnerStruct;
import com.facebook.hive.orc.OrcTestUtils.MiddleStruct;
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
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath);

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
    Reader reader = OrcFile.createReader(fs, testFilePath);

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
    Reader reader = OrcFile.createReader(fs, testFilePath);
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
    Reader reader = OrcFile.createReader(fs, testFilePath);
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
    OrcStruct row = new OrcStruct(2);
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
    Reader reader = OrcFile.createReader(fs, testFilePath);
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
    Reader reader = OrcFile.createReader(fs, testFilePath);
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
    Reader reader = OrcFile.createReader(fs, testFilePath);
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

  @Test
  public void testSeek() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRowTwoStrings.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        200000, CompressionKind.ZLIB, 65536, 1000);
    Random rand = new Random(42);
    final int COUNT=32768;
    long[] intValues= new long[COUNT];
    double[] doubleValues = new double[COUNT];
    String[] stringValues = new String[COUNT];
    BytesWritable[] byteValues = new BytesWritable[COUNT];
    String[] words = new String[128];
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < COUNT/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = intValues[2*i];
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = stringValues[2*i];
    }
    for(int i=0; i < COUNT; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < COUNT; ++i) {
      BigRowTwoStrings bigrow = createRandomRow(intValues, doubleValues, stringValues,
          byteValues, words, i);
      writer.addRow(bigrow);
    }
    writer.close();
    writer = null;
    ReaderWriterProfiler.setProfilerOptions(conf);
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    for(int i=COUNT-1; i >= 0; --i) {
      rows.seekToRow(i);
      lazyRow = (OrcLazyStruct) rows.next(lazyRow);
      row = (OrcStruct) lazyRow.materialize();
      BigRowTwoStrings expected = createRandomRow(intValues, doubleValues,
          stringValues, byteValues, words, i);
      assertEquals(expected.boolean1.booleanValue(),
          ((BooleanWritable) ((OrcLazyBoolean) row.getFieldValue(0)).materialize()).get());
      assertEquals(expected.byte1.byteValue(),
          ((ByteWritable) ((OrcLazyByte) row.getFieldValue(1)).materialize()).get());
      assertEquals(expected.short1.shortValue(),
          ((ShortWritable) ((OrcLazyShort) row.getFieldValue(2)).materialize()).get());
      assertEquals(expected.int1.intValue(),
          ((IntWritable) ((OrcLazyInt) row.getFieldValue(3)).materialize()).get());
      assertEquals(expected.long1.longValue(),
          ((LongWritable) ((OrcLazyLong) row.getFieldValue(4)).materialize()).get());
      assertEquals(expected.float1.floatValue(),
          ((FloatWritable) ((OrcLazyFloat) row.getFieldValue(5)).materialize()).get(), 0.0001);
      assertEquals(expected.double1.doubleValue(),
          ((DoubleWritable) ((OrcLazyDouble) row.getFieldValue(6)).materialize()).get(), 0.0001);
      assertEquals(expected.bytes1, ((OrcLazyBinary) row.getFieldValue(7)).materialize());
      assertEquals(expected.string1, ((OrcLazyString) row.getFieldValue(8)).materialize());
      assertEquals(expected.string2, ((OrcLazyString) row.getFieldValue(9)).materialize());
      List<InnerStruct> expectedList = expected.middle.list;
      List<OrcStruct> actualList =
          (List) ((OrcStruct) ((OrcLazyStruct) row.getFieldValue(10)).materialize()).getFieldValue(0);
      compareList(expectedList, actualList);
      compareList(expected.list, (List) ((OrcLazyList) row.getFieldValue(11)).materialize());
      compareMap(expected.map, (Map) ((OrcLazyMap) row.getFieldValue(12)).materialize());
    }
    rows.close();
  }

  @Test
  public void testNulls() throws Exception {
    ObjectInspector inspector;
    synchronized (TestOrcFile.class) {
      inspector = ObjectInspectorFactory.getReflectionObjectInspector
          (BigRowTwoStrings.class, ObjectInspectorFactory.ObjectInspectorOptions.JAVA);
    }
    Writer writer = OrcFile.createWriter(fs, testFilePath, conf, inspector,
        200000, CompressionKind.ZLIB, 65536, 1000);
    Random rand = new Random(42);
    final int COUNT=32768;
    long[] intValues= new long[COUNT];
    double[] doubleValues = new double[COUNT];
    String[] stringValues = new String[COUNT];
    BytesWritable[] byteValues = new BytesWritable[COUNT];
    String[] words = new String[128];
    for(int i=0; i < words.length; ++i) {
      words[i] = Integer.toHexString(rand.nextInt());
    }
    for(int i=0; i < COUNT/2; ++i) {
      intValues[2*i] = rand.nextLong();
      intValues[2*i+1] = intValues[2*i];
      stringValues[2*i] = words[rand.nextInt(words.length)];
      stringValues[2*i+1] = stringValues[2*i];
    }
    for(int i=0; i < COUNT; ++i) {
      doubleValues[i] = rand.nextDouble();
      byte[] buf = new byte[20];
      rand.nextBytes(buf);
      byteValues[i] = new BytesWritable(buf);
    }
    for(int i=0; i < COUNT; ++i) {
      BigRowTwoStrings bigrow = createRandomRowWithNulls(intValues, doubleValues, stringValues,
          byteValues, words, i);
      writer.addRow(bigrow);
    }
    writer.close();
    writer = null;
    Reader reader = OrcFile.createReader(fs, testFilePath);
    assertEquals(COUNT, reader.getNumberOfRows());
    RecordReader rows = reader.rows(null);
    OrcLazyRow lazyRow = new OrcLazyRow(null);
    OrcStruct row = null;
    for(int i=0; i < COUNT; i++) {
      rows.next(lazyRow);
      row = (OrcStruct) lazyRow.materialize();
      BigRowTwoStrings expected = createRandomRowWithNulls(intValues, doubleValues,
          stringValues, byteValues, words, i);
      if (((OrcLazyObject) row.getFieldValue(0)).nextIsNull()) {
        assertNull(expected.boolean1);
      } else {
        assertEquals(expected.boolean1.booleanValue(),
            ((BooleanWritable) ((OrcLazyBoolean) row.getFieldValue(0)).materialize()).get());
      }
      if (((OrcLazyObject) row.getFieldValue(1)).nextIsNull()) {
        assertNull(expected.byte1);
      } else {
        assertEquals(expected.byte1.byteValue(),
            ((ByteWritable) ((OrcLazyByte) row.getFieldValue(1)).materialize()).get());
      }
      if (((OrcLazyObject) row.getFieldValue(2)).nextIsNull()) {
        assertNull(expected.short1);
      } else {
        assertEquals(expected.short1.shortValue(),
            ((ShortWritable) ((OrcLazyShort) row.getFieldValue(2)).materialize()).get());
      }
      if (((OrcLazyObject) row.getFieldValue(3)).nextIsNull()) {
        assertNull(expected.int1);
      } else {
        assertEquals(expected.int1.intValue(),
            ((IntWritable) ((OrcLazyInt) row.getFieldValue(3)).materialize()).get());
      }
      if (((OrcLazyObject) row.getFieldValue(4)).nextIsNull()) {
        assertNull(expected.long1);
      } else {
        assertEquals(expected.long1.longValue(),
            ((LongWritable) ((OrcLazyLong) row.getFieldValue(4)).materialize()).get());
      }
      if (((OrcLazyObject) row.getFieldValue(5)).nextIsNull()) {
        assertNull(expected.float1);
      } else {
        assertEquals(expected.float1.floatValue(),
            ((FloatWritable) ((OrcLazyFloat) row.getFieldValue(5)).materialize()).get(), 0.0001);
      }
      if (((OrcLazyObject) row.getFieldValue(6)).nextIsNull()) {
        assertNull(expected.double1);
      } else {
        assertEquals(expected.double1.doubleValue(),
            ((DoubleWritable) ((OrcLazyDouble) row.getFieldValue(6)).materialize()).get(), 0.0001);
      }
      if (((OrcLazyObject) row.getFieldValue(7)).nextIsNull()) {
        assertNull(expected.bytes1);
      } else {
        assertEquals(expected.bytes1, ((OrcLazyBinary) row.getFieldValue(7)).materialize());
      }
      if (((OrcLazyObject) row.getFieldValue(8)).nextIsNull()) {
        assertNull(expected.string1);
      } else {
        assertEquals(expected.string1, ((OrcLazyString) row.getFieldValue(8)).materialize());
      }
      if (((OrcLazyString) row.getFieldValue(9)).nextIsNull()) {
        assertNull(expected.string2);
      } else {
        assertEquals(expected.string2, ((OrcLazyString) row.getFieldValue(9)).materialize());
      }
      if (((OrcLazyObject) row.getFieldValue(10)).nextIsNull()) {
        assertNull(expected.middle);
      } else {
        List<InnerStruct> expectedList = expected.middle.list;
        List<OrcStruct> actualList =
            (List) ((OrcStruct) ((OrcLazyStruct) row.getFieldValue(10)).materialize()).getFieldValue(0);
        compareList(expectedList, actualList);
      }
      if (((OrcLazyObject) row.getFieldValue(11)).nextIsNull()) {
        assertNull(expected.list);
      } else {
        compareList(expected.list, (List) ((OrcLazyList) row.getFieldValue(11)).materialize());
      }
      if (((OrcLazyObject) row.getFieldValue(12)).nextIsNull()) {
        assertNull(expected.map);
      } else {
        compareMap(expected.map, (Map) ((OrcLazyMap) row.getFieldValue(12)).materialize());
      }
    }
    rows.close();
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

  private void compareList(List<InnerStruct> expect,
                           List<OrcStruct> actual) throws Exception {
    assertEquals(expect.size(), actual.size());
    for(int j=0; j < expect.size(); ++j) {
      compareInner(expect.get(j), actual.get(j));
    }
  }

  private void compareMap(Map<Text, InnerStruct> expect, Map<Text, OrcStruct> actual)
      throws Exception {
    assertEquals(expect.size(), actual.size());
    for (Text key : expect.keySet()) {
      compareInner(expect.get(key), actual.get(key));
    }
  }

  private BigRowTwoStrings createRandomRow(long[] intValues, double[] doubleValues,
                                 String[] stringValues,
                                 BytesWritable[] byteValues,
                                 String[] words, int i) {
    InnerStruct inner = new InnerStruct((int) intValues[i], stringValues[i]);
    InnerStruct inner2 = new InnerStruct((int) (intValues[i] >> 32),
        words[i % words.length] + "-x");
    return new BigRowTwoStrings((intValues[i] & 1) == 0, (byte) intValues[i],
        (short) intValues[i], (int) intValues[i], intValues[i],
        (float) doubleValues[i], doubleValues[i], byteValues[i],stringValues[i], Integer.toHexString(i),
        new MiddleStruct(inner, inner2), list(), map(inner,inner2));
  }

  private BigRowTwoStrings createRandomRowWithNulls(long[] intValues, double[] doubleValues,
      String[] stringValues, BytesWritable[] byteValues, String[] words, int i) {
    Boolean booleanVal = intValues[i] % 10 == 0 ? null : (intValues[i] & 1) == 0;
    Byte byteVal = intValues[i] % 11 == 0 ? null : (byte) intValues[i];
    Short shortVal = intValues[i] % 12 == 0 ? null : (short) intValues[i];
    Integer intVal = intValues[i] % 13 == 0 ? null : (int) intValues[i];
    Long longVal = intValues[i] % 14 == 0 ? null : intValues[i];
    Float floatVal = intValues[i] % 15 == 0 ? null : (float) doubleValues[i];
    Double doubleVal = intValues[i] % 16 == 0 ? null : doubleValues[i];
    BytesWritable bytesVal = intValues[i] % 17 == 0 ? null : byteValues[i];
    String strVal = intValues[i] % 18 == 0 ? null : stringValues[i];
    InnerStruct inner = intValues[i] % 19 == 0 ? null : new InnerStruct(
        intValues[i] % 10 == 0 ? null : (int) intValues[i],
        intValues[i] % 11 == 0 ? null : stringValues[i]);
    InnerStruct inner2 = intValues[i] % 12 == 0 ? null : new InnerStruct(
        intValues[i] % 13 == 0 ? null : (int) (intValues[i] >> 32),
        intValues[i] % 14 == 0 ? null : words[i % words.length] + "-x");
    MiddleStruct middle = intValues[i] % 15 == 0 ? null : new MiddleStruct(inner, inner2);
    List<InnerStruct> list = intValues[i] % 16 == 0 ? null : list(inner, inner2);
    Map<Text, InnerStruct> map = intValues[i] % 17 == 0 ? null : map(inner, inner2);
    String strVal2 = intValues[i] % 18 == 0 ? null : Integer.toHexString(i);
    return new BigRowTwoStrings(booleanVal, byteVal, shortVal, intVal, longVal, floatVal, doubleVal,
        bytesVal, strVal, strVal2, middle, list, map);
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
                   MemoryManager.Callback callback) {
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
    Reader reader = OrcFile.createReader(fs, testFilePath);
    int i = 0;
    for(StripeInformation stripe: reader.getStripes()) {
      i += 1;
      assertTrue("stripe " + i + " is too long at " + stripe.getDataLength(),
          stripe.getDataLength() < 5000);
    }
    assertEquals(25, i);
    assertEquals(2500, reader.getNumberOfRows());
  }
}
