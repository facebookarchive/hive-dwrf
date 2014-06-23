package com.facebook.hive.orc;

import java.io.IOException;
import java.sql.Timestamp;

import junit.framework.Assert;

import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.io.ByteWritable;
import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BinaryObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.BooleanObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ByteObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.FloatObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.LongObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.ShortObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.apache.hadoop.io.BooleanWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.FloatWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.junit.Test;

import com.facebook.hive.orc.lazy.LazyBinaryTreeReader;
import com.facebook.hive.orc.lazy.LazyBooleanTreeReader;
import com.facebook.hive.orc.lazy.LazyByteTreeReader;
import com.facebook.hive.orc.lazy.LazyDoubleTreeReader;
import com.facebook.hive.orc.lazy.LazyFloatTreeReader;
import com.facebook.hive.orc.lazy.LazyIntTreeReader;
import com.facebook.hive.orc.lazy.LazyLongTreeReader;
import com.facebook.hive.orc.lazy.LazyShortTreeReader;
import com.facebook.hive.orc.lazy.LazyStringTreeReader;
import com.facebook.hive.orc.lazy.LazyTimestampTreeReader;
import com.facebook.hive.orc.lazy.OrcLazyBinary;
import com.facebook.hive.orc.lazy.OrcLazyBoolean;
import com.facebook.hive.orc.lazy.OrcLazyByte;
import com.facebook.hive.orc.lazy.OrcLazyDouble;
import com.facebook.hive.orc.lazy.OrcLazyFloat;
import com.facebook.hive.orc.lazy.OrcLazyInt;
import com.facebook.hive.orc.lazy.OrcLazyList;
import com.facebook.hive.orc.lazy.OrcLazyListObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyLong;
import com.facebook.hive.orc.lazy.OrcLazyMap;
import com.facebook.hive.orc.lazy.OrcLazyMapObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyObjectInspectorUtils;
import com.facebook.hive.orc.lazy.OrcLazyShort;
import com.facebook.hive.orc.lazy.OrcLazyString;
import com.facebook.hive.orc.lazy.OrcLazyStructObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyTimestamp;
import com.facebook.hive.orc.lazy.OrcLazyUnionObjectInspector;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.Lists;

public class TestObjectInspector {

  private static final ListTypeInfo LIST_TYPE_INFO =
      (ListTypeInfo) TypeInfoFactory.getListTypeInfo(TypeInfoFactory.stringTypeInfo);
  private static final MapTypeInfo MAP_TYPE_INFO = (MapTypeInfo) TypeInfoFactory.getMapTypeInfo(
      TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);
  private static final StructTypeInfo STRUCT_TYPE_INFO =
      (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(Lists.newArrayList("field0"),
          Lists.newArrayList((TypeInfo) TypeInfoFactory.stringTypeInfo));
  private static final UnionTypeInfo UNION_TYPE_INFO =
      (UnionTypeInfo) TypeInfoFactory.getUnionTypeInfo(
          Lists.newArrayList((TypeInfo) TypeInfoFactory.stringTypeInfo));

  private static final ListObjectInspector LIST_OI =
      new OrcLazyListObjectInspector(LIST_TYPE_INFO);
  private static final MapObjectInspector MAP_OI = new OrcLazyMapObjectInspector(MAP_TYPE_INFO);
  private static final StructObjectInspector STRUCT_OI =
      new OrcLazyStructObjectInspector(STRUCT_TYPE_INFO);
  private static final UnionObjectInspector UNION_OI =
      new OrcLazyUnionObjectInspector(UNION_TYPE_INFO);

  @Test
  public void TestNullList() {
    Assert.assertNull(LIST_OI.getList(null));
    Assert.assertEquals(-1, LIST_OI.getListLength(null));
    Assert.assertNull(LIST_OI.getListElement(null, 0));
  }

  @Test
  public void TestNullMap() {
    Assert.assertNull(MAP_OI.getMap(null));
    Assert.assertEquals(-1, MAP_OI.getMapSize(null));
    Assert.assertNull(MAP_OI.getMapValueElement(null, "key"));
  }

  @Test
  public void TestNullStruct() {
    Assert.assertNull(STRUCT_OI.getStructFieldData(null, STRUCT_OI.getStructFieldRef("field0")));
    Assert.assertNull(STRUCT_OI.getStructFieldsDataAsList(null));
  }

  @Test
  public void TestNullUnion() {
    Assert.assertNull(UNION_OI.getField(null));
    Assert.assertEquals(-1, UNION_OI.getTag(null));
  }

  /**
   * Tests accessing indices of an array that are outside the allowed range of
   * [0, size - 1]
   */
  @Test
  public void TestInvalidIndexArray() {
    OrcLazyList list = new OrcLazyList(null) {
      @Override
      public Object materialize() throws IOException {
        return Lists.newArrayList("a");
      }
    };

    // Test an index < 0
    Assert.assertNull(LIST_OI.getListElement(list, -1));
    // Test a valid index (control case)
    Assert.assertEquals("a", LIST_OI.getListElement(list, 0));
    //Test an index >= the size of the list
    Assert.assertNull(LIST_OI.getListElement(list, 1));
  }

  /**
   * Tests trying to get the value for a key in a map that doesn't exist
   */
  @Test
  public void TestNonexistentKeyMap() {
    OrcLazyMap map = new OrcLazyMap(null) {
      @Override
      public Object materialize() throws IOException {
        return ImmutableMap.of("a", "b");
      }
    };

    // Test a key that exists (control case)
    Assert.assertEquals("b", MAP_OI.getMapValueElement(map, "a"));
    //Test a key that doesn't exist
    Assert.assertNull(MAP_OI.getMapValueElement(map, "z"));
  }

  /**
   * Tests that after copying a lazy binary object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyBinary() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyBinary lazyBinary = new OrcLazyBinary(new LazyBinaryTreeReader(0, 0) {
      int nextCalls = 0;

      @Override
      public Object next(Object previous) throws IOException {
        if (nextCalls == 0) {
          nextCalls++;
          return new BytesWritable("a".getBytes());
        }

        throw new IOException("next should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    BinaryObjectInspector binaryOI = (BinaryObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.binaryTypeInfo);

    OrcLazyBinary lazyBinary2 = (OrcLazyBinary) binaryOI.copyObject(lazyBinary);

    Assert.assertEquals("a", new String(((BytesWritable) lazyBinary.materialize()).getBytes()));
    Assert.assertEquals("a", new String(((BytesWritable) lazyBinary2.materialize()).getBytes()));
  }

  /**
   * Tests that after copying a lazy boolean object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyBoolean() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyBoolean lazyBoolean = new OrcLazyBoolean(new LazyBooleanTreeReader(0, 0) {
      int nextCalls = 0;

      @Override
      public Object next(Object previous) throws IOException {
        if (nextCalls == 0) {
          return new BooleanWritable(true);
        }

        throw new IOException("next should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    BooleanObjectInspector booleanOI = (BooleanObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.booleanTypeInfo);

    OrcLazyBoolean lazyBoolean2 = (OrcLazyBoolean) booleanOI.copyObject(lazyBoolean);

    Assert.assertEquals(true, ((BooleanWritable) lazyBoolean.materialize()).get());
    Assert.assertEquals(true, ((BooleanWritable) lazyBoolean2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy byte object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyByte() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyByte lazyByte = new OrcLazyByte(new LazyByteTreeReader(0, 0) {
      int nextCalls = 0;

      @Override
      public Object next(Object previous) throws IOException {
        if (nextCalls == 0) {
          return new ByteWritable((byte) 1);
        }

        throw new IOException("next should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    ByteObjectInspector byteOI = (ByteObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.byteTypeInfo);

    OrcLazyByte lazyByte2 = (OrcLazyByte) byteOI.copyObject(lazyByte);

    Assert.assertEquals(1, ((ByteWritable) lazyByte.materialize()).get());
    Assert.assertEquals(1, ((ByteWritable) lazyByte2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy double object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyDouble() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyDouble lazyDouble = new OrcLazyDouble(new LazyDoubleTreeReader(0, 0) {
      int nextCalls = 0;

      @Override
      public Object next(Object previous) throws IOException {
        if (nextCalls == 0) {
          return new DoubleWritable(1.0);
        }

        throw new IOException("next should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    DoubleObjectInspector doubleOI = (DoubleObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.doubleTypeInfo);

    OrcLazyDouble lazyDouble2 = (OrcLazyDouble) doubleOI.copyObject(lazyDouble);

    Assert.assertEquals(1.0, ((DoubleWritable) lazyDouble.materialize()).get());
    Assert.assertEquals(1.0, ((DoubleWritable) lazyDouble2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy float object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyFloat() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyFloat lazyFloat = new OrcLazyFloat(new LazyFloatTreeReader(0, 0) {
      int nextCalls = 0;

      @Override
      public Object next(Object previous) throws IOException {
        if (nextCalls == 0) {
          return new FloatWritable(1.0f);
        }

        throw new IOException("next should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    FloatObjectInspector floatOI = (FloatObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.floatTypeInfo);

    OrcLazyFloat lazyFloat2 = (OrcLazyFloat) floatOI.copyObject(lazyFloat);

    Assert.assertEquals(1.0f, ((FloatWritable) lazyFloat.materialize()).get());
    Assert.assertEquals(1.0f, ((FloatWritable) lazyFloat2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy int object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyInt() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyInt lazyInt = new OrcLazyInt(new LazyIntTreeReader(0, 0) {
      int getCalls = 0;

      @Override
      public Object get(long currentRow, Object previous) throws IOException {
        if (getCalls == 0) {
          return new IntWritable(1);
        }

        throw new IOException("get should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    IntObjectInspector intOI = (IntObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.intTypeInfo);

    OrcLazyInt lazyInt2 = (OrcLazyInt) intOI.copyObject(lazyInt);

    Assert.assertEquals(1, ((IntWritable) lazyInt.materialize()).get());
    Assert.assertEquals(1, ((IntWritable) lazyInt2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy long object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyLong() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyLong lazyLong = new OrcLazyLong(new LazyLongTreeReader(0, 0) {
      int getCalls = 0;

      @Override
      public Object get(long currentRow, Object previous) throws IOException {
        if (getCalls == 0) {
          return new LongWritable(1);
        }

        throw new IOException("get should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    LongObjectInspector longOI = (LongObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.longTypeInfo);

    OrcLazyLong lazyLong2 = (OrcLazyLong) longOI.copyObject(lazyLong);

    Assert.assertEquals(1, ((LongWritable) lazyLong.materialize()).get());
    Assert.assertEquals(1, ((LongWritable) lazyLong2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy short object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyShort() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyShort lazyShort = new OrcLazyShort(new LazyShortTreeReader(0, 0) {
      int getCalls = 0;

      @Override
      public Object get(long currentRow, Object previous) throws IOException {
        if (getCalls == 0) {
          return new ShortWritable((short) 1);
        }

        throw new IOException("get should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    ShortObjectInspector shortOI = (ShortObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.shortTypeInfo);

    OrcLazyShort lazyShort2 = (OrcLazyShort) shortOI.copyObject(lazyShort);

    Assert.assertEquals(1, ((ShortWritable) lazyShort.materialize()).get());
    Assert.assertEquals(1, ((ShortWritable) lazyShort2.materialize()).get());
  }

  /**
   * Tests that after copying a lazy string object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyString() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyString lazyString = new OrcLazyString(new LazyStringTreeReader(0, 0) {
      int getCalls = 0;

      @Override
      public Object get(long currentRow, Object previous) throws IOException {
        if (getCalls == 0) {
          return new Text("a");
        }

        throw new IOException("get should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    StringObjectInspector stringOI = (StringObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.stringTypeInfo);

    OrcLazyString lazyString2 = (OrcLazyString) stringOI.copyObject(lazyString);

    Assert.assertEquals("a", ((Text) lazyString.materialize()).toString());
    Assert.assertEquals("a", ((Text) lazyString2.materialize()).toString());
  }

  /**
   * Tests that after copying a lazy timestamp object, calling materialize on the original and the
   * copy doesn't advance the tree reader twice
   * @throws Exception
   */
  @Test
  public void TestCopyTimestamp() throws Exception {
    ReaderWriterProfiler.setProfilerOptions(null);
    OrcLazyTimestamp lazyTimestamp = new OrcLazyTimestamp(new LazyTimestampTreeReader(0, 0) {
      int nextCalls = 0;

      @Override
      public Object next(Object previous) throws IOException {
        if (nextCalls == 0) {
          return new TimestampWritable(new Timestamp(1));
        }

        throw new IOException("next should only be called once");
      }

      @Override
      protected boolean seekToRow(long currentRow) throws IOException {
        return true;
      }
    });

    TimestampObjectInspector timestampOI = (TimestampObjectInspector)
        OrcLazyObjectInspectorUtils.createLazyObjectInspector(TypeInfoFactory.timestampTypeInfo);

    OrcLazyTimestamp lazyTimestamp2 = (OrcLazyTimestamp) timestampOI.copyObject(lazyTimestamp);

    Assert.assertEquals(new Timestamp(1), ((TimestampWritable) lazyTimestamp.materialize()).getTimestamp());
    Assert.assertEquals(new Timestamp(1), ((TimestampWritable) lazyTimestamp2.materialize()).getTimestamp());
  }
}
