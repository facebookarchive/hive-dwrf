package com.facebook.hive.orc;

import java.io.IOException;

import junit.framework.Assert;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.junit.Test;

import com.facebook.hive.orc.lazy.OrcLazyList;
import com.facebook.hive.orc.lazy.OrcLazyListObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyMap;
import com.facebook.hive.orc.lazy.OrcLazyMapObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyStructObjectInspector;
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
}
