package com.facebook.hive.orc;

import junit.framework.Assert;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;
import org.junit.Test;

import com.facebook.hive.orc.lazy.OrcLazyListObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyMapObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyStructObjectInspector;
import com.facebook.hive.orc.lazy.OrcLazyUnionObjectInspector;
import com.google.common.collect.Lists;

public class TestObjectInspector {

  @Test
  public void TestNullList() {
    ListTypeInfo listTypeInfo = (ListTypeInfo) TypeInfoFactory.getListTypeInfo(
        TypeInfoFactory.stringTypeInfo);

    ListObjectInspector listOI = new OrcLazyListObjectInspector(listTypeInfo);
    Assert.assertNull(listOI.getList(null));
    Assert.assertEquals(-1, listOI.getListLength(null));
    Assert.assertNull(listOI.getListElement(null, 0));
  }

  @Test
  public void TestNullMap() {
    MapTypeInfo mapTypeInfo = (MapTypeInfo) TypeInfoFactory.getMapTypeInfo(
        TypeInfoFactory.stringTypeInfo, TypeInfoFactory.stringTypeInfo);

    MapObjectInspector mapOI = new OrcLazyMapObjectInspector(mapTypeInfo);
    Assert.assertNull(mapOI.getMap(null));
    Assert.assertEquals(-1, mapOI.getMapSize(null));
    Assert.assertNull(mapOI.getMapValueElement(null, "key"));
  }

  @Test
  public void TestNullStruct() {
    StructTypeInfo structTypeInfo = (StructTypeInfo) TypeInfoFactory.getStructTypeInfo(
        Lists.newArrayList("field0"), Lists.newArrayList(TypeInfoFactory.stringTypeInfo));

    StructObjectInspector structOI = new OrcLazyStructObjectInspector(structTypeInfo);
    Assert.assertNull(structOI.getStructFieldData(null, structOI.getStructFieldRef("field0")));
    Assert.assertNull(structOI.getStructFieldsDataAsList(null));
  }

  @Test
  public void TestNullUnion() {
    UnionTypeInfo unionTypeInfo = (UnionTypeInfo) TypeInfoFactory.getUnionTypeInfo(
        Lists.newArrayList(TypeInfoFactory.stringTypeInfo));

    UnionObjectInspector unionOI = new OrcLazyUnionObjectInspector(unionTypeInfo);
    Assert.assertNull(unionOI.getField(null));
    Assert.assertEquals(-1, unionOI.getTag(null));
  }
}
