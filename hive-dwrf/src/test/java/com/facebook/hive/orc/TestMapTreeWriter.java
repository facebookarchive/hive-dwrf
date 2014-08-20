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

import java.io.File;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.StringObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.Text;
import org.junit.Before;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.TestName;

import com.facebook.hive.orc.lazy.OrcLazyMap;
import com.facebook.hive.orc.lazy.OrcLazyStruct;

/**
 * Tests for the writer for map columns in ORC files.
 */
public class TestMapTreeWriter {

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

  /**
   * MapObjectInspectorIgnoreNullKeys.
   *
   * An implementation of a map object inspector that counts the null key towards its size, but
   * removes it before returning getMap (this is similar to what LazyMap does).
   */
  private static class MapObjectInspectorIgnoreNullKeys implements MapObjectInspector {

    ObjectInspector mapKeyObjectInspector;
    ObjectInspector mapValueObjectInspector;

    public MapObjectInspectorIgnoreNullKeys (ObjectInspector mapKeyObjectInspector,
        ObjectInspector mapValueObjectInspector) {
      this.mapKeyObjectInspector = mapKeyObjectInspector;
      this.mapValueObjectInspector = mapValueObjectInspector;
    }


    @Override
    public String getTypeName() {
      return org.apache.hadoop.hive.serde.Constants.MAP_TYPE_NAME + "<"
          + mapKeyObjectInspector.getTypeName() + ","
          + mapValueObjectInspector.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.MAP;
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
      return mapKeyObjectInspector;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
      return mapValueObjectInspector;
    }

    @Override
    public Object getMapValueElement(Object data, Object key) {
      if (data == null || key == null) {
        return null;
      }

      return ((Map<?, ?>) data).get(key);
    }

    @Override
    public Map<?, ?> getMap(Object data) {
      if (data == null) {
        return null;
      }

      Map<?, ?> map = (Map<?, ?>) data;
      if (map.containsKey(null)) {
        map.remove(null);
      }
      return map;
    }

    @Override
    public int getMapSize(Object data) {
      if (data == null) {
        return -1;
      }

      return ((Map<?, ?>) data).size();
    }

  }

  /**
   * MapStruct.
   *
   * A simple object that only contains a map.
   */
  private static class MapStruct {
    protected final Map<?, ?> map;

    public MapStruct(Map<?, ?> map) {
      this.map = map;
    }

    public Map<?, ?> getMap() {
      return map;
    }
  }

  /**
   * VisibleJavaStringObjectInspector.
   *
   * Like JavaStringObjectInspector, but things aren't package private.
   */
  private static class VisibleJavaStringObjectInspector implements StringObjectInspector {

    @Override
    public PrimitiveCategory getPrimitiveCategory() {
      return PrimitiveCategory.STRING;
    }

    @Override
    public Class<?> getPrimitiveWritableClass() {
      return Text.class;
    }

    @Override
    public Class<?> getJavaPrimitiveClass() {
      return String.class;
    }

    @Override
    public Object copyObject(Object o) {
      return null;
    }

    @Override
    public boolean preferWritable() {
      return false;
    }

    @Override
    public String getTypeName() {
      return "String";
    }

    @Override
    public Category getCategory() {
      return Category.PRIMITIVE;
    }

    @Override
    public Text getPrimitiveWritableObject(Object o) {
      return o == null ? null : new Text(o.toString());
    }

    @Override
    public String getPrimitiveJavaObject(Object o) {
      return o == null ? null : o.toString();
    }

    /**
     * The precision of the underlying data.
     */
    @SuppressWarnings({"override", "UnusedDeclaration"}) // Hive 0.13
    public int precision() {
      return 0;
    }

    /**
     * The scale of the underlying data.
     */
    @SuppressWarnings({"override", "UnusedDeclaration"}) // Hive 0.13
    public int scale() {
      return 0;
    }

    @SuppressWarnings({"override", "UnusedDeclaration", "RedundantCast"}) // FB Hive
    public PrimitiveTypeInfo getTypeInfo() {
      return (PrimitiveTypeInfo) TypeInfoFactory.stringTypeInfo;
    }
  }

  /**
   * MapStructObjectInspector.
   *
   * An object inspector that can be used to inspect a MapStruct.  It always returns a
   * MapObjectInspectorIgnoreNullKeys of VisibleJavaStringObjectInspectors as the object inspector
   * for the one field it has.
   */
  private static class MapStructObjectInspector extends StructObjectInspector {

    public MapStructObjectInspector() {
      this.fields = new ArrayList<MapField>();
      fields.add(new MapField());
    }

    @Override
    public String getTypeName() {
      return ObjectInspectorUtils.getStandardStructTypeName(this);
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }

    protected static class MapField implements StructField {

      public MapField() {
      }

      @Override
      public String getFieldName() {
        return "map";
      }

      @Override
      public ObjectInspector getFieldObjectInspector() {
        return new MapObjectInspectorIgnoreNullKeys(new VisibleJavaStringObjectInspector(),
            new VisibleJavaStringObjectInspector());
      }

      @Override
      public String getFieldComment() {
        return null;
      }
    }

    protected List<MapField> fields;

    @Override
    public List<? extends StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String fieldName) {
      return ObjectInspectorUtils.getStandardStructFieldRef(fieldName, fields);
    }

    @Override
    public Object getStructFieldData(Object data, StructField fieldRef) {
      if (data == null) {
        return null;
      }

      return ((MapStruct) data).getMap();
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object data) {
      return Arrays.asList((Object) ((MapStruct) data).getMap());
    }

  }

  @Test
  /**
   * Tests writing a Map containing only an entry with a null key.
   */
  public void testIntegerEnterLowMemoryModeAndOnNotCarriedOverStripe() throws Exception {
    ObjectInspector inspector;
    synchronized (TestMapTreeWriter.class) {
      inspector = new MapStructObjectInspector();
    }
    ReaderWriterProfiler.setProfilerOptions(conf);
    Writer writer = new WriterImpl(fs, testFilePath, conf, inspector,
        1000000, CompressionKind.NONE, 100, 10000, new MemoryManager(conf));

    Map<String, String> map = new HashMap<String, String>();
    map.put(null, null);
    // Write a map containing only a null key
    writer.addRow(new MapStruct(map));

    writer.close();
    Reader reader = OrcFile.createReader(fs, testFilePath, conf);
    RecordReader rows = reader.rows(null);
    OrcLazyStruct lazyRow = null;
    OrcStruct row = null;
    lazyRow = (OrcLazyStruct) rows.next(lazyRow);
    row = (OrcStruct) lazyRow.materialize();
    // Read it back, since we're using MapStructObjectInspector which wraps
    // MapObjectInspectorIgnoreNullKeys it should return an empty map
    assertEquals(new HashMap<String, String>(), ((OrcLazyMap) row.getFieldValue(0)).materialize());
    rows.close();
  }
}
