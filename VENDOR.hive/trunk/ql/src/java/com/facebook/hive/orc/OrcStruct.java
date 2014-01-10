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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableMapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.SettableStructObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.io.Writable;

import com.facebook.hive.orc.lazy.OrcLazyObjectInspectorUtils;

public final class OrcStruct implements Writable {

  private Object[] fields;
  private List<String> fieldNames;

  public OrcStruct() {
    this.fields = new Object[0];
    this.fieldNames = new ArrayList<String>();
  }

  public OrcStruct(List<String> fieldNames) {
    this.fields = new Object[fieldNames.size()];
    this.fieldNames = fieldNames;
  }

  public Object getFieldValue(int fieldIndex) {
    return fields[fieldIndex];
  }

  public void setFieldValue(int fieldIndex, Object value) {
    fields[fieldIndex] = value;
  }

  public int getNumFields() {
    return fields.length;
  }

  public List<String> getFieldNames() {
    return fieldNames;
  }

  /**
   * Change the names and number of fields in the struct. No effect if the number of
   * fields is the same. The old field values are copied to the new array.
   * @param numFields the new number of fields
   */
  public void setFieldNames(List<String> fieldNames) {
    this.fieldNames = fieldNames;
    if (fields.length != fieldNames.size()) {
      Object[] oldFields = fields;
      fields = new Object[fieldNames.size()];
      System.arraycopy(oldFields, 0, fields, 0,
          Math.min(oldFields.length, fieldNames.size()));
    }
  }

  public void appendField(String fieldName) {
    fieldNames.add(fieldName);
    setFieldNames(fieldNames);
  }

   @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
  }

  @Override
  public boolean equals(Object other) {
    if (other == null || other.getClass() != OrcStruct.class) {
      return false;
    } else {
      OrcStruct oth = (OrcStruct) other;
      if (fields.length != oth.fields.length) {
        return false;
      }
      for(int i=0; i < fields.length; ++i) {
        if (fields[i] == null) {
          if (oth.fields[i] != null) {
            return false;
          }
        } else {
          if (!fields[i].equals(oth.fields[i])) {
            return false;
          }
        }
      }
      return true;
    }
  }

  @Override
  public int hashCode() {
    int result = fields.length;
    for(Object field: fields) {
      if (field != null) {
        result ^= field.hashCode();
      }
    }
    return result;
  }

  @Override
  public String toString() {
    StringBuilder buffer = new StringBuilder();
    buffer.append("{");
    for(int i=0; i < fields.length; ++i) {
      if (i != 0) {
        buffer.append(", ");
      }
      buffer.append(fields[i]);
    }
    buffer.append("}");
    return buffer.toString();
  }

  public static class Field implements StructField {
    private final String name;
    private final ObjectInspector inspector;
    private final int offset;

    public Field(String name, ObjectInspector inspector, int offset) {
      this.name = name;
      this.inspector = inspector;
      this.offset = offset;
    }

    @Override
    public String getFieldName() {
      return name;
    }

    @Override
    public ObjectInspector getFieldObjectInspector() {
      return inspector;
    }

    @Override
    public String getFieldComment() {
      return null;
    }

    public int getOffset() {
      return offset;
    }
  }

  public static class OrcStructInspector extends SettableStructObjectInspector {
    private final List<StructField> fields;

    public OrcStructInspector(StructTypeInfo info) {
      ArrayList<String> fieldNames = info.getAllStructFieldNames();
      ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
      fields = new ArrayList<StructField>(fieldNames.size());
      for(int i=0; i < fieldNames.size(); ++i) {
        fields.add(new Field(fieldNames.get(i),
            OrcLazyObjectInspectorUtils.createWritableObjectInspector(fieldTypes.get(i)), i));
      }
    }

    public OrcStructInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      int fieldCount = type.getSubtypesCount();
      fields = new ArrayList<StructField>(fieldCount);
      for(int i=0; i < fieldCount; ++i) {
        int fieldType = type.getSubtypes(i);
        fields.add(new Field(type.getFieldNames(i),
            OrcLazyObjectInspectorUtils.createWritableObjectInspector(fieldType, types), i));
      }
    }

    @Override
    public List<StructField> getAllStructFieldRefs() {
      return fields;
    }

    @Override
    public StructField getStructFieldRef(String s) {
      for(StructField field: fields) {
        if (field.getFieldName().equals(s)) {
          return field;
        }
      }
      return null;
    }

    @Override
    public Object getStructFieldData(Object object, StructField field) {
      int offset = ((Field) field).offset;
      OrcStruct struct = (OrcStruct) object;
      if (offset >= struct.fields.length) {
        return null;
      }

      return struct.fields[offset];
    }

    @Override
    public List<Object> getStructFieldsDataAsList(Object object) {
      OrcStruct struct = (OrcStruct) object;
      List<Object> result = new ArrayList<Object>(struct.fields.length);
      for (Object child: struct.fields) {
        result.add(child);
      }
      return result;
    }

    @Override
    public String getTypeName() {
      StringBuilder buffer = new StringBuilder();
      buffer.append("struct<");
      for(int i=0; i < fields.size(); ++i) {
        StructField field = fields.get(i);
        if (i != 0) {
          buffer.append(",");
        }
        buffer.append(field.getFieldName());
        buffer.append(":");
        buffer.append(field.getFieldObjectInspector().getTypeName());
      }
      buffer.append(">");
      return buffer.toString();
    }

    @Override
    public Category getCategory() {
      return Category.STRUCT;
    }

    @Override
    public Object create() {
      return new OrcStruct();
    }

    @Override
    public Object setStructFieldData(Object struct, StructField field,
                                     Object fieldValue) {
      OrcStruct orcStruct = (OrcStruct) struct;
      int offset = ((Field) field).offset;
      // if the offset is bigger than our current number of fields, grow it
      if (orcStruct.getNumFields() <= offset) {
        orcStruct.appendField(((Field) field).getFieldName());
      }
      orcStruct.setFieldValue(offset, fieldValue);
      return struct;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        List<StructField> other = ((OrcStructInspector) o).fields;
        if (other.size() != fields.size()) {
          return false;
        }
        for(int i = 0; i < fields.size(); ++i) {
          StructField left = other.get(i);
          StructField right = fields.get(i);
          if (!(left.getFieldName().equals(right.getFieldName()) &&
                left.getFieldObjectInspector().equals
                    (right.getFieldObjectInspector()))) {
            return false;
          }
        }
        return true;
      }
    }
  }

  public static class OrcMapObjectInspector
      implements MapObjectInspector, SettableMapObjectInspector {
    private final ObjectInspector key;
    private final ObjectInspector value;

    public OrcMapObjectInspector(MapTypeInfo info) {
      key = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getMapKeyTypeInfo());
      value = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getMapValueTypeInfo());
    }

    public OrcMapObjectInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      key = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(0), types);
      value = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(1), types);
    }

    @Override
    public ObjectInspector getMapKeyObjectInspector() {
      return key;
    }

    @Override
    public ObjectInspector getMapValueObjectInspector() {
      return value;
    }

    @Override
    public Object getMapValueElement(Object map, Object key) {
      return ((Map) map).get(key);
    }

    @Override
    @SuppressWarnings("unchecked")
    public Map<Object, Object> getMap(Object map) {
      return (Map) map;
    }

    @Override
    public int getMapSize(Object map) {
      return ((Map) map).size();
    }

    @Override
    public String getTypeName() {
      return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.MAP;
    }

    @Override
    public Object create() {
      return new HashMap<Object,Object>();
    }

    @Override
    public Object put(Object map, Object key, Object value) {
      ((Map) map).put(key, value);
      return map;
    }

    @Override
    public Object remove(Object map, Object key) {
      ((Map) map).remove(key);
      return map;
    }

    @Override
    public Object clear(Object map) {
      ((Map) map).clear();
      return map;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        OrcMapObjectInspector other = (OrcMapObjectInspector) o;
        return other.key.equals(key) && other.value.equals(value);
      }
    }
  }

  public static class OrcListObjectInspector
      implements ListObjectInspector, SettableListObjectInspector {
    private final ObjectInspector child;

    public OrcListObjectInspector(ListTypeInfo info) {
      child = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getListElementTypeInfo());
    }

    public OrcListObjectInspector(int columnId, List<OrcProto.Type> types) {
      OrcProto.Type type = types.get(columnId);
      child = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(0), types);
    }

    @Override
    public ObjectInspector getListElementObjectInspector() {
      return child;
    }

    @Override
    public Object getListElement(Object list, int i) {
      return ((List) list).get(i);
    }

    @Override
    public int getListLength(Object list) {
      return ((List) list).size();
    }

    @Override
    @SuppressWarnings("unchecked")
    public List<?> getList(Object list) {
      return (List) list;
    }

    @Override
    public String getTypeName() {
      return "array<" + child.getTypeName() + ">";
    }

    @Override
    public Category getCategory() {
      return Category.LIST;
    }

    @Override
    public Object create(int size) {
      ArrayList<Object> result = new ArrayList<Object>(size);
      for(int i = 0; i < size; ++i) {
        result.add(null);
      }
      return result;
    }

    @Override
    public Object set(Object list, int index, Object element) {
      List l = (List) list;
      for(int i=l.size(); i < index+1; ++i) {
        l.add(null);
      }
      l.set(index, element);
      return list;
    }

    @Override
    public Object resize(Object list, int newSize) {
      ((ArrayList) list).ensureCapacity(newSize);
      return list;
    }

    @Override
    public boolean equals(Object o) {
      if (o == null || o.getClass() != getClass()) {
        return false;
      } else if (o == this) {
        return true;
      } else {
        ObjectInspector other = ((OrcListObjectInspector) o).child;
        return other.equals(child);
      }
    }
  }
}
