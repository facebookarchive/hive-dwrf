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

package com.facebook.hive.orc.lazy;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcStruct;
import com.facebook.hive.orc.OrcStruct.Field;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class OrcLazyStructObjectInspector extends StructObjectInspector {

  protected final List<StructField> fields;

  protected OrcLazyStructObjectInspector() {
    this.fields = new ArrayList<StructField>();
  }

  public OrcLazyStructObjectInspector(StructTypeInfo info) {
    ArrayList<String> fieldNames = info.getAllStructFieldNames();
    ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
    fields = new ArrayList<StructField>(fieldNames.size());
    for(int i=0; i < fieldNames.size(); ++i) {
      fields.add(new Field(fieldNames.get(i),
          OrcLazyObjectInspectorUtils.createWritableObjectInspector(fieldTypes.get(i)), i));
    }
  }

  public OrcLazyStructObjectInspector(int columnId, List<OrcProto.Type> types) {
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
  public List<? extends StructField> getAllStructFieldRefs() {
    return fields;
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    if (data == null) {
      return null;
    }

    int offset = ((Field) fieldRef).getOffset();
    OrcStruct struct;
    try {
      struct = (OrcStruct) ((OrcLazyStruct) data).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    if (offset >= struct.getNumFields()) {
      return null;
    }

    return struct.getFieldValue(offset);
  }

  @Override
  public StructField getStructFieldRef(String fieldName) {
    for(StructField field: fields) {
      if (field.getFieldName().equals(fieldName)) {
        return field;
      }
    }
    return null;
  }

  @Override
  public List<Object> getStructFieldsDataAsList(Object data) {
    if (data == null) {
      return null;
    }

    OrcStruct struct;
    try {
      struct = (OrcStruct) ((OrcLazyStruct) data).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    List<Object> result = new ArrayList<Object>(struct.getNumFields());
    for (int i = 0; i < struct.getNumFields(); i++) {
      result.add(struct.getFieldValue(i));
    }
    return result;
  }

  @Override
  public Category getCategory() {
    return Category.STRUCT;
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
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      List<StructField> other = ((OrcLazyStructObjectInspector) o).fields;
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
