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
import com.facebook.hive.orc.OrcStruct.Field;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.typeinfo.StructTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

public class OrcLazyRowObjectInspector extends OrcLazyStructObjectInspector {

  public OrcLazyRowObjectInspector(StructTypeInfo info) {
    super(info.getAllStructFieldNames().size());
    ArrayList<String> fieldNames = info.getAllStructFieldNames();
    ArrayList<TypeInfo> fieldTypes = info.getAllStructFieldTypeInfos();
    for(int i=0; i < fieldNames.size(); ++i) {
      fields.add(new Field(fieldNames.get(i),
          OrcLazyObjectInspectorUtils.createLazyObjectInspector(fieldTypes.get(i)), i));
    }
  }

  public OrcLazyRowObjectInspector(int columnId, List<OrcProto.Type> types) {
    super(types.get(columnId).getSubtypesCount());
    OrcProto.Type type = types.get(columnId);
    int fieldCount = type.getSubtypesCount();
    for(int i=0; i < fieldCount; ++i) {
      int fieldType = type.getSubtypes(i);
      fields.add(new Field(type.getFieldNames(i),
          OrcLazyObjectInspectorUtils.createLazyObjectInspector(fieldType, types), i));
    }
  }

  @Override
  public Object getStructFieldData(Object data, StructField fieldRef) {
    int offset = ((Field) fieldRef).getOffset();

    try {
      OrcLazyObject obj = ((OrcLazyRow) data).getFieldValue(offset);
      if (obj != null) {
        obj.materialize();
        return obj.nextIsNull() ? null : obj;
      }
      return null;
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
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
    OrcLazyRow row = (OrcLazyRow) data;
    int numFields = row.getNumFields();
    List<Object> result = new ArrayList<Object>(numFields);
    for (int i = 0; i < numFields; i++) {
      try {
        OrcLazyObject obj = (OrcLazyObject) row.getFieldValue(i);
        result.add(obj == null || obj.nextIsNull() ? null : obj);
      } catch (IOException e) {
        throw new RuntimeException(e);
      }

    }
    return result;
  }
}
