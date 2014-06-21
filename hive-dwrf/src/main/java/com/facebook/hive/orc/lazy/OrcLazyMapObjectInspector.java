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
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.objectinspector.MapObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.MapTypeInfo;

import com.facebook.hive.orc.OrcProto;

public class OrcLazyMapObjectInspector implements MapObjectInspector {

  private final ObjectInspector key;
  private final ObjectInspector value;

  public OrcLazyMapObjectInspector(MapTypeInfo info) {
    key = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getMapKeyTypeInfo());
    value = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getMapValueTypeInfo());
  }

  public OrcLazyMapObjectInspector(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    key = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(0), types);
    value = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(1), types);
  }

  @Override
  public Map<?, ?> getMap(Object data) {
    if (data == null) {
      return null;
    }

    try {
      return (Map<?, ?>) ((OrcLazyMap) data).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public ObjectInspector getMapKeyObjectInspector() {
    return key;
  }

  @Override
  public int getMapSize(Object data) {
    if (data == null) {
      return -1;
    }

    return getMap(data).size();
  }

  @Override
  public Object getMapValueElement(Object data, Object key) {
    if (data == null) {
      return null;
    }

    return getMap(data).get(key);
  }

  @Override
  public ObjectInspector getMapValueObjectInspector() {
    return value;
  }

  @Override
  public Category getCategory() {
    return Category.MAP;
  }

  @Override
  public String getTypeName() {
    return "map<" + key.getTypeName() + "," + value.getTypeName() + ">";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      OrcLazyMapObjectInspector other = (OrcLazyMapObjectInspector) o;
      return other.key.equals(key) && other.value.equals(value);
    }
  }

}
