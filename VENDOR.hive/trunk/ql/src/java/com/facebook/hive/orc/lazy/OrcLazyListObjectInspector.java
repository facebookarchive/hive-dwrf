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

import com.facebook.hive.orc.OrcProto;
import org.apache.hadoop.hive.serde2.objectinspector.ListObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.ListTypeInfo;

public class OrcLazyListObjectInspector implements ListObjectInspector {

  private final ObjectInspector child;

  public OrcLazyListObjectInspector(ListTypeInfo info) {
    child = OrcLazyObjectInspectorUtils.createWritableObjectInspector(info.getListElementTypeInfo());
  }

  public OrcLazyListObjectInspector(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    child = OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(0), types);
  }

  @Override
  public List<?> getList(Object data) {
    if (data == null) {
      return null;
    }

    try {
      return (List<?>) ((OrcLazyList) data).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object getListElement(Object data, int index) {
    if (data == null) {
      return null;
    }

    return getList(data).get(index);
  }

  @Override
  public ObjectInspector getListElementObjectInspector() {
    return child;
  }

  @Override
  public int getListLength(Object data) {
    if (data == null) {
      return -1;
    }

    return getList(data).size();
  }

  @Override
  public Category getCategory() {
    return Category.LIST;
  }

  @Override
  public String getTypeName() {
    return "array<" + child.getTypeName() + ">";
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      ObjectInspector other = ((OrcLazyListObjectInspector) o).child;
      return other.equals(child);
    }
  }
}
