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
import com.facebook.hive.orc.OrcUnion;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.UnionObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.UnionTypeInfo;

public class OrcLazyUnionObjectInspector implements UnionObjectInspector {

  private final List<ObjectInspector> children;

  public OrcLazyUnionObjectInspector(int columnId, List<OrcProto.Type> types) {
    OrcProto.Type type = types.get(columnId);
    children = new ArrayList<ObjectInspector>(type.getSubtypesCount());
    for(int i=0; i < type.getSubtypesCount(); ++i) {
      children.add(OrcLazyObjectInspectorUtils.createWritableObjectInspector(type.getSubtypes(i),
          types));
    }
  }

  public OrcLazyUnionObjectInspector(UnionTypeInfo info) {
    List<TypeInfo> unionChildren = info.getAllUnionObjectTypeInfos();
    this.children = new ArrayList<ObjectInspector>(unionChildren.size());
    for(TypeInfo child: info.getAllUnionObjectTypeInfos()) {
      this.children.add(OrcLazyObjectInspectorUtils.createWritableObjectInspector(child));
    }
  }

  private OrcUnion get(Object o) {
    if (o == null) {
      return null;
    }

    try {
      return (OrcUnion) ((OrcLazyUnion) o).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public Object getField(Object o) {
    if (o == null) {
      return null;
    }

    return get(o).getObject();
  }

  @Override
  public List<ObjectInspector> getObjectInspectors() {
    return children;
  }

  @Override
  public byte getTag(Object o) {
    if (o == null) {
      return -1;
    }

    return get(o).getTag();
  }

  @Override
  public Category getCategory() {
    return Category.UNION;
  }

  @Override
  public String getTypeName() {
    StringBuilder builder = new StringBuilder("uniontype<");
    boolean first = true;
    for(ObjectInspector child: children) {
      if (first) {
        first = false;
      } else {
        builder.append(",");
      }
      builder.append(child.getTypeName());
    }
    builder.append(">");
    return builder.toString();
  }

  @Override
  public boolean equals(Object o) {
    if (o == null || o.getClass() != getClass()) {
      return false;
    } else if (o == this) {
      return true;
    } else {
      List<ObjectInspector> other = ((OrcLazyUnionObjectInspector) o).children;
      if (other.size() != children.size()) {
        return false;
      }
      for(int i = 0; i < children.size(); ++i) {
        if (!other.get(i).equals(children.get(i))) {
          return false;
        }
      }
      return true;
    }
  }
}
