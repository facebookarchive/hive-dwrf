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

import org.apache.hadoop.hive.serde2.objectinspector.primitive.IntObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;
import org.apache.hadoop.io.IntWritable;

public class OrcLazyIntObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyInt, IntWritable> implements
    IntObjectInspector {

  protected OrcLazyIntObjectInspector() {
    super(PrimitiveObjectInspectorUtils.intTypeEntry);
  }

  @Override
  public int get(Object o) {
    return ((IntWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyInt((OrcLazyInt) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    IntWritable writable = (IntWritable) getPrimitiveWritableObject(o);
    return writable == null ? null : Integer.valueOf(writable.get());
  }

  @SuppressWarnings({"override", "UnusedDeclaration", "RedundantCast"}) // FB Hive
  public PrimitiveTypeInfo getTypeInfo() {
    return (PrimitiveTypeInfo) TypeInfoFactory.intTypeInfo;
  }
}
