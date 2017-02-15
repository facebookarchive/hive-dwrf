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

import java.sql.Timestamp;

import org.apache.hadoop.hive.serde2.io.TimestampWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.TimestampObjectInspector;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfoFactory;

public class OrcLazyTimestampObjectInspector extends
    OrcLazyPrimitiveObjectInspector<OrcLazyTimestamp, TimestampWritable> implements TimestampObjectInspector {

  protected OrcLazyTimestampObjectInspector() {
    super(PrimitiveObjectInspectorUtils.timestampTypeEntry);
  }

  @Override
  public Timestamp getPrimitiveJavaObject(Object o) {
    TimestampWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : writable.getTimestamp();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyTimestamp((OrcLazyTimestamp) o);
  }

  @SuppressWarnings({"override", "UnusedDeclaration", "RedundantCast"}) // FB Hive
  public PrimitiveTypeInfo getTypeInfo() {
    return (PrimitiveTypeInfo) TypeInfoFactory.timestampTypeInfo;
  }
}
