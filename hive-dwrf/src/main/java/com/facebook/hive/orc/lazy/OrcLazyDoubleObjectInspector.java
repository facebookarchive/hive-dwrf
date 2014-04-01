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

import org.apache.hadoop.hive.serde2.io.DoubleWritable;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.DoubleObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;

public class OrcLazyDoubleObjectInspector extends OrcLazyPrimitiveObjectInspector<OrcLazyDouble, DoubleWritable>
    implements DoubleObjectInspector {

  protected OrcLazyDoubleObjectInspector() {
    super(PrimitiveObjectInspectorUtils.doubleTypeEntry);
  }

  public double get(Object o) {
    return ((DoubleWritable)getPrimitiveWritableObject(o)).get();
  }

  @Override
  public Object copyObject(Object o) {
    return o == null ? null : new OrcLazyDouble((OrcLazyDouble) o);
  }

  @Override
  public Object getPrimitiveJavaObject(Object o) {
    DoubleWritable writable = getPrimitiveWritableObject(o);
    return writable == null ? null : Double.valueOf(writable.get());
  }
}
