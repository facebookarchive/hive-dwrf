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

import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils.PrimitiveTypeEntry;
import org.apache.hadoop.io.Writable;

public abstract class OrcLazyPrimitiveObjectInspector<T extends OrcLazyObject, U extends Writable> implements PrimitiveObjectInspector {

  protected final transient PrimitiveTypeEntry typeEntry;

  protected OrcLazyPrimitiveObjectInspector(PrimitiveTypeEntry typeEntry) {
    this.typeEntry = typeEntry;
  }

  @SuppressWarnings("unchecked")
  @Override
  public U getPrimitiveWritableObject(Object o) {
    try {
      return o == null ? null : (U)((T)o).materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public boolean preferWritable() {
    return true;
  }

  /**
   * Return the associated Java primitive class for this primitive
   * ObjectInspector.
   */
  @Override
  public Class<?> getJavaPrimitiveClass() {
    return typeEntry.primitiveJavaClass;
  }

  /**
   * Return the associated primitive category for this primitive
   * ObjectInspector.
   */
  @Override
  public PrimitiveCategory getPrimitiveCategory() {
    return typeEntry.primitiveCategory;
  }

  /**
   * Return the associated primitive Writable class for this primitive
   * ObjectInspector.
   */
  @Override
  public Class<?> getPrimitiveWritableClass() {
    return typeEntry.primitiveWritableClass;
  }

  /**
   * Return the associated category this primitive ObjectInspector.
   */
  @Override
  public Category getCategory() {
    return Category.PRIMITIVE;
  }

  /**
   * Return the type name for this primitive ObjectInspector.
   */
  @Override
  public String getTypeName() {
    return typeEntry.typeName;
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
}
