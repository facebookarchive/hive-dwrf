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

import com.facebook.hive.orc.WriterImpl;
import org.apache.hadoop.hive.serde2.io.ShortWritable;
import com.facebook.hive.orc.lazy.OrcLazyObject.ValueNotPresentException;

class LazyShortDictionaryTreeReader extends LazyNumericDictionaryTreeReader {
  LazyShortDictionaryTreeReader (int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  protected int getNumBytes() {
    return WriterImpl.SHORT_BYTE_SIZE;
  }

  private int latestIndex; //< Latest key that was read from reader.

  /**
   * Read an short value from the stream.
   */
  private short readShort() throws IOException {
    latestIndex = (int) reader.next();
    return (short) dictionaryValues[latestIndex];
  }

  private short latestValue() {
    return (short) dictionaryValues[latestIndex];
  }

  ShortWritable createWritable(Object previous, short v) throws IOException {
    ShortWritable result = null;
    if (previous == null) {
      result = new ShortWritable();
    } else {
      result = (ShortWritable) previous;
    }
    result.set(v);
    return result;
  }

  @Override
  public Object createWritableFromLatest(Object previous) throws IOException {
    return createWritable(previous, latestValue());
  }

  /**
   * Give the next short as a primitive
   */
  @Override
  public short nextShort(boolean readStream) throws IOException {
    if (!readStream)
      return latestValue();
    if (!valuePresent)
      throw new ValueNotPresentException("Cannot materialize short.");
    return readShort();
  }

  @Override
  public Object next(Object previous) throws IOException {
    ShortWritable result = null;
    if (valuePresent) {
      result = createWritable(previous, readShort());
    }
    return result;
  }

}
