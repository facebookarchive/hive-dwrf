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

import com.facebook.hive.orc.SerializationUtils;
import com.facebook.hive.orc.WriterImpl;
import org.apache.hadoop.hive.serde2.io.ShortWritable;

class LazyShortDirectTreeReader extends LazyNumericDirectTreeReader {
  LazyShortDirectTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public Object next(Object previous) throws IOException {
    ShortWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new ShortWritable();
      } else {
        result = (ShortWritable) previous;
      }
      result.set((short)SerializationUtils.readIntegerType(input, WriterImpl.SHORT_BYTE_SIZE,
            true, input.useVInts()));
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    for (int i = 0; i < numNonNullValues; i++) {
      SerializationUtils.readIntegerType(input, WriterImpl.SHORT_BYTE_SIZE,
          true, input.useVInts());
    }
  }
}
