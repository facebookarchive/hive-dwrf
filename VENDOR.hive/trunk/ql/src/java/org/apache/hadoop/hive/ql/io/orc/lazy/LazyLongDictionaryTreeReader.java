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

package org.apache.hadoop.hive.ql.io.orc.lazy;

import java.io.IOException;

import org.apache.hadoop.hive.ql.io.orc.WriterImpl;
import org.apache.hadoop.io.LongWritable;

class LazyLongDictionaryTreeReader extends LazyNumericDictionaryTreeReader {

  LazyLongDictionaryTreeReader (int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  protected int getNumBytes() {
    return WriterImpl.LONG_BYTE_SIZE;
  }

  @Override
  public Object next(Object previous) throws IOException {
    LongWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new LongWritable();
      } else {
        result = (LongWritable) previous;
      }
      int key = (int) reader.next();
      long resultVal = (long) dictionaryValues[key];
      result.set((long) resultVal);
    }
    return result;
  }
}
