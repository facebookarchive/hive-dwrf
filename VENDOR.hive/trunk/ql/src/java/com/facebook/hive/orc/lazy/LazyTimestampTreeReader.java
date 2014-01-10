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
import java.sql.Timestamp;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;
import com.facebook.hive.orc.OrcProto.RowIndex;
import org.apache.hadoop.hive.serde2.io.TimestampWritable;

public class LazyTimestampTreeReader extends LazyTreeReader {

  private RunLengthIntegerReader data;
  private RunLengthIntegerReader nanos;

  public LazyTimestampTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    data = new RunLengthIntegerReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.DATA)), true, WriterImpl.LONG_BYTE_SIZE);
    nanos = new RunLengthIntegerReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.NANO_DATA)), false, WriterImpl.LONG_BYTE_SIZE);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    data.seek(index);
    nanos.seek(index);
  }

  @Override
  public Object next(Object previous) throws IOException {
    TimestampWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new TimestampWritable();
      } else {
        result = (TimestampWritable) previous;
      }
      long millis = (data.next() + WriterImpl.BASE_TIMESTAMP) *
          WriterImpl.MILLIS_PER_SECOND;
      int newNanos = parseNanos(nanos.next());
      // fix the rounding when we divided by 1000.
      if (millis >= 0) {
        millis += newNanos / 1000000;
      } else {
        millis -= newNanos / 1000000;
      }
      Timestamp timestamp = result.getTimestamp();
      timestamp.setTime(millis);
      timestamp.setNanos(newNanos);
      result.set(timestamp);
    }
    return result;
  }

  private static int parseNanos(long serialized) {
    int zeros = 7 & (int) serialized;
    int result = (int) serialized >>> 3;
    if (zeros != 0) {
      for(int i =0; i <= zeros; ++i) {
        result *= 10;
      }
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    data.skip(numNonNullValues);
    nanos.skip(numNonNullValues);
  }
}
