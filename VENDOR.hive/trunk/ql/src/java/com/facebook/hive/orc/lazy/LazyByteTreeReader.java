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
import java.util.Map;

import org.apache.hadoop.hive.serde2.io.ByteWritable;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.RunLengthByteReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;

public class LazyByteTreeReader extends LazyTreeReader {

  private RunLengthByteReader reader = null;

  public LazyByteTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    reader = new RunLengthByteReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.DATA)));
    if (indexes[columnId] != null) {
      loadIndeces(indexes[columnId].getEntryList(), 0);
    }
  }

  @Override
  public void seek(int index) throws IOException {
    reader.seek(index);
  }

  @Override
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = super.loadIndeces(rowIndexEntries, startIndex);
    return reader.loadIndeces(rowIndexEntries, updatedStartIndex);
  }

  @Override
  public Object next(Object previous) throws IOException {
    ByteWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new ByteWritable();
      } else {
        result = (ByteWritable) previous;
      }
      result.set(reader.next());
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skip(numNonNullValues);
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (reader != null) {
      reader.close();
    }
  }
}
