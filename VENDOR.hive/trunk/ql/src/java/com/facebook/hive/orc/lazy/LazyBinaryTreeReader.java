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

import java.io.EOFException;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;

public class LazyBinaryTreeReader extends LazyTreeReader {

  private InStream stream;
  private RunLengthIntegerReader lengths;

  public LazyBinaryTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    StreamName name = new StreamName(columnId,
        OrcProto.Stream.Kind.DATA);
    stream = streams.get(name);
    lengths = new RunLengthIntegerReader(streams.get(new
        StreamName(columnId, OrcProto.Stream.Kind.LENGTH)),
        false, WriterImpl.INT_BYTE_SIZE);
    if (indexes[columnId] != null) {
      loadIndeces(indexes[columnId].getEntryList(), 0);
    }
  }

  @Override
  public void seek(int index) throws IOException {
    stream.seek(index);
    lengths.seek(index);
  }

  @Override
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = super.loadIndeces(rowIndexEntries, startIndex);
    updatedStartIndex = stream.loadIndeces(rowIndexEntries, updatedStartIndex);
    return lengths.loadIndeces(rowIndexEntries, updatedStartIndex);
  }

  @Override
  public Object next(Object previous) throws IOException {
    BytesWritable result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new BytesWritable();
      } else {
        result = (BytesWritable) previous;
      }
      int len = (int) lengths.next();
      result.setSize(len);
      int offset = 0;
      while (len > 0) {
        int written = stream.read(result.getBytes(), offset, len);
        if (written < 0) {
          throw new EOFException("Can't finish byte read from " + stream);
        }
        len -= written;
        offset += written;
      }
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    long lengthToSkip = 0;
    for(int i=0; i < numNonNullValues; ++i) {
      lengthToSkip += lengths.next();
    }
    stream.skip(lengthToSkip);
  }

}
