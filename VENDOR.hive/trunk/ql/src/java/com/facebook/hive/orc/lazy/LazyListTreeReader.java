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
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;

public class LazyListTreeReader extends LazyTreeReader {
  private final LazyTreeReader elementReader;
  private RunLengthIntegerReader lengths;

  public LazyListTreeReader(int columnId, long rowIndexStride, LazyTreeReader elementReader) {
    super(columnId, rowIndexStride);
    this.elementReader = elementReader;
  }

  @Override
  public Object next(Object previous) throws IOException {
    List<Object> result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new ArrayList<Object>();
      } else {
        result = (ArrayList<Object>) previous;
      }
      int prevLength = result.size();
      int length = nextLength();
      // extend the list to the new length
      for(int i=prevLength; i < length; ++i) {
        result.add(null);
      }
      // read the new elements into the array
      for(int i=0; i< length; i++) {
        result.set(i, elementReader.getInComplexType(i < prevLength ?
            result.get(i) : null));
      }
      // remove any extra elements
      for(int i=prevLength - 1; i >= length; --i) {
        result.remove(i);
      }
    }
    return result;
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    super.seek(rowIndexEntry, backwards);
    elementReader.seek(rowIndexEntry, backwards);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams,
      List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes,
      long rowBaseInStripe
     ) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    elementReader.startStripe(streams, encodings, indexes, rowBaseInStripe);
    lengths = new RunLengthIntegerReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.LENGTH)), false, WriterImpl.INT_BYTE_SIZE);
    if (indexes[columnId] != null) {
      loadIndeces(indexes[columnId].getEntryList(), 0);
    }
  }

  @Override
  public void seek(int index) throws IOException {
    lengths.seek(index);
  }

  @Override
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = super.loadIndeces(rowIndexEntries, startIndex);
    return lengths.loadIndeces(rowIndexEntries, updatedStartIndex);
  }

  public int nextLength() throws IOException {
    return (int) lengths.next();
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    long childSkip = 0;
    for(long i=0; i < numNonNullValues; ++i) {
      childSkip += lengths.next();
    }
    elementReader.skipRowsInComplexType(childSkip);
  }
}
