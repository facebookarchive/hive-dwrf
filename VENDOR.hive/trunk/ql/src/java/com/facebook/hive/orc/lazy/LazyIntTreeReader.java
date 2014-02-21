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

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;

public class LazyIntTreeReader extends LazyTreeReader {

  protected LazyTreeReader reader;

  public LazyIntTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    switch (encodings.get(columnId).getKind()) {
      case DICTIONARY:
        reader = new LazyIntDictionaryTreeReader(columnId, rowIndexStride);
        break;
      case DIRECT:
        reader = new LazyIntDirectTreeReader(columnId, rowIndexStride);
        break;
      default:
        throw new IllegalArgumentException("Unsupported encoding " +
            encodings.get(columnId).getKind());
    }
    reader.startStripe(streams, encodings, indexes, rowBaseInStripe);
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
    return reader.loadIndeces(rowIndexEntries, startIndex);
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    reader.seek(rowIndexEntry, backwards);
  }

  @Override
  public boolean nextIsNull(long currentRow) throws IOException {
    return reader.nextIsNull(currentRow);
  }

  @Override
  public boolean nextIsNullInComplexType() throws IOException {
    return reader.nextIsNullInComplexType();
  }

  @Override
  public Object getInComplexType(Object previous) throws IOException {
    return reader.getInComplexType(previous);
  }

  @Override
  public Object get(long currentRow, Object previous) throws IOException {
    return reader.get(currentRow, previous);
  }

  @Override
  public Object next(Object previous) throws IOException {
    return reader.next(previous);
  }

  @Override
  public void skipRowsInComplexType(long numRows) throws IOException {
    reader.skipRowsInComplexType(numRows);
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skipRows(numNonNullValues);
  }
}
