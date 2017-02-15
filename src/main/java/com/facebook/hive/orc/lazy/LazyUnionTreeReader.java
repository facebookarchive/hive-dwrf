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
import com.facebook.hive.orc.OrcUnion;
import com.facebook.hive.orc.RunLengthByteReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;

public class LazyUnionTreeReader extends LazyTreeReader {

  private final LazyTreeReader[] fields;
  private RunLengthByteReader tags;

  public LazyUnionTreeReader(int columnId, long rowIndexStride, LazyTreeReader[] fields) {
    super(columnId, rowIndexStride);
    this.fields = fields;
  }

  @Override
  public Object next(Object previous) throws IOException {
    OrcUnion result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new OrcUnion();
      } else {
        result = (OrcUnion) previous;
      }
      byte tag = nextTag();
      Object previousVal = result.getObject();
      result.set(tag, fields[tag].getInComplexType(tag == result.getTag() ?
          previousVal : null, previousRow));
    }
    return result;
  }

  @Override
  protected void seek(int rowIndexEntry, boolean backwards) throws IOException {
    super.seek(rowIndexEntry, backwards);
    for (LazyTreeReader field : fields) {
      field.seek(rowIndexEntry, backwards);
    }
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    for (int i = 0; i < fields.length; i++) {
      fields[i].startStripe(streams, encodings, indexes, rowBaseInStripe);
    }
    tags = new RunLengthByteReader(streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.DATA)));
    if (indexes[columnId] != null) {
      loadIndeces(indexes[columnId].getEntryList(), 0);
    }
  }

  @Override
  public void seek(int index) throws IOException {
    tags.seek(index);
  }

  @Override
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = super.loadIndeces(rowIndexEntries, startIndex);
    return tags.loadIndeces(rowIndexEntries, updatedStartIndex);
  }

  public byte nextTag() throws IOException {
    return (byte) tags.next();
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    long[] counts = new long[fields.length];
    for(int i=0; i < numNonNullValues; ++i) {
      counts[tags.next()] += 1;
    }
    for(int i=0; i < counts.length; ++i) {
      fields[i].skipRowsInComplexType(counts[i]);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    for (LazyTreeReader field : fields) {
      field.close();
    }
    if (tags != null) {
      tags.close();
    }
  }
}
