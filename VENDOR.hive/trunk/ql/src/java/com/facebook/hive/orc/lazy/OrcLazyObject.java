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

import java.io.DataInput;
import java.io.DataOutput;
import java.io.IOException;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.hive.serde2.ReaderWriterProfiler;
import org.apache.hadoop.io.Writable;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;


public abstract class OrcLazyObject implements Writable {
  private long currentRow = 0;
  private final LazyTreeReader treeReader;
  protected Object previous;
  private boolean nextIsNull;
  private boolean materialized;
  private boolean nextIsNullSet;

  public OrcLazyObject(LazyTreeReader treeReader) {
    this.treeReader = treeReader;
  }

  public OrcLazyObject(OrcLazyObject copy) {
    materialized = copy.materialized;
    currentRow = copy.currentRow;
    nextIsNull = copy.nextIsNull;
    nextIsNullSet = copy.nextIsNullSet;
    previous = copy.previous;
    treeReader = copy.treeReader;
  }

  @Override
  public void write(DataOutput dataOutput) throws IOException {
    throw new UnsupportedOperationException("write unsupported");
  }

  @Override
  public void readFields(DataInput dataInput) throws IOException {
    throw new UnsupportedOperationException("readFields unsupported");
  }

  public LazyTreeReader getLazyTreeReader() {
    return treeReader;
  }

  public Object materialize() throws IOException {
    if (!materialized) {
      ReaderWriterProfiler.start(ReaderWriterProfiler.Counter.DECODING_TIME);
      previous = materialize(currentRow, previous);
      materialized = true;
      nextIsNull = previous == null;
      nextIsNullSet = true;
      ReaderWriterProfiler.end(ReaderWriterProfiler.Counter.DECODING_TIME);
    } else if (nextIsNull) {
      // If the value has been materialized and nextIsNull then the value is null
      return null;
    }
    return previous;
  }

  protected Object materialize(long row, Object previous) throws IOException {
    return treeReader.get(row, previous);
  }

  public void next() {
    currentRow++;
    materialized = false;
    nextIsNullSet = false;
  }

  public boolean nextIsNull() throws IOException {
    if (!nextIsNullSet) {
      ReaderWriterProfiler.start(ReaderWriterProfiler.Counter.DECODING_TIME);
      nextIsNull = treeReader.nextIsNull(currentRow);
      nextIsNullSet = true;
      // If the next value is null, we've essentially just materialized the value
      materialized = nextIsNull;
      ReaderWriterProfiler.end(ReaderWriterProfiler.Counter.DECODING_TIME);
    }
    return nextIsNull;
  }

  public void startStripe(Map<StreamName, InStream> streams,
      List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes,
      long rowBaseInStripe
     ) throws IOException {
    treeReader.startStripe(streams, encodings, indexes, rowBaseInStripe);
  }

  public void seekToRow(long rowNumber) throws IOException {
    currentRow = rowNumber;
  }

  @Override
  public int hashCode() {
    try {
      materialize();
    } catch (IOException e) {
      throw new RuntimeException(e);
    }
    return nextIsNull ? 0 : previous.hashCode();
  }
}
