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

import org.apache.hadoop.io.FloatWritable;

import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.SerializationUtils;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;
import com.facebook.hive.orc.lazy.OrcLazyObject.ValueNotPresentException;

public class LazyFloatTreeReader extends LazyTreeReader {

  private InStream stream;
  private float latestRead = 0; //< Last float that was read from stream.

  public LazyFloatTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);
    StreamName name = new StreamName(columnId,
        OrcProto.Stream.Kind.DATA);
    stream = streams.get(name);
    if (indexes[columnId] != null) {
      loadIndeces(indexes[columnId].getEntryList(), 0);
    }
  }

  @Override
  public void seek(int index) throws IOException {
    stream.seek(index);
  }

  @Override
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = super.loadIndeces(rowIndexEntries, startIndex);
    return stream.loadIndeces(rowIndexEntries, updatedStartIndex);
  }

  /**
   * Read a float value from the stream.
   */
  private float readFloat() throws IOException {
    latestRead = SerializationUtils.readFloat(stream);
    return latestRead;
  }


  FloatWritable createWritable(Object previous, float value) throws IOException {
    FloatWritable result = null;
    if (previous == null) {
      result = new FloatWritable();
    } else {
      result = (FloatWritable) previous;
    }
    result.set(value);
    return result;
  }

  @Override
  public Object createWritableFromLatest(Object previous) throws IOException {
    return createWritable(previous, latestRead);
  }

  /**
   * Give the next float as a primitive.
   */
  @Override
  public float nextFloat(boolean readStream) throws IOException, ValueNotPresentException {
    if (!readStream) {
      return latestRead;
    }
    if (!valuePresent) {
      throw new ValueNotPresentException("Cannot materialize float..");
    }
    return readFloat();
  }

  @Override
  public Object next(Object previous) throws IOException {
    FloatWritable result = null;
    if (valuePresent) {
      result = createWritable(previous, readFloat());
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    for(int i=0; i < numNonNullValues; ++i) {
      SerializationUtils.readFloat(stream);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (stream != null) {
      stream.close();
    }
  }
}
