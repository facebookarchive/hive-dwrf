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

import com.facebook.hive.orc.DynamicByteArray;
import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;
import com.facebook.hive.orc.OrcProto.RowIndex;
import org.apache.hadoop.io.Text;

class LazyStringDictionaryTreeReader extends LazyTreeReader {
  private DynamicByteArray dictionaryBuffer = null;
  private int dictionarySize;
  private int[] dictionaryOffsets;
  private RunLengthIntegerReader reader;

  LazyStringDictionaryTreeReader(int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);

    dictionarySize = encodings.get(columnId).getDictionarySize();

    // read the lengths
    StreamName name = new StreamName(columnId, OrcProto.Stream.Kind.LENGTH);
    InStream in = streams.get(name);
    RunLengthIntegerReader lenReader = new RunLengthIntegerReader(in, false,
        WriterImpl.INT_BYTE_SIZE);
    int offset = 0;
    if (dictionaryOffsets == null ||
        dictionaryOffsets.length < dictionarySize + 1) {
      dictionaryOffsets = new int[dictionarySize + 1];
    }
    for(int i=0; i < dictionarySize; ++i) {
      dictionaryOffsets[i] = offset;
      offset += (int) lenReader.next();
    }
    dictionaryOffsets[dictionarySize] = offset;
    in.close();

    // read the dictionary blob
    name = new StreamName(columnId,
        OrcProto.Stream.Kind.DICTIONARY_DATA);
    in = streams.get(name);
    if (in.available() > 0) {
      dictionaryBuffer = new DynamicByteArray(dictionaryOffsets[dictionarySize]);
      dictionaryBuffer.readAll(in);
    } else {
      dictionaryBuffer = null;
    }
    in.close();

    // set up the row reader
    name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
    reader = new RunLengthIntegerReader(streams.get(name), false, WriterImpl.INT_BYTE_SIZE);
  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    reader.seek(index);
  }

  @Override
  public Object next(Object previous) throws IOException {
    Text result = null;
    if (valuePresent) {
      int entry = (int) reader.next();
      if (previous == null) {
        result = new Text();
      } else {
        result = (Text) previous;
      }
      int offset = dictionaryOffsets[entry];
      int length;
      // if it isn't the last entry, subtract the offsets otherwise use
      // the buffer length.
      if (entry < dictionaryOffsets.length - 1) {
        length = dictionaryOffsets[entry + 1] - offset;
      } else {
        length = dictionaryBuffer.size() - offset;
      }
      // If the column is just empty strings, the size will be zero, so the buffer will be null,
      // in that case just return result as it will default to empty
      if (dictionaryBuffer != null) {
        dictionaryBuffer.setText(result, offset, length);
      } else {
        result.clear();
      }
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skip(numNonNullValues);
  }
}
