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
import com.facebook.hive.orc.PositionProvider;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.SerializationUtils;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;
import com.facebook.hive.orc.OrcProto.RowIndex;

abstract class LazyNumericDictionaryTreeReader extends LazyTreeReader {
  protected long[] dictionaryValues;
  protected int dictionarySize;
  protected RunLengthIntegerReader reader;

  LazyNumericDictionaryTreeReader (int columnId, long rowIndexStride) {
    super(columnId, rowIndexStride);
  }

  protected abstract int getNumBytes();

  @Override
  public void startStripe(Map<StreamName, InStream> streams, List<OrcProto.ColumnEncoding> encodings,
      RowIndex[] indexes, long rowBaseInStripe) throws IOException {
    super.startStripe(streams, encodings, indexes, rowBaseInStripe);

    // read the dictionary blob
    dictionarySize = encodings.get(columnId).getDictionarySize();
    dictionaryValues = new long[dictionarySize];
    StreamName name = new StreamName(columnId,
        OrcProto.Stream.Kind.DICTIONARY_DATA);
    InStream in = streams.get(name);
    for(int i = 0; i < dictionarySize; ++i) {
      dictionaryValues[i] = SerializationUtils.readIntegerType(in, WriterImpl.INT_BYTE_SIZE,
          true, in.useVInts());
    }
    in.close();
    // set up the row reader
    name = new StreamName(columnId, OrcProto.Stream.Kind.DATA);
    reader = new RunLengthIntegerReader(streams.get(name), false, getNumBytes());

  }

  @Override
  public void seek(PositionProvider index) throws IOException {
    reader.seek(index);
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skip(numNonNullValues);
  }
}
