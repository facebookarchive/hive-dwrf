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

import org.apache.hadoop.io.Text;

import com.facebook.hive.orc.BitFieldReader;
import com.facebook.hive.orc.DynamicByteArray;
import com.facebook.hive.orc.InStream;
import com.facebook.hive.orc.OrcProto;
import com.facebook.hive.orc.OrcProto.RowIndex;
import com.facebook.hive.orc.OrcProto.RowIndexEntry;
import com.facebook.hive.orc.RunLengthIntegerReader;
import com.facebook.hive.orc.StreamName;
import com.facebook.hive.orc.WriterImpl;

public class LazyStringDictionaryTreeReader extends LazyTreeReader {
  private DynamicByteArray dictionaryBuffer = null;
  private DynamicByteArray strideDictionaryBuffer;
  private int dictionarySize;
  private int[] strideDictionarySizes;
  private int[] dictionaryOffsets;
  private int[] strideDictionaryOffsets;
  private RunLengthIntegerReader reader;
  private BitFieldReader inDictionary;
  private InStream directReader;
  private RunLengthIntegerReader directLengths;
  private int currentUnitDictionary = -1;

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
    InStream inDictionaryStream = streams.get(new StreamName(columnId, OrcProto.Stream.Kind.IN_DICTIONARY));
    inDictionary = inDictionaryStream == null ? null : new BitFieldReader(inDictionaryStream);
    directReader = streams.get(new StreamName(columnId, OrcProto.Stream.Kind.STRIDE_DICTIONARY));
    InStream directLengthsStream = streams.get(new StreamName(columnId,
        OrcProto.Stream.Kind.STRIDE_DICTIONARY_LENGTH));
    directLengths = directLengthsStream == null ? null : new RunLengthIntegerReader(
        directLengthsStream, false, WriterImpl.INT_BYTE_SIZE);
    if (indexes[columnId] != null) {
      loadIndeces(indexes[columnId].getEntryList(), 0);
    }
  }

  @Override
  public void seek(int index) throws IOException {
    reader.seek(index);
    if (inDictionary != null) {
      inDictionary.seek(index);
    }
  }

  @Override
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = super.loadIndeces(rowIndexEntries, startIndex);
    if (inDictionary != null) {
      updatedStartIndex = directReader.loadIndeces(rowIndexEntries, updatedStartIndex);
      updatedStartIndex = directLengths.loadIndeces(rowIndexEntries, updatedStartIndex);
      int numIndeces = rowIndexEntries.size();
      strideDictionarySizes = new int[numIndeces + 1];
      int i = 0;
      for (RowIndexEntry rowIndexEntry : rowIndexEntries) {
        strideDictionarySizes[i] = (int) rowIndexEntry.getPositions(updatedStartIndex);
        i++;
      }
      updatedStartIndex++;
      updatedStartIndex = reader.loadIndeces(rowIndexEntries, updatedStartIndex);
      return inDictionary.loadIndeces(rowIndexEntries, updatedStartIndex);
    } else {
      updatedStartIndex = reader.loadIndeces(rowIndexEntries, updatedStartIndex);
      return updatedStartIndex;
    }
  }

  private void nextFromDictionary(Text result) throws IOException {
    int entry = (int) reader.next();
    int offset = dictionaryOffsets[entry];
    int length = dictionaryOffsets[entry + 1] - dictionaryOffsets[entry];

    // If the column is just empty strings, the size will be zero, so the buffer will be null,
    // in that case just return result as it will default to empty
    if (dictionaryBuffer != null) {
      dictionaryBuffer.setText(result, offset, length);
    } else {
      result.clear();
    }
  }

  private void loadStrideDictionary(int indexEntry) throws IOException {
    currentUnitDictionary = indexEntry;
    int offset = 0;
    int unitDictionarySize = strideDictionarySizes[indexEntry];
    if (strideDictionaryOffsets == null ||
        strideDictionaryOffsets.length < unitDictionarySize + 1) {
      strideDictionaryOffsets = new int[unitDictionarySize + 1];
    }
    directLengths.seek(indexEntry);
    for(int i=0; i < unitDictionarySize; ++i) {
      strideDictionaryOffsets[i] = offset;
      offset += (int) directLengths.next();
    }
    strideDictionaryOffsets[unitDictionarySize] = offset;
    if (offset != 0) {
      directReader.seek(indexEntry);
      strideDictionaryBuffer = new DynamicByteArray(offset);
      strideDictionaryBuffer.read(directReader, offset);
    } else {
      // It only contains the empty string
      strideDictionaryBuffer = null;
    }
  }

  private void nextFromStrideDictionary(Text result) throws IOException {
    int indexEntry = computeRowIndexEntry(previousRow);
    if (indexEntry != currentUnitDictionary) {
      loadStrideDictionary(indexEntry);
    }
    int entry = (int) reader.next();
    int offset = strideDictionaryOffsets[entry];
    int length;
    // if it isn't the last entry, subtract the offsets otherwise use
    // the buffer length.
    if (entry < strideDictionaryOffsets.length - 1) {
      length = strideDictionaryOffsets[entry + 1] - offset;
    } else {
      length = strideDictionaryBuffer.size() - offset;
    }
    // If the column is just empty strings, the size will be zero, so the buffer will be null,
    // in that case just return result as it will default to empty
    if (strideDictionaryBuffer != null) {
      strideDictionaryBuffer.setText(result, offset, length);
    } else {
      result.clear();
    }
  }

  @Override
  public Object next(Object previous) throws IOException {
    Text result = null;
    if (valuePresent) {
      if (previous == null) {
        result = new Text();
      } else {
        result = (Text) previous;
      }
      boolean isDictionaryEncoded = inDictionary == null ||  inDictionary.next() == 1;
      if  (isDictionaryEncoded) {
        nextFromDictionary(result);
      } else {
        nextFromStrideDictionary(result);
      }
    }
    return result;
  }

  @Override
  public void skipRows(long numNonNullValues) throws IOException {
    reader.skip(numNonNullValues);
    if (inDictionary != null) {
      inDictionary.skip(numNonNullValues);
    }
  }

  @Override
  public void close() throws IOException {
    super.close();
    if (reader != null) {
      reader.close();
    }
    if (inDictionary != null) {
      inDictionary.close();
    }
    if (directReader != null) {
      directReader.close();
    }
    if (directLengths != null) {
      directLengths.close();
    }
    dictionaryBuffer = null;
    strideDictionaryBuffer = null;
    strideDictionarySizes = null;
    dictionaryOffsets = null;
    strideDictionaryOffsets = null;
  }
}
