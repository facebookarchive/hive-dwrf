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
package com.facebook.hive.orc;

import java.io.EOFException;
import java.io.IOException;
import java.util.List;

import com.facebook.hive.orc.OrcProto.RowIndexEntry;

/**
 * Reads a run length encoded stream of bytes.
 * See {@link RunLengthByteWriter} for details about storage.
 */

public class RunLengthByteReader {
  private final InStream input;
  private final byte[] literals = new byte[RunLengthConstants.MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private int used = 0;
  private boolean isRunLengthEncoded = false;
  private int[] indeces;

  public RunLengthByteReader(InStream input) throws IOException {
    this.input = input;
  }

  private void readValues() throws IOException {
    int control = input.read();
    used = 0;
    if (control == RunLengthConstants.END_OF_BUFFER_BYTE) {
      throw new EOFException("Read past end of buffer RLE byte from " + input);
    }
    else if (control < RunLengthConstants.MAX_LITERAL_SIZE) {
      isRunLengthEncoded = true;
      numLiterals = control + RunLengthConstants.MIN_REPEAT_SIZE;
      int repeatedByte = input.read();
      if (repeatedByte == RunLengthConstants.END_OF_BUFFER_BYTE) {
        throw new EOFException("Reading RLE byte got EOF");
      }
      literals[0] = (byte) repeatedByte;
    }
    else {
      isRunLengthEncoded = false;
      numLiterals = 0x100 - control; // convert back from 2's compliment
      int bytes = 0;
      while (bytes < numLiterals) {
        int result = input.read(literals, bytes, numLiterals - bytes);
        if (result == RunLengthConstants.END_OF_BUFFER_BYTE) {
          throw new EOFException("Reading RLE byte literal got EOF");
        }
        bytes += result;
      }
    }
  }

  boolean hasNext() throws IOException {
    return used != numLiterals || input.available() > 0;
  }

  public byte next() throws IOException {
    if (used == numLiterals) {
      readValues();
    }

    byte result = isRunLengthEncoded == true ? literals[0] : literals[used];
    used++;
    return result;
  }

  public void seek(int index) throws IOException {
    input.seek(index);
    int consumed = indeces[index];
    if (consumed != 0) {
      // a loop is required for cases where we break the run into two parts
      while (consumed > 0) {
        readValues();
        used = consumed;
        consumed -= numLiterals;
      }
    } else {
      used = 0;
      numLiterals = 0;
    }
  }

  /**
   * Read in the number of bytes consumed at each index entry and store it,
   * also call loadIndeces on child stream and return the index of the next
   * streams indexes.
   */
  public int loadIndeces(List<RowIndexEntry> rowIndexEntries, int startIndex) {
    int updatedStartIndex = input.loadIndeces(rowIndexEntries, startIndex);

    int numIndeces = rowIndexEntries.size();
    indeces = new int[numIndeces + 1];
    int i = 0;
    for (RowIndexEntry rowIndexEntry : rowIndexEntries) {
      indeces[i] = (int) rowIndexEntry.getPositions(updatedStartIndex);
      i++;
    }
    return updatedStartIndex + 1;
  }

  public void skip(long items) throws IOException {
    while (items > 0) {
      if (used == numLiterals) {
        readValues();
      }
      long consume = Math.min(items, numLiterals - used);
      used += consume;
      items -= consume;
    }
  }

  public void close() throws IOException {
    input.close();
  }
}
