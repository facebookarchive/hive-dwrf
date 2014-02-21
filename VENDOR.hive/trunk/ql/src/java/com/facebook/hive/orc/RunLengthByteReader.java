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
 * A reader that reads a sequence of bytes. A control byte is read before
 * each run with positive values 0 to 127 meaning 3 to 130 repetitions. If the
 * byte is -1 to -128, 1 to 128 literal byte values follow.
 */
public class RunLengthByteReader {
  private final InStream input;
  private final byte[] literals =
    new byte[RunLengthByteWriter.MAX_LITERAL_SIZE];
  private int numLiterals = 0;
  private int used = 0;
  private boolean repeat = false;
  private int[] indeces;

  public RunLengthByteReader(InStream input) throws IOException {
    this.input = input;
  }

  private void readValues() throws IOException {
    int control = input.read();
    used = 0;
    if (control == -1) {
      throw new EOFException("Read past end of buffer RLE byte from " + input);
    } else if (control < 0x80) {
      repeat = true;
      numLiterals = control + RunLengthByteWriter.MIN_REPEAT_SIZE;
      int val = input.read();
      if (val == -1) {
        throw new EOFException("Reading RLE byte got EOF");
      }
      literals[0] = (byte) val;
    } else {
      repeat = false;
      numLiterals = 0x100 - control;
      int bytes = 0;
      while (bytes < numLiterals) {
        int result = input.read(literals, bytes, numLiterals - bytes);
        if (result == -1) {
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
    byte result;
    if (used == numLiterals) {
      readValues();
    }
    if (repeat) {
      used += 1;
      result = literals[0];
    } else {
      result = literals[used++];
    }
    return result;
  }

  public void seek(int index) throws IOException {
    input.seek(index);
    int consumed = (int) indeces[index];
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
}
