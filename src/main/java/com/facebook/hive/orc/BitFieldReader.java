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

public class BitFieldReader {
  private final RunLengthByteReader input;
  private int current;
  private int bitsLeft;
  // The number of consumed bytes at each index stride
  private int[] indeces;

  public BitFieldReader(InStream input) throws IOException {
    this.input = new RunLengthByteReader(input);
  }

  private void readByte() throws IOException {
    if (input.hasNext()) {
      current = 0xff & input.next();
      bitsLeft = 8;
    } else {
      throw new EOFException("Read past end of bit field from " + input);
    }
  }

  public int next() throws IOException {
    int result = 0;

    if (bitsLeft == 0) {
      readByte();
    }

    bitsLeft--;
    result |= (current >>> bitsLeft) & 1;

    return result & 1;
  }

  public void seek(int index) throws IOException {
    input.seek(index);
    int consumed = (int) indeces[index];
    if (consumed > 8) {
      throw new IllegalArgumentException("Seek past end of byte at " +
          consumed + " in " + input);
    } else if (consumed != 0) {
      readByte();
      bitsLeft = 8 - consumed;
    } else {
      bitsLeft = 0;
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
    long totalBits = items;
    if (bitsLeft >= totalBits) {
      bitsLeft -= totalBits;
    } else {
      totalBits -= bitsLeft;
      input.skip(totalBits / 8);
      bitsLeft = (int) (8 - (totalBits % 8));

      // Load the next value only if the stream still has data. If not,
      // then mark bitsLeft as zero to force exception when values are
      // attempted to be read.
      if (input.hasNext()) {
        current = input.next();
      } else {
        bitsLeft = 0;
      }
    }
  }

  public void close() throws IOException {
    input.close();
  }
}
