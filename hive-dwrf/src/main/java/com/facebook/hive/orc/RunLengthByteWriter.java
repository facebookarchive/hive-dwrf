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

import java.io.IOException;

/**
 * Writes a run length encoded stream of bytes.
 * Before each 'run', the control bye is written. Its structure is:
 *
 *    ---------------------------------------
 *    |    bit 0    | bits 1 to 7           |
 *    =======================================
 *    |    0 / 1    | the length of the run |
 *    =======================================
 *
 * If the control byte is an integer in range (0 to 127), run length encoding was used.
 * The minimum number of repetitions seen before the writer switches to run length encoding
 * is {@link RunLengthConstants#MIN_REPEAT_SIZE}. The length stored in this case which ranges
 * from (0 to 127) actually maps to (3 to 130) repetitions. The repeating byte is written post
 * the control byte.
 *
 * If the control byte is an integer in range (128 to 255), run length encoding was NOT used.
 * If a 'run' is not run length encoded, the bits are written off as is. The max number of
 * bytes stored in such 'run' would be 128 (we store it as 2's compliment).
 */

class RunLengthByteWriter {
  private final PositionedOutputStream output;
  private final byte[] buffer = new byte[RunLengthConstants.MAX_LITERAL_SIZE];

  // Number of bytes in the current 'run'
  private int numLiterals = 0;

  // When set to true, it means that the current 'run' would be run length encoded which happens
  // only when the number of repeats found >= {@link RunLengthConstants#MIN_REPEAT_SIZE}
  private boolean isRunLengthEncoded = false;

  // Keeps the length of repeat of the last byte seen in current 'run'
  private int tailRunLength = 0;

  RunLengthByteWriter(PositionedOutputStream output) {
    this.output = output;
  }

  private void writeValues() throws IOException {
    if (numLiterals == 0)
      return;

    if (isRunLengthEncoded) {
      // The number of repetitions would be in range 3 to 130. We map this to the range 0 to 127 as there
      // are only 7 bits in control byte to store this. This is followed by the byte which is repeated.
      output.write(numLiterals - RunLengthConstants.MIN_REPEAT_SIZE);
      output.write(buffer, 0, 1);
    } else {
      output.write(-numLiterals);
      output.write(buffer, 0, numLiterals); // all the bytes in this 'run'
    }

    // reset everything for the next 'run'
    isRunLengthEncoded = false;
    tailRunLength = 0;
    numLiterals = 0;
  }

  void flush() throws IOException {
    writeValues();
    output.flush();
  }

  void write(byte value) throws IOException {
    if (numLiterals == 0) {       // the start of the stream
      buffer[numLiterals++] = value;
      tailRunLength = 1;
    }
    else if (isRunLengthEncoded) {
      if (value == buffer[0]) {
        numLiterals += 1;
        if (numLiterals == RunLengthConstants.MAX_REPEAT_SIZE) {
          writeValues();
        }
      } else {
        writeValues();
        buffer[numLiterals++] = value;
        tailRunLength = 1;
      }
    }
    else {
      if (value == buffer[numLiterals - 1]) {
        tailRunLength += 1;
      } else {
        tailRunLength = 1;
      }

      if (tailRunLength == RunLengthConstants.MIN_REPEAT_SIZE) {
        if (numLiterals+1 > RunLengthConstants.MIN_REPEAT_SIZE) {
          numLiterals -= RunLengthConstants.MIN_REPEAT_SIZE-1;
          writeValues();
          buffer[0] = value;
        }
        isRunLengthEncoded = true;
        numLiterals = RunLengthConstants.MIN_REPEAT_SIZE;
      } else {
        buffer[numLiterals++] = value;
        if (numLiterals == RunLengthConstants.MAX_LITERAL_SIZE) {
          writeValues();
        }
      }
    }
  }

  void getPosition(PositionRecorder recorder) throws IOException {
    output.getPosition(recorder);
    recorder.addPosition(numLiterals);
  }
}
