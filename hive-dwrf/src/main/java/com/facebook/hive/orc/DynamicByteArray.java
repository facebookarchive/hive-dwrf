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
import java.io.InputStream;
import java.io.OutputStream;
import java.nio.ByteBuffer;

import org.apache.hadoop.hive.ql.io.slice.Slice;
import org.apache.hadoop.hive.ql.io.slice.Slices;
import org.apache.hadoop.io.Text;

/**
 * A class that is a growable array of bytes. Growth is managed in terms of
 * chunks that are allocated when needed.
 */
final class DynamicByteArray {
  static final int DEFAULT_SIZE = 32 * 1024;

  private Slice data;                 // the real data
  private int length = 0;

  public DynamicByteArray() {
    this(DEFAULT_SIZE);
  }

  public DynamicByteArray(int size) {
    if (size == 0) {
      throw new IllegalArgumentException("bad chunksize");
    }
    data = Slices.allocate(size);
  }

  /**
   * Ensure that the given index is valid.
   */
  private void grow(int index) {
    if (index >= data.length()) {
      int newSize = Math.max(index + DEFAULT_SIZE, 2 * data.length());
      Slice newSlice = Slices.allocate(newSize);
      newSlice.setBytes(0, data);
      data = newSlice;
    }
  }

  public byte get(int index) {
    if (index >= length) {
      throw new IndexOutOfBoundsException("Index " + index +
                                            " is outside of 0.." +
                                            (length - 1));
    }
    return data.getByte(index);
  }

  public void set(int index, byte value) {
    grow(index);
    if (index >= length) {
      length = index + 1;
    }
    data.setByte(index, value);
  }

  public int add(byte value) {
    grow(length);
    data.setByte(length, value);
    int result = length;
    length += 1;
    return result;
  }

  /**
   * Copy a slice of a byte array into our buffer.
   * @param value the array to copy from
   * @param valueOffset the first location to copy from value
   * @param valueLength the number of bytes to copy from value
   * @return the offset of the start of the value
   */
  public int add(byte[] value, int valueOffset, int valueLength) {
    grow(length + valueLength);
    data.setBytes(length, value, valueOffset, valueLength);
    int result = length;
    length += valueLength;
    return result;
  }

  /**
   * Read the entire stream into this array.
   * @param in the stream to read from
   * @throws IOException
   */
  public void readAll(InputStream in) throws IOException {
    int read = 0;
    do {
      grow(length);
      read = data.setBytes(length, in, data.length() - length);
      if (read > 0) {
        length += read;
      }
    } while (read > 0);
  }

  /**
   * Byte compare a set of bytes against the bytes in this dynamic array.
   * @param other source of the other bytes
   * @param otherOffset start offset in the other array
   * @param otherLength number of bytes in the other array
   * @param ourOffset the offset in our array
   * @param ourLength the number of bytes in our array
   * @return negative for less, 0 for equal, positive for greater
   */
 public int compare(byte[] other, int otherOffset, int otherLength, int ourOffset, int ourLength) {
   return 0 - data.compareTo(ourOffset, ourLength, other, otherOffset, otherLength);
 }

 public int compare(int otherOffset, int otherLength, int ourOffset, int ourLength) {
   return 0 - data.compareTo(ourOffset, ourLength, data, otherOffset, otherLength);
 }

  public boolean equals(byte[] other, int otherOffset, int otherLength, int ourOffset, int ourLength) {
    return data.equals(ourOffset, ourLength, other, otherOffset, otherLength);
  }

  /**
   * Get the size of the array.
   * @return the number of bytes in the array
   */
  public int size() {
    return length;
  }

  /**
   * Clear the array to its original pristine state.
   */
  public void clear() {
    length = 0;
    data = Slices.allocate(DEFAULT_SIZE);
  }

  /**
   * Set a text value from the bytes in this dynamic array.
   * @param result the value to set
   * @param offset the start of the bytes to copy
   * @param length the number of bytes to copy
   */
  public void setText(Text result, int offset, int length) {
    result.clear();
    result.append(data.getBytes(), offset, length);
  }

  /**
   * Write out a range of this dynamic array to an output stream.
   * @param out the stream to write to
   * @param offset the first offset to write
   * @param length the number of bytes to write
   * @throws IOException
   */
  public void write(OutputStream out, int offset,
                    int length) throws IOException {
    data.getBytes(offset, out, length);
  }

  @Override
  public String toString() {
    int i;
    StringBuilder sb = new StringBuilder(length * 3);

    sb.append('{');
    int l = length - 1;
    for (i=0; i<l; i++) {
      sb.append(Integer.toHexString(get(i)));
      sb.append(',');
    }
    sb.append(get(i));
    sb.append('}');

    return sb.toString();
  }

  public void setByteBuffer(ByteBuffer result, int offset, int length) {
    result.clear();
    result.put(data.getBytes(), offset, length);
  }

  /**
   * Get the size of the buffers.
   */
  public long getSizeInBytes() {
    return data.length();
  }
}

